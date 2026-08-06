#!/usr/bin/env python3
"""
Recover an expired snapshot from GCS soft-delete, into a NEW dataset.

The bucket keeps deleted objects for a fixed retention period (7 days on
`opteryx_data`). Within that window every data file and manifest a snapshot
referenced is still physically present, just flagged deleted - so a snapshot
that garbage collection removed can be reconstructed, provided you can still
work out which objects it referenced.

That is what a manifest is. Manifests are cumulative: one manifest lists every
data file live as of its snapshot. So recovering a point in time is:

    manifest-<snapshot_id>.parquet  ->  its file_path entries  ->  those objects

and the manifest itself is usually recoverable from soft-delete too.

WHY IT RESTORES INTO A NEW DATASET
----------------------------------
Restoring in place would mutate the dataset you are trying to diagnose, and
would race whatever maintenance job deleted the files in the first place. The
recovered snapshot is written to a new dataset instead, so the damaged one is
left exactly as it is for comparison.

THE ONE UNAVOIDABLE MUTATION
----------------------------
A soft-deleted generation cannot be read or server-side copied - the GCS API
404s on both. `restoreObject` is the only way to get the bytes back, and it
restores to the ORIGINAL path. So this tool must briefly un-delete objects in
the source prefix before copying them out.

That has a consequence worth understanding before running it: while a
maintenance job is deleting from the same prefix, a restored object is a fresh
deletion candidate. Pause that job first. Every restore is announced, and
nothing is ever deleted by this tool - restored originals are left in place and
listed at the end for you to decide about.

USAGE
-----
    # Read-only. What restore points exist, and how long do we have?
    restore_snapshot.py inventory <workspace> <collection.dataset>

    # Restores ONE small manifest object, reads it, reports what it referenced.
    restore_snapshot.py inspect <workspace> <collection.dataset> <snapshot_id>

    # Plan the full restore (no copying without --execute).
    restore_snapshot.py restore <workspace> <collection.dataset> <snapshot_id> \
        --target <collection.dataset> [--execute]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

MANIFEST_RE = re.compile(r"/metadata/manifest-(\d+)\.parquet$")

# Loaded from the environment; see `_load_env`.
ENV_KEYS = (
    "GCP_PROJECT_ID",
    "GCS_BUCKET",
    "FIRESTORE_DATABASE",
    "GOOGLE_APPLICATION_CREDENTIALS",
)


def _load_env(env_path: str) -> None:
    """Read only the keys this tool needs out of a .env file.

    Deliberately selective: these .env files also carry OAuth secrets and
    third-party API keys that have no business being loaded here.
    """
    if not os.path.exists(env_path):
        sys.exit(f"env file not found: {env_path}")

    with open(env_path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            if key.strip() in ENV_KEYS:
                os.environ[key.strip()] = value.strip().strip("'\"")

    missing = sorted(k for k in ENV_KEYS if not os.environ.get(k))
    if missing:
        sys.exit(f"missing from env: {missing}")

    creds = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    if not os.path.exists(creds):
        candidate = os.path.join(os.path.dirname(env_path) or ".", creds)
        if not os.path.exists(candidate):
            sys.exit(f"credentials file not found: {creds}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(candidate)


class SoftDeleteIndex:
    """Live and soft-deleted objects under one dataset prefix.

    Soft-deleted entries are keyed by name and keep the most recently deleted
    generation. A path can have several: a file written, deleted, rewritten by
    compaction and deleted again shares one name across generations, and the
    newest is the one whose contents matched the newest manifest.
    """

    def __init__(self, client, bucket_name: str, prefix: str):
        self.client = client
        self.bucket_name = bucket_name
        self.bucket = client.bucket(bucket_name)
        self.prefix = prefix

        self.live: Dict[str, object] = {}
        for blob in client.list_blobs(bucket_name, prefix=prefix):
            self.live[blob.name] = blob

        self.deleted: Dict[str, object] = {}
        for blob in client.list_blobs(bucket_name, prefix=prefix, soft_deleted=True):
            existing = self.deleted.get(blob.name)
            if existing is None or (blob.soft_delete_time or _EPOCH) > (
                existing.soft_delete_time or _EPOCH
            ):
                self.deleted[blob.name] = blob

    def status(self, name: str) -> str:
        """One of 'live', 'recoverable', 'lost'."""
        if name in self.live:
            return "live"
        if name in self.deleted:
            return "recoverable"
        return "lost"

    def deadline(self) -> Optional[datetime]:
        """Earliest hard-delete time across recoverable objects."""
        times = [b.hard_delete_time for b in self.deleted.values() if b.hard_delete_time]
        return min(times) if times else None

    def manifests(self) -> List[Tuple[int, str, str]]:
        """(snapshot_id, object name, status) for every manifest seen."""
        found = {}
        for name in list(self.live) + list(self.deleted):
            match = MANIFEST_RE.search("/" + name if not name.startswith("/") else name)
            if match:
                found[name] = int(match.group(1))
        return sorted(
            ((snapshot_id, name, self.status(name)) for name, snapshot_id in found.items()),
            key=lambda row: row[0],
        )

    def restore(self, name: str) -> bool:
        """Un-delete one object at its original path. Returns True if restored.

        The only mutating call in this module, and never a delete.
        """
        if name in self.live:
            return False
        blob = self.deleted.get(name)
        if blob is None:
            raise KeyError(f"not recoverable: {name}")
        self.bucket.restore_blob(name, generation=blob.generation)
        self.live[name] = self.bucket.get_blob(name)
        return True


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _blob_name(path: str, bucket: str) -> str:
    prefix = f"gs://{bucket}/"
    return path[len(prefix):] if path.startswith(prefix) else path.lstrip("/")


def _connect(workspace: str):
    from google.cloud import firestore, storage

    firestore_client = firestore.Client(
        project=os.environ["GCP_PROJECT_ID"], database=os.environ["FIRESTORE_DATABASE"]
    )
    # Verified before constructing an OpteryxCatalog, whose __init__ creates a
    # $properties document for a workspace that does not exist - so a typo here
    # would otherwise write a new workspace into production.
    if workspace not in {c.id for c in firestore_client.collections()}:
        sys.exit(f"workspace {workspace!r} not found")

    storage_client = storage.Client(project=os.environ["GCP_PROJECT_ID"])

    from opteryx_catalog import OpteryxCatalog

    catalog = OpteryxCatalog(
        workspace=workspace,
        firestore_project=os.environ["GCP_PROJECT_ID"],
        firestore_database=os.environ["FIRESTORE_DATABASE"],
        gcs_bucket=os.environ["GCS_BUCKET"],
    )
    return catalog, storage_client


def _source_prefix(catalog, identifier: str) -> Tuple[str, str]:
    """(dataset location, blob prefix) for an existing dataset."""
    collection, dataset_name = identifier.split(".", 1)
    doc = catalog._dataset_doc_ref(collection, dataset_name).get()
    if not doc.exists:
        sys.exit(f"dataset {identifier!r} not found in the catalog")
    location = (doc.to_dict() or {}).get("location")
    if not location:
        sys.exit(f"dataset {identifier!r} has no location recorded")
    return location, _blob_name(location, os.environ["GCS_BUCKET"]).rstrip("/") + "/"


# --- commands ---------------------------------------------------------------


def cmd_inventory(args) -> None:
    """Read-only. Nothing here restores, copies, or deletes."""
    catalog, storage_client = _connect(args.workspace)
    location, prefix = _source_prefix(catalog, args.dataset)
    index = SoftDeleteIndex(storage_client, os.environ["GCS_BUCKET"], prefix)

    print(f"dataset:  {args.dataset}")
    print(f"location: {location}")
    print(f"live objects:        {len(index.live)}")
    print(f"recoverable objects: {len(index.deleted)}")

    deadline = index.deadline()
    if deadline:
        hours = (deadline - datetime.now(timezone.utc)).total_seconds() / 3600
        print(f"earliest hard delete: {deadline.isoformat()}  ({hours:.1f}h from now)")
    print()

    manifests = index.manifests()
    print(f"restore points ({len(manifests)} manifests):")
    print(f"  {'snapshot_id':>16}  {'when':<20}  status")
    for snapshot_id, name, status in manifests:
        when = datetime.fromtimestamp(snapshot_id / 1000, tz=timezone.utc)
        flag = "  <-- live" if status == "live" else ""
        print(f"  {snapshot_id:>16}  {when.strftime('%Y-%m-%d %H:%M:%S')}  {status}{flag}")

    # Firestore's account of the same history. Live entries are normal
    # snapshots; tombstoned ones were retired by expiration but keep their
    # manifest path for the record window (see EXPIRED_SNAPSHOT_RETENTION_MS)
    # - they are the restore points
    # that survive after the manifest objects themselves leave live storage.
    collection, dataset_name = args.dataset.split(".", 1)
    try:
        docs = list(catalog._snapshots_collection(collection, dataset_name).stream())
    except Exception as exc:
        docs = []
        print(f"\n(could not read Firestore snapshot entries: {exc})")

    if docs:
        from opteryx_catalog.catalog.metadata import (
            SNAPSHOT_EXPIRED_AT_KEY,
            snapshot_is_tombstoned,
        )

        tombstoned = [
            (doc.id, doc.to_dict() or {})
            for doc in docs
            if snapshot_is_tombstoned(doc.to_dict() or {})
        ]
        print(f"\nFirestore: {len(docs)} snapshot entries, {len(tombstoned)} tombstoned")
        for doc_id, data in sorted(tombstoned, key=lambda pair: pair[0]):
            expired_at = data.get(SNAPSHOT_EXPIRED_AT_KEY)
            when = (
                datetime.fromtimestamp(expired_at / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M"
                )
                if isinstance(expired_at, int)
                else "?"
            )
            manifest = data.get("manifest") or "?"
            manifest_status = (
                index.status(_blob_name(manifest, os.environ["GCS_BUCKET"]))
                if manifest != "?"
                else "unknown"
            )
            print(
                f"  {doc_id}: expired {when} by {data.get('expired-by')!r}, "
                f"manifest {manifest_status}"
            )

    print(
        "\nPick a snapshot_id and run `inspect` to see what it referenced."
        "\nNote: a manifest lists every file live at that moment, so the newest"
        "\nrecoverable manifest usually restores the most complete dataset."
    )


def _load_manifest_rows(index, catalog, prefix: str, snapshot_id: int, announce: bool):
    """Restore (if needed) and decode the manifest for `snapshot_id`."""
    name = f"{prefix}metadata/manifest-{snapshot_id}.parquet"
    status = index.status(name)
    if status == "lost":
        sys.exit(
            f"manifest for snapshot {snapshot_id} is neither live nor recoverable; "
            "its retention window has expired"
        )

    if status == "recoverable":
        if announce:
            print(f"restoring manifest object (un-deletes it at its original path):")
            print(f"  gs://{os.environ['GCS_BUCKET']}/{name}")
        index.restore(name)
        print("  restored")
    else:
        print(f"manifest is still live: gs://{os.environ['GCS_BUCKET']}/{name}")

    from opteryx_catalog.catalog.manifest import read_manifest_rows

    data = index.bucket.blob(name).download_as_bytes()
    return name, read_manifest_rows(data)


def cmd_inspect(args) -> None:
    """Restores one manifest object, then reports what it referenced."""
    catalog, storage_client = _connect(args.workspace)
    location, prefix = _source_prefix(catalog, args.dataset)
    index = SoftDeleteIndex(storage_client, os.environ["GCS_BUCKET"], prefix)
    bucket_name = os.environ["GCS_BUCKET"]

    name, rows = _load_manifest_rows(index, catalog, prefix, args.snapshot_id, announce=True)
    print(f"\nmanifest entries: {len(rows)}")

    tally = {"live": [], "recoverable": [], "lost": []}
    total_bytes = 0
    for row in rows:
        path = row.get("file_path")
        if not path:
            continue
        total_bytes += int(row.get("file_size_in_bytes") or 0)
        tally[index.status(_blob_name(path, bucket_name))].append(path)

    print(f"total recorded size: {total_bytes / (1024 ** 3):.3f} GiB\n")
    for state in ("live", "recoverable", "lost"):
        print(f"  {state:<12} {len(tally[state])}")
    for path in tally["lost"][:20]:
        print(f"      LOST: {path}")

    if tally["lost"]:
        print(
            f"\n{len(tally['lost'])} file(s) are past their retention window."
            "\nA restore from this snapshot will be INCOMPLETE."
        )
    else:
        print("\nEvery file this snapshot referenced is still obtainable.")


def cmd_restore(args) -> None:
    catalog, storage_client = _connect(args.workspace)
    bucket_name = os.environ["GCS_BUCKET"]
    location, prefix = _source_prefix(catalog, args.dataset)

    if args.target == args.dataset:
        sys.exit("target must differ from the source dataset")

    target_collection, target_name = args.target.split(".", 1)
    target_ref = catalog._dataset_doc_ref(target_collection, target_name)
    if target_ref.get().exists:
        sys.exit(f"target dataset {args.target!r} already exists; refusing to overwrite")

    target_location = (
        f"gs://{bucket_name}/{args.workspace}/{target_collection}/{target_name}"
    )
    target_prefix = _blob_name(target_location, bucket_name) + "/"
    if any(storage_client.list_blobs(bucket_name, prefix=target_prefix, max_results=1)):
        sys.exit(f"target location is not empty: {target_location}")

    index = SoftDeleteIndex(storage_client, bucket_name, prefix)
    manifest_name, rows = _load_manifest_rows(
        index, catalog, prefix, args.snapshot_id, announce=True
    )

    plan, lost = [], []
    for row in rows:
        path = row.get("file_path")
        if not path:
            continue
        source_name = _blob_name(path, bucket_name)
        status = index.status(source_name)
        if status == "lost":
            lost.append(path)
            continue
        # Preserve the layout below the dataset root so the restored dataset
        # mirrors the original rather than flattening it.
        relative = source_name[len(prefix):] if source_name.startswith(prefix) else source_name
        plan.append((source_name, target_prefix + relative, status, row))

    print(f"\nsource:  {location}")
    print(f"target:  {target_location}")
    print(f"files to copy: {len(plan)}  (unrecoverable: {len(lost)})")
    for source_name, target_name_, status, _ in plan[:10]:
        print(f"  [{status:<11}] {source_name}")
    if len(plan) > 10:
        print(f"  ... and {len(plan) - 10} more")
    for path in lost[:10]:
        print(f"  [LOST       ] {path}")

    if not args.execute:
        print("\nplan only - nothing copied. Re-run with --execute to perform it.")
        if lost:
            print(f"WARNING: {len(lost)} file(s) cannot be recovered; restore will be partial.")
        return

    if lost and not args.allow_partial:
        sys.exit(
            f"\n{len(lost)} file(s) are unrecoverable. Re-run with --allow-partial to "
            "restore what remains, knowing the result is incomplete."
        )

    print("\ncopying...")
    restored_in_place, new_rows = [], []
    for position, (source_name, dest_name, status, row) in enumerate(plan, start=1):
        if status == "recoverable":
            index.restore(source_name)
            restored_in_place.append(source_name)
        bucket = storage_client.bucket(bucket_name)
        bucket.copy_blob(bucket.blob(source_name), bucket, dest_name)
        new_row = dict(row)
        new_row["file_path"] = f"gs://{bucket_name}/{dest_name}"
        new_rows.append(new_row)
        if position % 25 == 0 or position == len(plan):
            print(f"  {position}/{len(plan)}")

    new_manifest = catalog.write_parquet_manifest(args.snapshot_id, new_rows, target_location)
    print(f"\nwrote manifest: {new_manifest}")

    source_collection, source_dataset = args.dataset.split(".", 1)
    source_doc = catalog._dataset_doc_ref(source_collection, source_dataset).get().to_dict() or {}

    target_ref.set(
        {
            **source_doc,
            "name": target_name,
            "collection": target_collection,
            "workspace": args.workspace,
            "location": target_location,
            "current-snapshot-id": args.snapshot_id,
            "timestamp-ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            "description": (
                f"Restored from {args.dataset} snapshot {args.snapshot_id} "
                f"via restore_snapshot.py"
            ),
            # Unversioned by default so the restored copy is not itself
            # expired by a maintenance pass before anyone has looked at it.
            "maintenance-policy": {"retained-snapshot-age-days": -1},
        }
    )

    for schema_doc in (
        catalog._dataset_doc_ref(source_collection, source_dataset).collection("schemas").stream()
    ):
        target_ref.collection("schemas").document(schema_doc.id).set(schema_doc.to_dict() or {})

    catalog._snapshots_collection(target_collection, target_name).document(
        str(args.snapshot_id)
    ).set(
        {
            "snapshot-id": args.snapshot_id,
            "timestamp-ms": args.snapshot_id,
            "manifest": new_manifest,
            "user-created": True,
            "summary": {
                "restored-from": args.dataset,
                "restored-at-ms": int(datetime.now(timezone.utc).timestamp() * 1000),
            },
        }
    )

    print(f"\nrestored to dataset: {args.target}")
    print(f"  files copied: {len(new_rows)}")
    if lost:
        print(f"  MISSING (unrecoverable): {len(lost)}")
    print(
        f"\n{len(restored_in_place)} object(s) were un-deleted in the SOURCE prefix to be read."
        "\nThey are still there. This tool never deletes; decide what to do with them"
        "\nyourself, and remember a running maintenance job will treat them as orphans."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--env", default="../opteryx-core/.env")
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("inventory", "inspect", "restore"):
        p = sub.add_parser(name)
        p.add_argument("workspace")
        p.add_argument("dataset", help="collection.dataset")
        if name != "inventory":
            p.add_argument("snapshot_id", type=int)
        if name == "restore":
            p.add_argument("--target", required=True, help="collection.dataset to create")
            p.add_argument("--execute", action="store_true")
            p.add_argument("--allow-partial", action="store_true")

    args = parser.parse_args()
    _load_env(args.env)

    {"inventory": cmd_inventory, "inspect": cmd_inspect, "restore": cmd_restore}[
        args.command
    ](args)


if __name__ == "__main__":
    main()
