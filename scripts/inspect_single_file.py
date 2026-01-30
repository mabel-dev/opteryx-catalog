"""Inspect a single manifest file entry and recompute its stats.

Usage:
  python scripts/inspect_single_file.py <dataset_identifier> <file_path> <output_jsonl_path>

Example:
  python scripts/inspect_single_file.py opteryx.ops.audit_log gs://.../data-...parquet artifacts/single_file.jsonl
"""

import json
import sys
import time
import os
import pyarrow.parquet as pq

from opteryx_catalog.opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry


def _preview(lst, n=6):
    if lst is None:
        return None
    if isinstance(lst, list):
        if len(lst) > n:
            return {"len": len(lst), "preview": lst[:n], "truncated": True}
        return {"len": len(lst), "preview": lst, "truncated": False}
    return lst


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python scripts/inspect_single_file.py <dataset_identifier> <file_path> <output_jsonl_path>")
        sys.exit(2)

    dataset_identifier = sys.argv[1]
    target_fp = sys.argv[2]
    out_path = sys.argv[3]

    catalog_kwargs = {
        "workspace": os.getenv("OPTERYX_WORKSPACE", "opteryx"),
        "firestore_project": os.getenv("OPTERYX_FIRESTORE_PROJECT", "mabeldev"),
        "gcs_bucket": os.getenv("OPTERYX_GCS_BUCKET", "opteryx_data"),
    }

    repo_root = os.getcwd()
    artifacts_dir = os.path.join(repo_root, "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)

    if not os.path.abspath(out_path).startswith(repo_root):
        out_path = os.path.join(artifacts_dir, os.path.basename(out_path))

    # Allow fully-qualified identifiers like 'workspace.collection.dataset'
    parts = dataset_identifier.split('.')
    if len(parts) == 3:
        wk, collection, dname = parts
        catalog_kwargs = dict(catalog_kwargs)
        catalog_kwargs['workspace'] = wk
        dataset_identifier = f"{collection}.{dname}"

    cat = OpteryxCatalog(**catalog_kwargs)
    ds = cat.load_dataset(dataset_identifier)

    snap = ds.snapshot()
    if snap is None or not getattr(snap, "manifest_list", None):
        print("No manifest available", file=sys.stderr)
        sys.exit(3)

    inp = ds.io.new_input(snap.manifest_list)
    with inp.open() as f:
        mbytes = f.read()

    rows = pq.read_table(mbytes).to_pylist()

    match = None
    match_idx = None
    for i, r in enumerate(rows):
        if r.get("file_path") == target_fp:
            match = r
            match_idx = i
            break

    if match is None:
        print("Target file not found in manifest", file=sys.stderr)
        sys.exit(4)

    rec = {"file_index": match_idx, "file_path": target_fp}
    ent = match
    rec["manifest_entry_summary"] = {
        "uncompressed_size": int(ent.get("uncompressed_size_in_bytes") or 0),
        "column_uncompressed_sizes": _preview(ent.get("column_uncompressed_sizes_in_bytes")),
        "null_counts": _preview(ent.get("null_counts")),
        "min_values": _preview(ent.get("min_values")),
        "max_values": _preview(ent.get("max_values")),
        "min_values_display": _preview(ent.get("min_values_display")),
        "max_values_display": _preview(ent.get("max_values_display")),
        "min_k_hashes": _preview(ent.get("min_k_hashes")),
    }

    inp2 = ds.io.new_input(target_fp)
    with inp2.open() as f:
        data = f.read()

    if not data:
        rec["recomputed"] = {"error": "empty file"}
    else:
        table = pq.read_table(data)
        recomputed = build_parquet_manifest_entry(table, target_fp, len(data)).to_dict()
        rec["recomputed"] = {
            "uncompressed_size": int(recomputed.get("uncompressed_size_in_bytes") or 0),
            "column_uncompressed_sizes": _preview(recomputed.get("column_uncompressed_sizes_in_bytes")),
            "null_counts": _preview(recomputed.get("null_counts")),
            "min_values": _preview(recomputed.get("min_values")),
            "max_values": _preview(recomputed.get("max_values")),
            "min_values_display": _preview(recomputed.get("min_values_display")),
            "max_values_display": _preview(recomputed.get("max_values_display")),
            "min_k_hashes": _preview(recomputed.get("min_k_hashes")),
        }
        # diffs
        diffs = {}
        if rec["manifest_entry_summary"]["uncompressed_size"] != rec["recomputed"]["uncompressed_size"]:
            diffs["uncompressed_size_mismatch"] = {"manifest": rec["manifest_entry_summary"]["uncompressed_size"], "recomputed": rec["recomputed"]["uncompressed_size"]}
        man_k = ent.get("min_k_hashes") or []
        rec_k = recomputed.get("min_k_hashes") or []
        diffs["min_k_hashes_nonempty_counts"] = {"manifest_nonempty": sum(1 for x in man_k if x), "recomputed_nonempty": sum(1 for x in rec_k if x)}
        rec["diffs"] = diffs

    with open(out_path, "w", encoding="utf-8") as of:
        of.write(json.dumps({"_meta": {"dataset": dataset_identifier, "timestamp": int(time.time() * 1000)}}) + "\n")
        of.write(json.dumps(rec) + "\n")

    print("WROTE", out_path)
