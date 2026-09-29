"""Bind a workspace to a Firestore database (kind = "firestore"), by hand.

Like postgres, this kind is not offered by control.opteryx's
/v1/catalog-kinds descriptor, so a Firestore-bound TEST workspace is created
with this script. It writes the same `catalog` block on the workspace's
`$properties` document that the control plane would, through the same library
function.

What the worker does with it (worker.opteryx/app/catalog_resolver.py):
  kind "firestore" -> opteryx.connectors.FirestoreConnector, no metastore.
  `config` becomes the connector's constructor keywords (project, database).
  Every collection reads as `<workspace>.<collection>` with the columns
  id, doc (the document as JSON), created_at, updated_at.

Two auth modes:

  ambient (default) - the worker reads as its own service account. Grant that
  account roles/datastore.viewer on the Firestore project first.

    python scripts/bind_firestore_workspace.py \\
        --project my-gcp-project --database catalogs \\
        --workspace fs_test --updated-by justin \\
        --firestore-project customer-project [--firestore-database '(default)'] \\
        [--collection jobs --collection runs]

  --collection limits the workspace to the named top-level collections; omit
  it and every collection in the database is readable through the workspace.

  stored - a service-account key, KMS-envelope-encrypted and injected as the
  connector's `credentials`. The key is read from a file, never from argv:

    python scripts/bind_firestore_workspace.py ... \\
        --key-file ./reader-key.json \\
        --kms-key projects/P/locations/L/keyRings/R/cryptoKeys/K

    python scripts/bind_firestore_workspace.py ... --clear   # revert to native

Stored mode requires the `kms` extra (opteryx-catalog[kms]).
"""

import argparse
import json
import sys

from google.cloud import firestore

from opteryx_catalog.binding import clear_catalog_binding
from opteryx_catalog.binding import read_catalog_binding
from opteryx_catalog.binding import write_catalog_binding


def _parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", required=True, help="GCP project of the catalogs Firestore")
    parser.add_argument("--database", required=True, help="Firestore database holding the workspaces")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--updated-by", required=True, help="who is recorded as the binding's author")
    parser.add_argument("--clear", action="store_true", help="remove the binding (workspace reverts to native)")
    parser.add_argument("--firestore-project", help="GCP project of the Firestore database to read")
    parser.add_argument("--firestore-database", default="(default)", help="database id to read")
    parser.add_argument(
        "--collection",
        action="append",
        dest="collections",
        help="expose only this collection (repeatable); default is the whole database",
    )
    parser.add_argument("--key-file", help="service-account key JSON (stored mode)")
    parser.add_argument("--kms-key", help="KMS key resource name that wraps the key (stored mode)")
    parser.add_argument(
        "--preserve-sql-case",
        action="store_true",
        help="use collection names exactly as typed (Firestore ids are case-sensitive)",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    client = firestore.Client(project=args.project, database=args.database)

    if args.clear:
        cleared = clear_catalog_binding(client, args.workspace)
        print(f"{args.workspace}: binding {'cleared' if cleared else 'was not set'}")
        return 0

    if not args.firestore_project:
        print("missing required option: --firestore-project", file=sys.stderr)
        return 2
    if bool(args.key_file) != bool(args.kms_key):
        print("stored mode needs both --key-file and --kms-key", file=sys.stderr)
        return 2

    existing = read_catalog_binding(client, args.workspace)
    if existing is not None and existing.kind != "firestore":
        print(
            f"{args.workspace} is already bound to kind '{existing.kind}'; a workspace's "
            "storage type is fixed for its lifetime — refusing to rebind",
            file=sys.stderr,
        )
        return 1

    config = {"project": args.firestore_project, "database": args.firestore_database}
    if args.collections:
        config["collections"] = sorted(set(args.collections))
    auth = {}
    if args.key_file:
        from opteryx_catalog.security.kms import encrypt_secret  # optional extra; import late

        with open(args.key_file, encoding="utf-8") as handle:
            key_text = handle.read()
        try:
            if json.loads(key_text).get("type") != "service_account":
                raise ValueError
        except ValueError:
            print(f"{args.key_file} is not a service-account key", file=sys.stderr)
            return 2
        auth = {
            "auth_mode": "stored",
            "ciphertext": encrypt_secret(key_text, args.kms_key),
            "kms_key": args.kms_key,
            "inject_as": "credentials",
        }

    version = write_catalog_binding(
        client,
        args.workspace,
        kind="firestore",
        config=config,
        preserve_sql_case=args.preserve_sql_case,
        updated_by=args.updated_by,
        **auth,
    )
    mode = "stored key" if auth else "ambient identity"
    scope = ", ".join(config.get("collections", [])) or "all collections"
    print(
        f"{args.workspace}: bound to firestore {args.firestore_project}/{args.firestore_database} "
        f"[{scope}] ({mode}, version {version})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
