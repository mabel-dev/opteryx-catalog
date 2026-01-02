"""Create test tables using the `FirestoreCatalog` and write Parquet-only
manifests/files (no Avro). This script creates a small deterministic set of
tables under `tests_temp`, writes a single Parquet data file per table, writes
a Parquet manifest for each snapshot, and records a minimal snapshot document
in Firestore so planners can discover the Parquet manifest.

Run locally with valid GCP credentials set in `GOOGLE_APPLICATION_CREDENTIALS`.
"""

import os
import sys
import time
import traceback

import pyarrow as pa

from opteryx_catalog import OpteryxCatalog

sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))


def create_parquet_only_tables(count: int = 2) -> list:
    workspace = os.environ.get("OPTERYX_WORKSPACE", "opteryx")
    catalog = OpteryxCatalog(
        workspace=workspace,
        firestore_project=os.environ.get("GCP_PROJECT_ID"),
        firestore_database=os.environ.get("FIRESTORE_DATABASE"),
        gcs_bucket=os.environ.get("GCS_BUCKET"),
    )

    collection = "tests_temp"
    created = []

    for i in range(count):
        table_name = f"test_table_{i}_{int(time.time())}"
        # location = f"gs://{os.environ.get('GCS_BUCKET')}/{workspace}/{collection}/{table_name}"

        # Use fixed test author
        author = "me"

        # Create dataset metadata (create_collection is handled inside create_dataset)
        try:
            tbl = catalog.create_dataset(
                f"{collection}.{table_name}",
                pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())]),
                author=author,
            )
            print("Created dataset metadata:", f"{collection}.{table_name}")
        except Exception:
            tbl = catalog.load_dataset(f"{collection}.{table_name}")
            print("Loaded existing dataset metadata:", f"{collection}.{table_name}")

        # Add an example sort order to the table metadata and persist it so
        # we can inspect `sort-orders` in Firestore.
        try:
            tbl.metadata.sort_orders = [
                {"order-id": 1, "fields": [{"name": "id", "direction": "asc"}]}
            ]
            if hasattr(catalog, "save_dataset_metadata"):
                catalog.save_dataset_metadata(f"{collection}.{table_name}", tbl.metadata)
        except Exception:
            pass

        # Create a deterministic planets dataset so read_table tests can validate
        planets = [
            (1, "Mercury"),
            (2, "Venus"),
            (3, "Earth"),
            (4, "Mars"),
            (5, "Jupiter"),
            (6, "Saturn"),
            (7, "Uranus"),
            (8, "Neptune"),
        ]

        data = pa.table({"id": [p[0] for p in planets], "name": [p[1] for p in planets]})

        # Append data using the new SimpleDataset.append() which writes the data
        # file, creates a Parquet manifest and persists snapshot metadata.
        try:
            tbl.append(data, author=author)
            print("Appended data via Dataset.append() for", f"{collection}.{table_name}")
        except Exception as e:
            print("Dataset.append() failed:", e)
            raise

        # Also append a second dataset (an edit) so we have multiple snapshots/files
        try:
            # small additional rows to simulate an update
            extra = [(9, "Pluto"), (10, "Eris")]
            data2 = pa.table({"id": [p[0] for p in extra], "name": [p[1] for p in extra]})
            tbl.append(data2, author=author, commit_message="append extra planets")
            print("Appended second dataset via Dataset.append() for", f"{collection}.{table_name}")
        except Exception as e:
            print("Second Dataset.append() failed:", e)
            raise

        # Inspect snapshot and Firestore snapshot doc for parquet-manifest
        parquet_manifest_path = None
        try:
            snapshot = tbl.snapshot()
            snapshot_id = snapshot.snapshot_id if snapshot else None
            print("Current snapshot id:", snapshot_id)
            if snapshot_id is not None:
                from google.cloud import firestore

                db = firestore.Client(
                    project=os.environ.get("GCP_PROJECT_ID"),
                    database=os.environ.get("FIRESTORE_DATABASE"),
                )
                snap_doc = (
                    db.collection(workspace)
                    .document(collection)
                    .collection("datasets")
                    .document(table_name)
                    .collection("snapshots")
                    .document(str(snapshot_id))
                    .get()
                )
                if snap_doc.exists:
                    d = snap_doc.to_dict() or {}
                    parquet_manifest_path = d.get("manifest")
                    print("Snapshot doc keys:", list(d.keys()))
                    print("manifest:", parquet_manifest_path)
                else:
                    print("Snapshot document not found in Firestore for", snapshot_id)
        except Exception as e:
            print("Failed to read snapshot/Firestore doc:", e)

        created.append(
            {
                "collection": collection,
                "dataset": table_name,
                "location": tbl.metadata.location,
                "manifest": parquet_manifest_path,
            }
        )

        # Create a simple view document in Firestore under `views` subcollection
        try:
            from google.cloud import firestore

            db = firestore.Client(
                project=os.environ.get("GCP_PROJECT_ID"),
                database=os.environ.get("FIRESTORE_DATABASE"),
            )
            view_name = f"view_{table_name}"
            view_doc_ref = (
                db.collection(workspace)
                .document(collection)
                .collection("views")
                .document(view_name)
            )
            now_ms = int(time.time() * 1000)
            author = "me"
            # Include optional last-execution metrics (populated later by the view runner)
            # Store the SQL text in a `statement` subcollection so we can version/track changes
            statement_id = str(now_ms)
            statement_coll = view_doc_ref.collection("statement")
            statement_coll.document(statement_id).set(
                {
                    "sql": f"SELECT * FROM {workspace}.{collection}.{table_name}",
                    "timestamp-ms": now_ms,
                    "author": author,
                    "sequence-number": 1,
                }
            )

            # Root view doc references the statement doc via `statement-id` (no inline query)
            view_doc_ref.set(
                {
                    "name": view_name,
                    "collection": collection,
                    "workspace": workspace,
                    "timestamp-ms": now_ms,
                    "author": author,
                    "description": f"View over {table_name}",
                    "describer": author,
                    "last-execution-ms": None,
                    "last-execution-data-size": None,
                    "last-execution-records": None,
                    "statement-id": statement_id,
                }
            )
            print("Created view doc:", f"{collection}.{view_name}")
        except Exception as e:
            print("Failed to create view doc:", e)

    return created


if __name__ == "__main__":
    try:
        created = create_parquet_only_tables(2)
        print("\nCreated datasets summary:")
        for c in created:
            print(c)
    except Exception:
        traceback.print_exc()
        raise
