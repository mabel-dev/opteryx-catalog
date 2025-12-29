import os
import time
import traceback

import pyarrow as pa

from pyiceberg_firestore_gcs import FirestoreCatalog


def main():
    os.environ.setdefault("GCP_PROJECT_ID", "mabeldev")
    os.environ.setdefault("FIRESTORE_DATABASE", "catalogs")
    os.environ.setdefault("GCS_BUCKET", "opteryx_data")

    workspace = os.environ.get("OPTERYX_WORKSPACE", "opteryx")
    catalog = FirestoreCatalog(
        catalog_name=workspace,
        firestore_project=os.environ.get("GCP_PROJECT_ID"),
        firestore_database=os.environ.get("FIRESTORE_DATABASE"),
        gcs_bucket=os.environ.get("GCS_BUCKET"),
    )

    namespace = "tests_temp"
    table_name = f"test_table_api_{int(time.time())}"
    location = f"gs://{os.environ['GCS_BUCKET']}/{workspace}/{namespace}/{table_name}"

    print("Creating table:", f"{namespace}.{table_name}")
    try:
        tbl = catalog.create_table(
            f"{namespace}.{table_name}",
            pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())]),
            location=location,
        )
        print("Created table metadata at:", tbl.metadata.location)
    except Exception as e:
        print("Create failed or already exists:", e)
        try:
            tbl = catalog.load_table((namespace, table_name))
            print("Loaded existing table metadata at:", tbl.metadata.location)
        except Exception as e2:
            print("Failed to load table:", e2)
            traceback.print_exc()
            return 1

    # Append data using Iceberg Table.append (public API)
    data = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
    try:
        tbl.append(data)
        print("Appended data via Table.append()")
    except Exception as e:
        print("Append failed:", e)
        traceback.print_exc()
        return 1

    # Print snapshot info
    try:
        snapshot = tbl.current_snapshot()
        print("Current snapshot:", snapshot.snapshot_id if snapshot else None)
        if snapshot:
            print("Manifest list:", snapshot.manifest_list)
    except Exception as e:
        print("Failed to read snapshot info:", e)

    # Check Firestore snapshot doc for parquet-manifest field
    try:
        from google.cloud import firestore

        db = firestore.Client(
            project=os.environ.get("GCP_PROJECT_ID"), database=os.environ.get("FIRESTORE_DATABASE")
        )
        snap_id = snapshot.snapshot_id if snapshot else None
        if snap_id is not None:
            doc = (
                db.collection(workspace)
                .document(namespace)
                .collection("tables")
                .document(table_name)
                .collection("snapshots")
                .document(str(snap_id))
                .get()
            )
            if doc.exists:
                d = doc.to_dict()
                print("Snapshot doc keys:", list(d.keys()))
                print("parquet-manifest:", d.get("parquet-manifest"))
            else:
                print("Snapshot document not found in Firestore")
    except Exception as e:
        print("Failed to query Firestore snapshot doc:", e)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
