"""Verify that Parquet manifests are being written correctly."""

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

from pyiceberg_firestore_gcs import FirestoreCatalog
from google.cloud import storage


def check_parquet_manifest(catalog_name: str, namespace: str, table_name: str):
    """Check if Parquet manifest exists for a table."""

    catalog = FirestoreCatalog(
        catalog_name,
        firestore_project="mabeldev",
        firestore_database="catalogs",
        gcs_bucket="opteryx_data",
    )

    # Load table
    table = catalog.load_table(f"{namespace}.{table_name}")
    print(f"\n{'=' * 80}")
    print(f"Table: {catalog_name}.{namespace}.{table_name}")
    print(f"{'=' * 80}")

    # Get snapshot info
    snapshot = table.metadata.current_snapshot()
    if not snapshot:
        print("❌ No snapshot found")
        return

    print(f"✓ Current snapshot ID: {snapshot.snapshot_id}")

    # Check for Parquet manifest
    location = table.metadata.location
    parquet_path = f"{location}/metadata/manifest-{snapshot.snapshot_id}.parquet"

    # Parse GCS path
    if parquet_path.startswith("gs://"):
        parquet_path = parquet_path[5:]

    bucket_name, blob_path = parquet_path.split("/", 1)

    # Check if exists
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if blob.exists():
        blob.reload()
        print(f"✓ Parquet manifest EXISTS: gs://{bucket_name}/{blob_path}")
        print(f"  Size: {blob.size:,} bytes ({blob.size / 1024:.1f} KB)")
        print(f"  Created: {blob.time_created}")

        # Check Avro manifests for comparison
        avro_manifests = list(snapshot.manifests(table.io))
        print(f"  Avro manifests: {len(avro_manifests)}")

        total_avro_size = 0
        for manifest in avro_manifests:
            # Extract path from manifest
            avro_path = manifest.manifest_path
            if avro_path.startswith("gs://"):
                avro_path = avro_path[5:]
            _, avro_blob_path = avro_path.split("/", 1)
            avro_blob = bucket.blob(avro_blob_path)
            if avro_blob.exists():
                avro_blob.reload()
                total_avro_size += avro_blob.size

        print(f"  Total Avro size: {total_avro_size:,} bytes ({total_avro_size / 1024:.1f} KB)")

        if total_avro_size > 0:
            ratio = blob.size / total_avro_size
            print(f"  Parquet/Avro ratio: {ratio:.2f}x")

        # Scan to verify it works
        print("\n📊 Testing query planning...")
        import time
        import logging

        # Enable logging to see which path is used
        logging.basicConfig(level=logging.INFO, force=True)

        start = time.perf_counter()
        files = list(table.scan().plan_files())
        elapsed = time.perf_counter() - start
        print(f"  ✓ Found {len(files)} data files in {elapsed * 1000:.1f}ms")
        print("  Check logs above for 'PARQUET manifest' or 'AVRO manifests' message")

    else:
        print(f"❌ Parquet manifest NOT FOUND: gs://{bucket_name}/{blob_path}")
        print("   This table may not have gone through commit_table() yet")


if __name__ == "__main__":
    # Check the test table
    check_parquet_manifest("test", "ops", "varlog")
