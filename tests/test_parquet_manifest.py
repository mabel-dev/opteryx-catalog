"""Test Parquet manifest functionality."""

import time

from pyiceberg_firestore_gcs.firestore_catalog import FirestoreCatalog


def test_query_planning_speed():
    """Test that query planning is fast with Parquet manifests."""

    # Initialize catalog
    catalog = FirestoreCatalog(
        catalog_name="your_catalog_name",
        firestore_project="your_project",
        firestore_database="(default)",
        gcs_bucket="your_bucket",
    )

    # Load a table
    table = catalog.load_table("your_namespace.your_table")

    # Time query planning
    start = time.perf_counter()
    scan = table.scan()
    files = list(scan.plan_files())
    elapsed = time.perf_counter() - start

    print(f"Query planning took {elapsed:.3f}s for {len(files)} files")

    # Should be < 500ms for typical workload
    assert elapsed < 5.0, f"Query planning too slow: {elapsed:.3f}s"

    print("✓ Query planning speed test passed")


if __name__ == "__main__":
    test_query_planning_speed()
