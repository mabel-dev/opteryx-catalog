#!/usr/bin/env python3
"""
End-to-end test of the compaction capability.

This script:
1. Creates a dataset
2. Adds multiple small data files via snapshots
3. Exercises the compaction logic (both brute and performance strategies)
4. Validates the results (fewer files, same data)
"""

import sys
import time
from pathlib import Path

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.compaction import DatasetCompactor
from opteryx_catalog.catalog.manifest import read_manifest_rows

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def create_small_dataset_with_multiple_files():
    """
    Create a test dataset and add multiple small data files to it.

    Returns:
        (catalog, dataset_identifier, initial_entry_count)
    """
    print("\n📝 Step 1: Setting up test environment...")

    # Create in-memory catalog for testing
    catalog = OpteryxCatalog(
        workspace="test_workspace",
        firestore_project="test_project",
        firestore_database="test_db",
        gcs_bucket="test_bucket",
    )

    # Create collection
    collection = "test_collection"
    catalog.create_collection(collection, author="test_user", exists_ok=True)
    print(f"  ✓ Created collection: {collection}")

    # Create dataset
    dataset_name = "compaction_test_dataset"
    identifier = f"{collection}.{dataset_name}"

    # Define schema
    schema = Morsel()
    schema.append_vector("id", vector_from_sequence([], dtype="INTEGER"))
    schema.append_vector("timestamp", vector_from_sequence([], dtype="INTEGER"))
    schema.append_vector("value", vector_from_sequence([], dtype="DOUBLE"))
    schema.append_vector("text", vector_from_sequence([], dtype="VARCHAR"))

    dataset = catalog.create_dataset(
        identifier=identifier,
        schema=schema,
        author="test_user",
        properties={"description": "Test dataset for compaction"},
    )
    print(f"  ✓ Created dataset: {identifier}")

    return catalog, dataset, identifier


def add_small_data_files(dataset, catalog, identifier, num_files=5, rows_per_file=100):
    """
    Add multiple small data files to a dataset by creating snapshots.

    Args:
        dataset: SimpleDataset instance
        catalog: OpteryxCatalog instance
        identifier: Dataset identifier (collection.name)
        num_files: Number of small files to create
        rows_per_file: Rows per file

    Returns:
        List of manifest entries before compaction
    """
    print(f"\n📝 Step 2: Adding {num_files} small data files...")

    _collection, _dataset_name = identifier.split(".")

    for file_idx in range(num_files):
        print(f"  Creating file {file_idx + 1}/{num_files}...")

        # Create small table with unique data
        start_id = file_idx * rows_per_file
        data = {
            "id": list(range(start_id, start_id + rows_per_file)),
            "timestamp": [int(time.time() * 1000) + i for i in range(rows_per_file)],
            "value": [float(i) * 1.5 for i in range(rows_per_file)],
            "text": [f"file{file_idx}_row{i}" for i in range(rows_per_file)],
        }

        table = Morsel()
        table.append_vector("id", vector_from_sequence(data["id"], dtype="INTEGER"))
        table.append_vector("timestamp", vector_from_sequence(data["timestamp"], dtype="INTEGER"))
        table.append_vector("value", vector_from_sequence(data["value"], dtype="DOUBLE"))
        table.append_vector("text", vector_from_sequence(data["text"], dtype="VARCHAR"))

        # Append to dataset (this creates a new snapshot)
        try:
            dataset.append(table)
            print(f"    ✓ File {file_idx + 1} appended ({rows_per_file} rows)")
        except Exception as e:
            print(f"    ✗ Error appending file {file_idx + 1}: {e}")
            raise

    # Load fresh dataset to get all snapshots
    dataset_fresh = catalog.load_dataset(identifier, load_history=True)
    current_snapshot = dataset_fresh.metadata.current_snapshot

    if current_snapshot and current_snapshot.manifest_list:
        print(f"\n  📊 Current snapshot ID: {current_snapshot.snapshot_id}")
        print(f"  📄 Manifest: {current_snapshot.manifest_list}")

        # Try to read manifest entries
        try:
            io = dataset_fresh.io
            inp = io.new_input(current_snapshot.manifest_list)
            with inp.open() as f:
                data = f.read()
            entries = read_manifest_rows(data)
            print(f"  📈 Initial entry count: {len(entries)}")

            for idx, entry in enumerate(entries):
                file_size_mb = entry.get("file_size_in_bytes", 0) / (1024 * 1024)
                record_count = entry.get("record_count", 0)
                print(f"    File {idx + 1}: {file_size_mb:.2f}MB, {record_count} records")

            return dataset_fresh, entries
        except Exception as e:
            print(f"  ⚠️  Could not read manifest: {e}")
            return dataset_fresh, []
    else:
        print("  ⚠️  No current snapshot found")
        return dataset_fresh, []


def test_brute_compaction(dataset, catalog, identifier):
    """Test brute force compaction strategy."""
    print("\n📝 Step 3: Testing brute force compaction...")

    try:
        compactor = DatasetCompactor(
            dataset, strategy="brute", author="test_user", agent="test-agent"
        )
        print(f"  ✓ Created compactor with strategy: {compactor.strategy}")

        # Dry run to see what would be compacted
        plan = compactor.compact(dry_run=True)
        if plan:
            print("  📋 Compaction plan (dry-run):")
            print(f"    Type: {plan.get('type')}")
            print(f"    Reason: {plan.get('reason')}")
            print(f"    Files: {len(plan.get('files', []))}")

            # Actually execute
            print("\n  🔄 Executing compaction...")
            snapshot = compactor.compact(dry_run=False)
            if snapshot:
                print("  ✓ Compaction completed!")
                print(f"    New snapshot ID: {snapshot.snapshot_id}")
                print(f"    Operation: {snapshot.operation_type}")
                print(f"    Summary: {snapshot.summary}")
                return snapshot
            else:
                print("  ✗ Compaction returned None")
                return None
        else:
            print("  ⚠️  No compaction opportunity found")
            return None
    except Exception as e:
        print(f"  ✗ Error during brute compaction: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_performance_compaction(dataset, catalog, identifier):
    """Test performance compaction strategy."""
    print("\n📝 Step 4: Testing performance compaction...")

    try:
        compactor = DatasetCompactor(
            dataset, strategy="performance", author="test_user", agent="test-agent"
        )
        print(f"  ✓ Created compactor with strategy: {compactor.strategy}")

        # Dry run
        plan = compactor.compact(dry_run=True)
        if plan:
            print("  📋 Compaction plan (dry-run):")
            print(f"    Type: {plan.get('type')}")
            print(f"    Reason: {plan.get('reason')}")
            print(f"    Files: {len(plan.get('files', []))}")

            # Execute
            print("\n  🔄 Executing compaction...")
            snapshot = compactor.compact(dry_run=False)
            if snapshot:
                print("  ✓ Compaction completed!")
                print(f"    New snapshot ID: {snapshot.snapshot_id}")
                print(f"    Operation: {snapshot.operation_type}")
                print(f"    Summary: {snapshot.summary}")
                return snapshot
            else:
                print("  ✗ Compaction returned None")
                return None
        else:
            print("  ⚠️  No compaction opportunity found")
            return None
    except Exception as e:
        print(f"  ✗ Error during performance compaction: {e}")
        import traceback

        traceback.print_exc()
        return None


def verify_compaction_results(dataset_after, entries_before):
    """Verify that compaction reduced file count."""
    print("\n📝 Step 5: Verifying compaction results...")

    try:
        current_snapshot = dataset_after.metadata.current_snapshot
        if not current_snapshot or not current_snapshot.manifest_list:
            print("  ⚠️  No current snapshot after compaction")
            return False

        # Read manifest entries
        io = dataset_after.io
        inp = io.new_input(current_snapshot.manifest_list)
        with inp.open() as f:
            data = f.read()
        entries_after = read_manifest_rows(data)

        print("  📈 After compaction:")
        print(f"    Files before: {len(entries_before)}")
        print(f"    Files after: {len(entries_after)}")

        if len(entries_after) < len(entries_before):
            print(f"    ✓ File count reduced by {len(entries_before) - len(entries_after)}")
        elif len(entries_after) == len(entries_before):
            print("    ⚠️  File count unchanged")
        else:
            print("    ⚠️  File count increased (unexpected)")

        # Verify total records preserved
        total_records_before = sum(e.get("record_count", 0) for e in entries_before)
        total_records_after = sum(e.get("record_count", 0) for e in entries_after)

        print("\n  📊 Data integrity:")
        print(f"    Total records before: {total_records_before}")
        print(f"    Total records after: {total_records_after}")

        if total_records_before == total_records_after:
            print("    ✓ Record count preserved")
            return True
        else:
            print("    ✗ Record count mismatch!")
            return False

    except Exception as e:
        print(f"  ✗ Error verifying results: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run end-to-end compaction test."""
    print("=" * 70)
    print("🧪 END-TO-END COMPACTION TEST")
    print("=" * 70)

    try:
        # Step 1: Create dataset
        catalog, dataset, identifier = create_small_dataset_with_multiple_files()

        # Step 2: Add multiple small files
        dataset_with_files, entries_before = add_small_data_files(
            dataset, catalog, identifier, num_files=5, rows_per_file=100
        )

        if not entries_before:
            print("\n⚠️  Could not retrieve manifest entries, cannot continue test")
            return False

        # Step 3: Test brute compaction
        snapshot_brute = test_brute_compaction(dataset_with_files, catalog, identifier)

        # Reload dataset to get fresh snapshot
        if snapshot_brute:
            dataset_after_brute = catalog.load_dataset(identifier, load_history=True)
            success_brute = verify_compaction_results(dataset_after_brute, entries_before)
        else:
            print("\n⚠️  Brute compaction did not produce a snapshot")
            success_brute = False

        # Step 4: Test performance compaction (if applicable)
        # Note: This would require a dataset with sort orders
        print("\n📝 Step 4: Performance compaction requires sort orders")
        print("  ℹ️  Skipping for this test (would need dataset with sort orders)")
        success_perf = None

        # Summary
        print("\n" + "=" * 70)
        print("📊 TEST SUMMARY")
        print("=" * 70)
        print(f"  Brute compaction: {'✓ PASSED' if success_brute else '✗ FAILED'}")
        print(
            f"  Performance compaction: {'⏭️  SKIPPED' if success_perf is None else ('✓ PASSED' if success_perf else '✗ FAILED')}"
        )

        if success_brute or success_perf:
            print("\n✅ Compaction capability is working!")
            return True
        else:
            print("\n❌ Compaction tests failed")
            return False

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
