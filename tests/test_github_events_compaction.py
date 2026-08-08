#!/usr/bin/env python3
"""
Test compaction planning against a simulated public.github.events dataset.

github.events is a large public dataset with many small files organized by date.
This script simulates that structure and shows what compaction would do.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))


from opteryx_catalog.catalog.compaction import DatasetCompactor


class MockDataset:
    """Minimal mock dataset for compaction planning."""

    def __init__(self, files):
        self.files = files
        self.metadata = None
        self.io = None


class MockCompactor(DatasetCompactor):
    """Compactor that works directly with file list for testing."""

    def __init__(self, files, strategy=None):
        # Create mock dataset
        dataset = MockDataset(files)
        self.dataset = dataset
        self.author = "test"
        self.agent = "github_events_test"
        self.strategy = strategy or "brute"
        self.decision = "user"
        self.sort_column_id = None
        self.files = files

    def _read_manifest(self, manifest_path):
        """Return our test files instead of reading from disk."""
        return self.files

    def get_compaction_plan(self):
        """Get plan without needing full dataset metadata.

        Mirrors compact(): rule A (brute, <512MB) and rule B (sort-aware,
        >500MB) are independent per pass. Returns whichever is found first
        (brute preferred for display) to keep this demo's single-plan-dict
        callers working; a real pass would execute both if both are present.
        """
        if self.strategy == "brute":
            return self._select_brute_compaction(self.files)
        return self._select_brute_merge(self.files) or self._select_sort_aware_merge(self.files)


def simulate_github_events():
    """
    Simulate public.github.events structure.

    github.events typically has:
    - 1000s of small parquet files (~20-40 MB each)
    - Organized by date and event type
    - Recent data comes in as small appends
    """
    files = []

    # Simulate 150 small files (typical for a day's worth of github events)
    # Each file ~30-40 MB uncompressed (github has many events daily)
    for i in range(150):
        date = f"2024-{(i // 30) % 12 + 1:02d}-{(i % 30) + 1:02d}"
        event_type = ["PushEvent", "CreateEvent", "PullRequestEvent", "IssuesEvent"][i % 4]

        files.append(
            {
                "file_path": f"gs://github_events/{date}/{event_type}/{i:04d}.parquet",
                "file_format": "PARQUET",
                "record_count": 500000 + i * 100,  # ~500k-550k records per file
                "file_size_in_bytes": (25 + i % 15) * 1024 * 1024,  # 25-40 MB compressed
                "uncompressed_size_in_bytes": (35 + i % 20) * 1024 * 1024,  # 35-55 MB uncompressed
                "lower_bounds": {
                    "id": i * 100,
                    "created_at": f"2024-{(i // 30) % 12 + 1:02d}-{(i % 30) + 1:02d}T00:00:00Z",
                },
                "upper_bounds": {
                    "id": (i + 1) * 100,
                    "created_at": f"2024-{(i // 30) % 12 + 1:02d}-{(i % 30) + 1:02d}T23:59:59Z",
                },
            }
        )

    return files


def simulate_github_events_with_large_files():
    """Variant with some overly-large files that should be split."""
    files = simulate_github_events()

    # Add a few large files (result of failed compaction or batch load)
    for i in range(3):
        files.append(
            {
                "file_path": f"gs://github_events/bulk_load_{i}.parquet",
                "file_format": "PARQUET",
                "record_count": 5000000,
                "file_size_in_bytes": 400 * 1024 * 1024,  # 400 MB compressed
                "uncompressed_size_in_bytes": 600 * 1024 * 1024,  # 600 MB uncompressed
                "lower_bounds": {"id": 0, "created_at": "2024-01-01T00:00:00Z"},
                "upper_bounds": {"id": 999999, "created_at": "2024-12-31T23:59:59Z"},
            }
        )

    return files


def print_analysis(files, title):
    """Analyze and print file distribution."""
    print(f"\n{'=' * 70}")
    print(f"{title}")
    print(f"{'=' * 70}")

    total_files = len(files)
    total_uncompressed = sum(f["uncompressed_size_in_bytes"] for f in files)
    total_compressed = sum(f["file_size_in_bytes"] for f in files)

    small_files = [f for f in files if f["uncompressed_size_in_bytes"] < 64 * 1024 * 1024]
    large_files = [f for f in files if f["uncompressed_size_in_bytes"] >= 500 * 1024 * 1024]
    acceptable = [
        f
        for f in files
        if 500 * 1024 * 1024 <= f["uncompressed_size_in_bytes"] <= 525 * 1024 * 1024
    ]

    print("\nDataset Summary:")
    print(f"  Total files: {total_files}")
    print(
        f"  Total uncompressed: {total_uncompressed / 1024 / 1024:.1f} MB ({total_uncompressed / 1024 / 1024 / 1024:.2f} GB)"
    )
    print(
        f"  Total compressed: {total_compressed / 1024 / 1024:.1f} MB ({total_compressed / 1024 / 1024 / 1024:.2f} GB)"
    )
    print(f"  Compression ratio: {total_compressed / total_uncompressed * 100:.1f}%")

    print("\nFile Size Distribution:")
    print(
        f"  Small files (< 64 MB): {len(small_files)} ({len(small_files) / total_files * 100:.1f}%)"
    )
    if small_files:
        avg_small = (
            sum(f["uncompressed_size_in_bytes"] for f in small_files)
            / len(small_files)
            / 1024
            / 1024
        )
        print(f"    Average: {avg_small:.1f} MB")

    print(
        f"  Acceptable (500-525 MB): {len(acceptable)} ({len(acceptable) / total_files * 100:.1f}%)"
    )
    print(f"  Large (≥ 500 MB): {len(large_files)} ({len(large_files) / total_files * 100:.1f}%)")
    if large_files:
        avg_large = (
            sum(f["uncompressed_size_in_bytes"] for f in large_files)
            / len(large_files)
            / 1024
            / 1024
        )
        print(f"    Average: {avg_large:.1f} MB")


def simulate_compaction_iterations(files, max_iterations=10):
    """Simulate running compaction repeatedly until no more changes."""
    print(f"\n{'=' * 70}")
    print("Running Compaction Iterations (Dry Run)")
    print(f"{'=' * 70}\n")

    iteration = 0
    current_files = files.copy()

    while iteration < max_iterations:
        print(f"--- Iteration {iteration + 1} ({len(current_files)} files) ---")

        compactor = MockCompactor(current_files, strategy="brute")
        plan = compactor.get_compaction_plan()

        if not plan:
            print("✓ No more compaction needed\n")
            break

        print(f"Plan: {plan['type'].upper()}")
        print(f"Reason: {plan['reason']}")
        print(f"Files to process: {len(plan['files'])}")

        total_size = sum(f["uncompressed_size_in_bytes"] for f in plan["files"]) / 1024 / 1024
        print(f"Combined size: {total_size:.1f} MB")

        if plan["type"] == "combine":
            # Simulate combining files
            print(f"→ Would combine into 1 file of ~{total_size:.0f} MB\n")
            # Remove old files, add new one
            for f in plan["files"]:
                current_files.remove(f)
            current_files.append(
                {
                    "file_path": f"gs://github_events/compacted_{iteration}.parquet",
                    "file_format": "PARQUET",
                    "record_count": sum(f["record_count"] for f in plan["files"]),
                    "file_size_in_bytes": int(
                        total_size * 1024 * 1024 * 0.7
                    ),  # Estimate compressed
                    "uncompressed_size_in_bytes": int(total_size * 1024 * 1024),
                }
            )

        elif plan["type"] == "split":
            # Simulate splitting large file
            print(f"→ Would split into ~{int(total_size / 512) + 1} files of ~512 MB each\n")
            # Remove old file, simulate 2 output files
            current_files.remove(plan["files"][0])
            for j in range(2):
                current_files.append(
                    {
                        "file_path": f"gs://github_events/split_{iteration}_{j}.parquet",
                        "file_format": "PARQUET",
                        "record_count": plan["files"][0]["record_count"] // 2,
                        "file_size_in_bytes": plan["files"][0]["file_size_in_bytes"] // 2,
                        "uncompressed_size_in_bytes": total_size / 2 * 1024 * 1024 / 1,
                    }
                )

        iteration += 1

    return current_files


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("GitHub Events Compaction Simulation")
    print("=" * 70)

    # Scenario 1: Many small files (realistic github.events snapshot)
    files = simulate_github_events()
    print_analysis(files, "Scenario 1: Typical GitHub Events (150 small files)")

    # Show what one compaction step would do
    compactor = MockCompactor(files, strategy="brute")
    plan = compactor.get_compaction_plan()
    if plan:
        print("\nFirst Compaction Action:")
        print(f"  Type: {plan['type']}")
        print(f"  Files: {len(plan['files'])}")
        total = sum(f["uncompressed_size_in_bytes"] for f in plan["files"]) / 1024 / 1024
        print(f"  Total size: {total:.1f} MB")

    # Scenario 2: Mixed small and large files
    files = simulate_github_events_with_large_files()
    print_analysis(files, "\nScenario 2: With Oversized Files")

    # Simulate multiple iterations
    result = simulate_compaction_iterations(files)
    print(f"Final result: {len(result)} files")
    final_total = sum(f["uncompressed_size_in_bytes"] for f in result) / 1024 / 1024 / 1024
    print(f"Final total: {final_total:.2f} GB uncompressed")
