#!/usr/bin/env python3
"""Find orphaned Parquet manifest files for a dataset.

Usage: python scripts/find_orphaned_manifests.py <collection.dataset>

This prints manifest files that exist in storage under <dataset_location>/metadata
but are not referenced by any snapshot document.
"""

import sys
from pathlib import Path

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.deep_clean import DatasetDeepClean

# Ensure local package resolution like other scripts
sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    if len(sys.argv) < 2:
        print("Usage: find_orphaned_manifests.py <collection.dataset>")
        raise SystemExit(1)

    identifier = sys.argv[1]
    # Prefer explicit workspace if provided by env or CLI; fall back to OpteryxCatalog() when available
    import os

    catalog = None
    # Allow optional second arg as workspace
    if len(sys.argv) > 2:
        wk = sys.argv[2]
        catalog = OpteryxCatalog(workspace=wk)
    else:
        try:
            catalog = OpteryxCatalog()
        except TypeError:
            wk = os.environ.get("OPTERYX_WORKSPACE") or os.environ.get("WORKSPACE") or "default"
            catalog = OpteryxCatalog(workspace=wk)

    cleaner = DatasetDeepClean(catalog)

    orphaned = cleaner.get_orphaned_manifests(identifier)
    if not orphaned:
        print(f"No orphaned manifest files found for {identifier}")
        return

    print(f"Orphaned manifests for {identifier} ({len(orphaned)}):")
    for m in sorted(orphaned):
        print(m)


if __name__ == "__main__":
    main()
