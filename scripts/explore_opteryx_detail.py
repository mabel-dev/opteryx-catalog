"""Explore the full structure of opteryx data in more detail"""

import os

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

storage_client = storage.Client()
bucket = storage_client.bucket("opteryx_data")

# Explore testdata structure
print("=" * 80)
print("TESTDATA STRUCTURE")
print("=" * 80)

blobs = bucket.list_blobs(prefix="opteryx/testdata/", delimiter="/")
for blob in blobs:
    pass

testdata_dirs = set()
for prefix in blobs.prefixes:
    testdata_dirs.add(prefix)
    print(f"\n📁 {prefix}")

    # List files in each subdirectory
    sub_blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
    for sub_blob in sub_blobs:
        print(f"  - {sub_blob.name}")

# Explore ops structure
print("\n" + "=" * 80)
print("OPS STRUCTURE")
print("=" * 80)

blobs = bucket.list_blobs(prefix="opteryx/ops/", delimiter="/")
for blob in blobs:
    pass

ops_dirs = set()
for prefix in blobs.prefixes:
    ops_dirs.add(prefix)
    print(f"\n📁 {prefix}")

    # List files in each subdirectory
    sub_blobs = list(bucket.list_blobs(prefix=prefix, max_results=5))
    for sub_blob in sub_blobs:
        print(f"  - {sub_blob.name}")

print("\n" + "=" * 80)
print(f"Found {len(testdata_dirs)} testdata datasets and {len(ops_dirs)} ops datasets")
print("=" * 80)
