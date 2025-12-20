"""Quick script to explore the structure of gs://opteryx_data/opteryx/"""

import os

from google.cloud import storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/Users/justin/Nextcloud/mabel/mabeldev-b37f651c2916.json"
)

storage_client = storage.Client()
bucket = storage_client.bucket("opteryx_data")

print("Exploring gs://opteryx_data/opteryx/\n")
print("=" * 80)

# List all blobs under opteryx/ prefix
blobs = bucket.list_blobs(prefix="opteryx/", delimiter="/")

# Get immediate subdirectories
prefixes = set()
for blob in blobs:
    pass

for prefix in blobs.prefixes:
    prefixes.add(prefix)
    print(f"📁 {prefix}")

print("\n" + "=" * 80)
print(f"\nFound {len(prefixes)} top-level directories under opteryx/")

# Sample some files from each directory
print("\nSampling files from each directory:\n")
for prefix in sorted(prefixes):
    blobs = list(bucket.list_blobs(prefix=prefix, max_results=3))
    print(f"\n{prefix}:")
    for blob in blobs:
        print(f"  - {blob.name}")
