import os
import sys
import traceback

# Now run opteryx sequence
import opteryx
from opteryx.connectors.iceberg_connector import IcebergConnector

from pyiceberg_firestore_gcs import FirestoreCatalog

# Ensure local packages are preferred
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
sys.path.insert(
    1,
    os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "opteryx-core")),
)

# Patch AvroFile to print stack
try:
    import pyiceberg.avro.file as avf

    _orig = avf.AvroFile.__enter__

    def _patched(self, *a, **kw):
        print("--- AvroFile.__enter__ stack (most recent call last) ---", file=sys.stderr)
        traceback.print_stack(limit=40, file=sys.stderr)
        print("--- end stack ---", file=sys.stderr)
        return _orig(self, *a, **kw)

    avf.AvroFile.__enter__ = _patched
except Exception as e:
    print("Failed to patch AvroFile:", e, file=sys.stderr)

print("Opteryx version:", getattr(opteryx, "__version__", None))

opteryx.set_default_connector(
    IcebergConnector,
    catalog=FirestoreCatalog,
    firestore_project=os.environ.get("GCP_PROJECT_ID"),
    firestore_database=os.environ.get("FIRESTORE_DATABASE"),
    gcs_bucket=os.environ.get("GCS_BUCKET"),
)

# Run query
print("Running query...")
df = opteryx.query_to_arrow("SELECT * FROM personal.bastian.tt")
print("Query returned rows:", len(df))
print(df)
