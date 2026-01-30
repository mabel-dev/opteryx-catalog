import json
import os
import time
import pyarrow.parquet as pq
from google.cloud import storage

import os
import sys

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))

from opteryx.draken import Vector  # type: ignore


from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry

TARGET = 'gs://opteryx_data/opteryx/ops/audit_log/data/188fa239430f10c3-59275747aed0-2.parquet'
OUT = 'artifacts/single_file_188fa239430f10c3_full.json'

_, rest = TARGET.split('://', 1)
bucket_name, path = rest.split('/', 1)
client = storage.Client()
blob = client.bucket(bucket_name).blob(path)
print('Downloading', TARGET)
data = blob.download_as_bytes()
print('Downloaded bytes:', len(data))

import pyarrow as pa

# read parquet bytes via a BufferReader
table = pq.read_table(pa.BufferReader(data))
entry = build_parquet_manifest_entry(table, TARGET, len(data)).to_dict()

out = {
    '_meta': {'dataset': 'opteryx.ops.audit_log', 'timestamp': int(time.time() * 1000), 'source': 'single-file-full-json'},
    'file_path': TARGET,
    'recomputed_full': entry,
}

# Recursively convert non-JSON types (bytes, pyarrow Buffers, etc.) to hex or JSON-safe types
import pyarrow as pa

def _hexify(obj):
    # raw bytes-like
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return obj.hex()
    # pyarrow buffers/scalars -> try to extract python value
    if hasattr(obj, "to_py"):
        try:
            val = obj.to_py()
            return _hexify(val)
        except Exception:
            try:
                # fallback: bytes representation
                return bytes(obj).hex()
            except Exception:
                return str(obj)
    # dict/list/tuple recursion
    if isinstance(obj, dict):
        return {k: _hexify(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_hexify(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_hexify(v) for v in obj)
    # primitive
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    # fallback to str
    try:
        return str(obj)
    except Exception:
        return None

safe_out = _hexify(out)

os.makedirs(os.path.dirname(OUT), exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as of:
    json.dump(safe_out, of, indent=2, ensure_ascii=False)

print('WROTE', OUT)
