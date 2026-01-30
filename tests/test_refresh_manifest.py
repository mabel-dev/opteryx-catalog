import io

import pyarrow as pa
import pyarrow.parquet as pq

import os
import sys

# Add local paths to sys.path to use local code instead of installed packages
sys.path.insert(0, os.path.join(sys.path[0], ".."))  # Add parent dir for pyiceberg_firestore_gcs
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-core"))
sys.path.insert(1, os.path.join(sys.path[0], "../pyiceberg-firestore-gcs"))


from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.metadata import DatasetMetadata, Snapshot
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry
from opteryx_catalog.opteryx_catalog import OpteryxCatalog
import pytest


def test_min_k_hashes_for_string_and_binary():
    try:
        import opteryx.draken as draken  # type: ignore
    except Exception:
        pytest.skip("opteryx.draken not available")

    import pyarrow as pa

    # short binary and short string columns should get min-k
    t = _make_parquet_table([("bin", pa.binary()), ("s", pa.string())], [(b'a', 'x'), (b'b', 'y'), (b'c', 'z')])
    e = build_parquet_manifest_entry(t, "mem://f", 0)
    assert len(e.min_k_hashes[0]) > 0
    assert len(e.min_k_hashes[1]) > 0



# Step 1: Create a local catalog
catalog = OpteryxCatalog(
    "opteryx",
    firestore_project="mabeldev",
    firestore_database="catalogs",
    gcs_bucket="opteryx_data",
)

# print(catalog.load_dataset("ops.stdout_log").describe())

class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


class _MemIO:
    def __init__(self, mapping: dict):
        self._mapping = mapping

    def new_input(self, path: str):
        return _MemInput(self._mapping[path])

    def new_output(self, path: str):
        class Out:
            def __init__(self, mapping, path):
                self._buf = io.BytesIO()
                self._mapping = mapping
                self._path = path

            def write(self, data: bytes):
                self._buf.write(data)

            def close(self):
                self._mapping[self._path] = self._buf.getvalue()

            def create(self):
                return self

        return Out(self._mapping, path)


class _FakeCatalog:
    def __init__(self, io):
        self.io = io

    def write_parquet_manifest(self, snapshot_id: int, entries: list[dict], dataset_location: str) -> str:
        # Minimal manifest writer using same schema as production
        schema = pa.schema(
            [
                ("file_path", pa.string()),
                ("file_format", pa.string()),
                ("record_count", pa.int64()),
                ("file_size_in_bytes", pa.int64()),
                ("uncompressed_size_in_bytes", pa.int64()),
                ("column_uncompressed_sizes_in_bytes", pa.list_(pa.int64())),
                ("null_counts", pa.list_(pa.int64())),
                ("min_k_hashes", pa.list_(pa.list_(pa.uint64()))),
                ("histogram_counts", pa.list_(pa.list_(pa.int64()))),
                ("histogram_bins", pa.int32()),
                ("min_values", pa.list_(pa.int64())),
                ("max_values", pa.list_(pa.int64())),
                ("min_values_display", pa.list_(pa.string())),
                ("max_values_display", pa.list_(pa.string())),
            ]
        )
        normalized = []
        for ent in entries:
            if not isinstance(ent, dict):
                normalized.append(ent)
                continue
            e = dict(ent)
            e.setdefault("min_k_hashes", [])
            e.setdefault("histogram_counts", [])
            e.setdefault("histogram_bins", 0)
            e.setdefault("column_uncompressed_sizes_in_bytes", [])
            e.setdefault("null_counts", [])
            e.setdefault("min_values_display", [])
            e.setdefault("max_values_display", [])
            mv = e.get("min_values") or []
            xv = e.get("max_values") or []
            mv_disp = e.get("min_values_display") or []
            xv_disp = e.get("max_values_display") or []
            e["min_values"] = [int(v) if v is not None else None for v in mv]
            e["max_values"] = [int(v) if v is not None else None for v in xv]
            e["min_values_display"] = [str(v) if v is not None else None for v in mv_disp]
            e["max_values_display"] = [str(v) if v is not None else None for v in xv_disp]
            normalized.append(e)

        table = pa.Table.from_pylist(normalized, schema=schema)
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression="zstd")
        data = buf.getvalue().to_pybytes()
        path = f"{dataset_location}/metadata/manifest-{snapshot_id}.parquet"
        out = self.io.new_output(path).create()
        out.write(data)
        out.close()
        return path


def _make_parquet_table(columns: list[tuple[str, pa.DataType]], rows: list[tuple]):
    arrays = []
    for i, (name, dtype) in enumerate(columns):
        col_vals = [r[i] for r in rows]
        arrays.append(pa.array(col_vals, type=dtype))
    return pa.Table.from_arrays(arrays, names=[c[0] for c in columns])


def test_refresh_manifest_with_single_file():
    # single file with columns a,b for quick iteration
    t1 = _make_parquet_table([("a", pa.int64()), ("b", pa.int64())], [(1, 10), (2, 20)])

    # Write parquet file to mem
    buf = pa.BufferOutputStream()
    pq.write_table(t1, buf, compression="zstd")
    d1 = buf.getvalue().to_pybytes()

    f1 = "mem://data/f1.parquet"
    manifest_path = "mem://manifest-old"

    # Build initial manifest entry for single file
    e1 = build_parquet_manifest_entry(t1, f1, len(d1)).to_dict()

    # Create in-memory IO mapping including manifest and data file
    mapping = {f1: d1}

    # Write initial manifest with the single entry using the same writer as the catalog
    fake_writer = _FakeCatalog(_MemIO(mapping))
    manifest_path = fake_writer.write_parquet_manifest(1, [e1], "mem://")
    # Ensure the manifest bytes are present in the mapping
    mapping[manifest_path] = mapping[manifest_path]

    # Persist the single-file manifest as JSON for quick inspection during
    # iterative debugging (writes to repo `artifacts/` so you can open it).
    import os, json

    artifacts_dir = os.path.join(os.getcwd(), "artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    with open(os.path.join(artifacts_dir, "single_file_manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(e1, fh, indent=2, default=str)

    # Create metadata and snapshot
    meta = DatasetMetadata(dataset_identifier="tests_temp.test", location="mem://", schema=None, properties={})
    meta.schemas.append({"schema_id": "s1", "columns": [{"name": "a"}, {"name": "b"}]})
    meta.current_schema_id = "s1"
    snap = Snapshot(snapshot_id=1, timestamp_ms=1, manifest_list=manifest_path)
    meta.snapshots.append(snap)
    meta.current_snapshot_id = 1

    ds = SimpleDataset(identifier="tests_temp.test", _metadata=meta)
    ds.io = _MemIO(mapping)
    ds.catalog = _FakeCatalog(ds.io)

    # Refresh manifest (should re-read f1 and write a new manifest)
    new_snap_id = ds.refresh_manifest(agent="test-agent", author="tester")
    assert new_snap_id is not None

    # Describe should include both columns and count bytes appropriately
    desc = ds.describe()
    assert "a" in desc
    assert "b" in desc

    # ensure uncompressed bytes are present and non-zero for both cols
    assert desc["a"]["uncompressed_bytes"] > 0
    assert desc["b"]["uncompressed_bytes"] > 0
