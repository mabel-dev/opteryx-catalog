"""`describe()` against manifest entries whose statistics do not line up.

The per-column stat arrays on a manifest entry are supposed to be parallel to
the schema, but entries written by an older version - or by a writer that saw
a different column set - can be short, scalar, or carry values that will not
convert. That used to be absorbed by a dozen `except Exception: pass` handlers
inside the accumulator loop, which also hid real errors. The handling is now
explicit (`_at` / `_as_int`), so these tests pin the behaviour it has to keep:
a missing statistic costs that column a pruning hint and nothing more, and
describe() still returns a full row per schema column.
"""

import io
from itertools import count

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.dataset import _decode_minmax
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot

# `get_arrow_manifest` caches parsed manifests by path for the life of the
# process, so every manifest built here needs a path of its own or the second
# test in a run silently describes the first one's data.
_manifest_paths = count()


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


def _manifest_bytes(columns: dict) -> bytes:
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    morsel = Morsel()
    for name, (values, dtype) in columns.items():
        morsel.append_vector(name, vector_from_sequence(values, dtype=dtype))
    return write_parquet(morsel)


def _describe(columns: dict, schema_columns: list[str], **kwargs) -> dict:
    manifest_path = f"mem://ragged-stats-{next(_manifest_paths)}"
    meta = DatasetMetadata(
        dataset_identifier="tests_temp.test",
        location="mem://",
        schema=None,
        properties={},
    )
    meta.schemas.append({"schema_id": "s1", "columns": [{"name": name} for name in schema_columns]})
    meta.current_schema_id = "s1"
    meta.snapshots.append(Snapshot(snapshot_id=1, timestamp_ms=1, manifest_list=manifest_path))
    meta.current_snapshot_id = 1

    dataset = SimpleDataset(identifier="tests_temp.test", _metadata=meta)
    dataset.io = _MemIO({manifest_path: _manifest_bytes(columns)})
    return dataset.describe(**kwargs)


def _two_file_entry(**overrides) -> dict:
    """Two manifest entries covering two columns, with sane stats throughout."""
    columns = {
        "file_path": (["f1.parquet", "f2.parquet"], "VARCHAR"),
        "file_format": (["parquet", "parquet"], "VARCHAR"),
        "record_count": ([10, 20], "INTEGER"),
        "file_size_in_bytes": ([100, 200], "INTEGER"),
        "uncompressed_size_in_bytes": ([1000, 2000], "INTEGER"),
        "column_uncompressed_sizes_in_bytes": ([[100, 400], [300, 200]], "ARRAY"),
        "null_counts": ([[0, 1], [2, 3]], "ARRAY"),
        "min_k_hashes": ([[1, 2], [3]], "ARRAY"),
        "histogram_counts": ([[1, 2], [3, 4]], "ARRAY"),
        "histogram_bins": ([32, 32], "INTEGER"),
        "min_values": ([[10, 20], [5, 30]], "ARRAY"),
        "max_values": ([[100, 400], [300, 200]], "ARRAY"),
        "min_values_display": ([[None, None], [None, None]], "ARRAY"),
        "max_values_display": ([[None, None], [None, None]], "ARRAY"),
    }
    columns.update(overrides)
    return columns


def test_short_stat_arrays_do_not_break_describe():
    """A third schema column with no statistics behind it still gets a row."""
    desc = _describe(_two_file_entry(), ["a", "b", "c"])

    assert set(desc) == {"a", "b", "c"}
    # 'c' has no data in any of the parallel arrays
    assert desc["c"]["null_count"] == 0
    assert desc["c"]["uncompressed_bytes"] == 0
    assert desc["c"]["min"] is None
    assert desc["c"]["max"] is None
    assert desc["c"]["cardinality"] == 0
    # ...and the columns that DO have statistics are unaffected by it
    assert desc["a"]["uncompressed_bytes"] == 400
    assert desc["b"]["uncompressed_bytes"] == 600
    assert desc["a"]["null_count"] == 2
    assert desc["b"]["null_count"] == 4


def test_ragged_rows_contribute_what_they_have():
    """One entry missing the second column's stats does not void the first's."""
    desc = _describe(
        _two_file_entry(
            null_counts=([[5, 7], [11]], "ARRAY"),
            column_uncompressed_sizes_in_bytes=([[100, 400], [300]], "ARRAY"),
        ),
        ["a", "b"],
    )

    assert desc["a"]["null_count"] == 16  # 5 + 11, both entries have index 0
    assert desc["b"]["null_count"] == 7  # only the first entry reaches index 1
    assert desc["a"]["uncompressed_bytes"] == 400
    assert desc["b"]["uncompressed_bytes"] == 400


def test_decode_minmax_handles_every_stored_shape():
    """The min/max decoder is fed numbers, text and raw bytes from storage.

    The Parquet writer used above cannot put bytes inside a stats array, so
    this exercises the decoder directly - the byte-encoded bounds it has to
    read come from writers outside this package.
    """
    # numbers pass through
    assert _decode_minmax(10) == 10
    assert _decode_minmax(1.5) == 1.5
    # text that looks numeric compares numerically; text that doesn't stays text
    assert _decode_minmax("10") == 10
    assert _decode_minmax("1.5") == 1.5
    assert _decode_minmax("apple") == "apple"
    # UTF-8 bytes decode the same way
    assert _decode_minmax(b"10") == 10
    assert _decode_minmax(b"apple") == "apple"
    assert _decode_minmax(bytearray(b"apple")) == "apple"
    assert _decode_minmax(memoryview(b"apple")) == "apple"
    # a trailing 0xFF marks a bound truncated to fit, and is not part of it
    assert _decode_minmax(b"apple\xff") == "apple"
    # nothing usable is None - never 0, which would read as a real bound
    assert _decode_minmax(None) is None
    assert _decode_minmax(b"\xff\xfe\x00") is None
    assert _decode_minmax(object()) is None


def test_numeric_strings_are_still_read_as_numbers():
    """Min/max written as text by an older writer keep comparing numerically."""
    desc = _describe(
        _two_file_entry(
            min_values=([["10", "20"], ["5", "30"]], "ARRAY"),
            max_values=([["100", "400"], ["300", "200"]], "ARRAY"),
        ),
        ["a", "b"],
    )

    assert desc["a"]["min"] == 5
    assert desc["a"]["max"] == 300
    assert desc["b"]["min"] == 20
    assert desc["b"]["max"] == 400
