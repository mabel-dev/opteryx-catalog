"""The streaming data file writer and the entries it builds as it writes.

Three things are pinned here.

1. The accumulator describes a file fed one row group at a time EXACTLY as the
   whole-morsel builder describes the same rows in one piece, for every
   statistic that merges exactly - which is every one but the histogram - and
   for the histogram too when exact mode is asked for.
2. In bounded mode the histogram conserves its total and its bounds, and lands
   within one fine bin of the exact answer, while retaining nothing per row.
3. A file written through `open_data_file_writer` is a real multi-row-group
   parquet file, and `compaction_commit(entries=...)` registers it WITHOUT
   reading it back - the read-back was a full extra pass over every byte
   compaction wrote.
"""

import copy
import io
import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from rugo.parquet import read_metadata
from rugo.parquet import read_parquet

from opteryx_catalog.catalog.dataset import SimpleDataset
from opteryx_catalog.catalog.manifest import FINE_HISTOGRAM_BINS
from opteryx_catalog.catalog.manifest import HISTOGRAM_BINS
from opteryx_catalog.catalog.manifest import ParquetManifestEntryAccumulator
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_morsel
from opteryx_catalog.catalog.manifest import clear_parsed_manifest_cache
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

LOCATION = "mem://ws/stream"


# ---------------------------------------------------------------------------
# Fixtures (same shapes as test_mor_deletes.py, plus a read counter and abort)
# ---------------------------------------------------------------------------


class _MemInput:
    def __init__(self, data: bytes):
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


class _MemIO:
    def __init__(self, mapping: dict):
        self._mapping = mapping
        self.reads: list = []
        self.aborted: list = []

    def new_input(self, path: str):
        self.reads.append(path)
        return _MemInput(self._mapping[path])

    def new_output(self, path: str):
        mem = self

        class Out:
            def __init__(self):
                self._buf = io.BytesIO()
                self._closed = False

            def write(self, data: bytes):
                if self._closed:
                    raise ValueError("write after close")
                self._buf.write(data)

            def close(self):
                self._closed = True
                mem._mapping[path] = self._buf.getvalue()

            def abort(self):
                self._closed = True
                mem.aborted.append(path)

            def create(self):
                return self

        return Out()


class _FakeCatalog:
    def __init__(self, io):
        self.io = io
        self.saved_snapshots = []
        self.saved_metadata = []

    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest

    def save_snapshot(self, identifier, snapshot):
        self.saved_snapshots.append((identifier, snapshot))

    def save_dataset_metadata(self, identifier, metadata, **kwargs):
        self.saved_metadata.append((identifier, copy.deepcopy(metadata)))


def _morsel(ints, strs=None, bools=None):
    m = Morsel()
    m.append_vector("i", vector_from_sequence(ints, dtype="INTEGER"))
    if strs is not None:
        m.append_vector("s", vector_from_sequence(strs, dtype="VARCHAR"))
    if bools is not None:
        m.append_vector("b", vector_from_sequence(bools, dtype="BOOLEAN"))
    return m


def _row_groups():
    """Three row groups with overlapping, differently placed int ranges."""
    return [
        _morsel([5, 1, 9, None, 3], ["a", "bb", None, "dddd", "e"], [True, False, True, None, True]),
        _morsel([100, 42, 7, 8, 8], ["zz", "y", "y", "", "x"], [False, False, None, True, True]),
        _morsel([-4, 50, 50, 50, 2], ["m", "n", "o", "p", "q"], [True, True, True, True, False]),
    ]


def _concat(morsels):
    return Morsel.combine(list(morsels))


def _seed_dataset(files: dict):
    clear_parsed_manifest_cache()
    storage: dict = {}
    mem_io = _MemIO(storage)
    catalog = _FakeCatalog(mem_io)

    from rugo.parquet import write_parquet

    entries = []
    for path, values in files.items():
        data = write_parquet(_morsel(values), compression="zstd")
        storage[path] = data
        entries.append(build_parquet_manifest_entry_from_bytes(data, path, len(data)).to_dict())

    snapshot_id = 1000
    manifest_path = catalog.write_parquet_manifest(snapshot_id, entries, LOCATION)
    meta = DatasetMetadata(dataset_identifier="col.stream", location=LOCATION, schema=None, properties={})
    meta.snapshots.append(
        Snapshot(
            snapshot_id=snapshot_id,
            timestamp_ms=snapshot_id,
            author="seed",
            sequence_number=1,
            user_created=True,
            operation_type="append",
            manifest_list=manifest_path,
        )
    )
    meta.current_snapshot_id = snapshot_id
    ds = SimpleDataset(identifier="col.stream", _metadata=meta)
    ds.io = mem_io
    ds.catalog = catalog
    mem_io.reads.clear()
    return ds, storage, mem_io


def _assert_sizes_match_up_to_bitmap_padding(parts: dict, whole: dict, row_groups: int):
    """Byte sizes are an in-memory footprint summed per row group: validity
    bitmaps round to whole bytes and a string column carries one offsets table
    per piece, so the sum of the parts differs from the whole by a few bytes
    per column per row group in either direction. Both builders that walk row
    groups (the bytes path and the accumulator) have always counted this way;
    only the whole-morsel builder sees the single-piece number."""
    slack = 8 * row_groups
    columns = len(whole["column_uncompressed_sizes_in_bytes"])
    for mine, theirs in zip(
        parts["column_uncompressed_sizes_in_bytes"], whole["column_uncompressed_sizes_in_bytes"]
    ):
        assert abs(mine - theirs) <= slack, (mine, theirs)
    assert (
        abs(parts["uncompressed_size_in_bytes"] - whole["uncompressed_size_in_bytes"])
        <= slack * columns
    )


def _row_group_count(data: bytes) -> int:
    with read_parquet(data) as reader:
        return sum(1 for _ in reader)


def _current_entries(ds):
    from opteryx_catalog.catalog.manifest import read_manifest_rows

    snap = ds.snapshot(None)
    with ds.io.new_input(snap.manifest_list).open() as f:
        return read_manifest_rows(f.read())


# ---------------------------------------------------------------------------
# 1. Exact parity with the whole-morsel builder
# ---------------------------------------------------------------------------

_EXACT_FIELDS = (
    "record_count",
    "null_counts",
    "min_k_hashes",
    "min_values",
    "max_values",
    "min_lengths",
    "max_lengths",
    "field_ids",
    "char_class_counts",
    "char_total_bytes",
    "element_min_values",
    "element_max_values",
    "element_min_k_hashes",
)


@pytest.mark.parametrize("exact", [True, False])
def test_row_group_accumulation_matches_the_whole_morsel_builder(exact):
    groups = _row_groups()
    whole = build_parquet_manifest_entry_from_morsel(
        _concat(groups), b"", "mem://f.parquet", 0, {"i": 1, "s": 2, "b": 3}
    ).to_dict()

    acc = ParquetManifestEntryAccumulator(
        field_id_by_name={"i": 1, "s": 2, "b": 3}, exact_histograms=exact
    )
    for g in groups:
        acc.add(g)
    streamed = acc.finish("mem://f.parquet", 0).to_dict()

    for field in _EXACT_FIELDS:
        assert streamed[field] == whole[field], field
    assert acc.row_group_count == 3
    _assert_sizes_match_up_to_bitmap_padding(streamed, whole, row_groups=3)

    # BOOL is exact in both modes: its domain is fixed, no range to discover.
    assert streamed["histogram_counts"][2] == whole["histogram_counts"][2]
    if exact:
        assert streamed["histogram_counts"] == whole["histogram_counts"]
        assert streamed["histogram_bins"] == whole["histogram_bins"]


def test_bytes_path_is_unchanged_by_the_refactor():
    """The bytes builder now runs through the accumulator in exact mode; a
    multi-row-group file must describe identically to its rows in one piece."""
    from rugo.parquet import write_parquet_stream

    groups = _row_groups()
    buffer = bytearray()
    assert write_parquet_stream(groups, buffer.extend, compression="zstd") == 3
    data = bytes(buffer)
    assert _row_group_count(data) == 3

    from_bytes = build_parquet_manifest_entry_from_bytes(data, "mem://f.parquet", len(data)).to_dict()
    from_morsel = build_parquet_manifest_entry_from_morsel(
        _concat(groups), data, "mem://f.parquet", len(data)
    ).to_dict()
    for field in _EXACT_FIELDS + ("histogram_counts", "histogram_bins"):
        assert from_bytes[field] == from_morsel[field], field
    _assert_sizes_match_up_to_bitmap_padding(from_bytes, from_morsel, row_groups=3)


# ---------------------------------------------------------------------------
# 2. Bounded histograms
# ---------------------------------------------------------------------------


def test_bounded_histogram_conserves_total_and_bounds_and_is_close():
    import random

    rng = random.Random(7)
    groups = []
    for _ in range(6):
        lo = rng.randint(-5000, 5000)
        values = [rng.randint(lo, lo + rng.randint(10, 4000)) for _ in range(2000)]
        values[rng.randrange(2000)] = None
        groups.append(_morsel(values))

    exact_acc = ParquetManifestEntryAccumulator(exact_histograms=True)
    bounded_acc = ParquetManifestEntryAccumulator(exact_histograms=False)
    for g in groups:
        exact_acc.add(g)
        bounded_acc.add(g)
    exact = exact_acc.finish("mem://f.parquet", 0).to_dict()
    bounded = bounded_acc.finish("mem://f.parquet", 0).to_dict()

    assert bounded["min_values"] == exact["min_values"]
    assert bounded["max_values"] == exact["max_values"]
    e_hist = exact["histogram_counts"][0]
    b_hist = bounded["histogram_counts"][0]
    assert len(b_hist) == HISTOGRAM_BINS
    assert sum(b_hist) == sum(e_hist)
    assert bounded["histogram_bins"] == HISTOGRAM_BINS

    # Placement error is bounded by one fine bin of a group's range. With
    # FINE_HISTOGRAM_BINS = 8x the target width that is well under a target
    # bin, so the mass that can move between neighbouring target bins is a
    # small fraction of the total.
    total = sum(e_hist)
    moved = sum(abs(a - b) for a, b in zip(e_hist, b_hist)) / 2
    assert moved <= total * (HISTOGRAM_BINS / FINE_HISTOGRAM_BINS) / 2, (moved, total)


def test_single_valued_and_all_null_groups_are_described():
    acc = ParquetManifestEntryAccumulator(exact_histograms=False)
    acc.add(_morsel([7, 7, 7]))
    acc.add(_morsel([None, None]))
    acc.add(_morsel([7, 9]))
    entry = acc.finish("mem://f.parquet", 0).to_dict()
    assert entry["record_count"] == 7
    assert entry["null_counts"] == [2]
    assert entry["min_values"][0] < entry["max_values"][0]
    assert sum(entry["histogram_counts"][0]) == 5


def test_a_row_group_missing_a_column_is_refused():
    acc = ParquetManifestEntryAccumulator()
    acc.add(_morsel([1], ["a"]))
    with pytest.raises(ValueError, match="missing column"):
        acc.add(_morsel([2]))


def test_finish_with_nothing_declared_is_refused():
    with pytest.raises(ValueError, match="no row groups"):
        ParquetManifestEntryAccumulator().finish("mem://f.parquet", 0)


# ---------------------------------------------------------------------------
# 3. The writer and the entry-based commit
# ---------------------------------------------------------------------------


def test_streamed_file_is_one_parquet_file_with_one_row_group_per_write():
    ds, storage, _mem_io = _seed_dataset({f"{LOCATION}/data/f1.parquet": [1, 2, 3]})
    groups = _row_groups()

    writer = ds.open_data_file_writer(sorted_by="i")
    for g in groups:
        writer.write_row_group(g)
        assert writer.uncompressed_size_in_bytes > 0
    entry = writer.close()

    assert entry.file_path.startswith(f"{LOCATION}/data/")
    assert entry.file_path in storage
    assert entry.file_size_in_bytes == len(storage[entry.file_path])
    assert entry.record_count == 15
    assert writer.row_group_count == 3

    assert read_metadata(storage[entry.file_path]).num_rows == 15
    assert _row_group_count(storage[entry.file_path]) == 3
    with read_parquet(storage[entry.file_path]) as reader:
        values = [v for m in reader for v in m.column(b"i").to_pylist()]
    assert values == [v for g in groups for v in g.column(b"i").to_pylist()]

    # The entry describes the rows that were written, keyed by the schema.
    from_bytes = build_parquet_manifest_entry_from_bytes(
        storage[entry.file_path], entry.file_path, entry.file_size_in_bytes
    ).to_dict()
    assert entry.to_dict()["null_counts"] == from_bytes["null_counts"]
    assert entry.to_dict()["min_values"] == from_bytes["min_values"]
    assert entry.to_dict()["max_values"] == from_bytes["max_values"]

    with pytest.raises(ValueError, match="finished writer"):
        writer.write_row_group(groups[0])


def test_abort_leaves_no_object_and_close_without_rows_is_refused():
    ds, storage, mem_io = _seed_dataset({f"{LOCATION}/data/f1.parquet": [1, 2, 3]})
    before = set(storage)

    writer = ds.open_data_file_writer()
    writer.write_row_group(_morsel([1, 2]))
    writer.abort()
    assert set(storage) == before
    assert mem_io.aborted == [writer.data_path]

    empty = ds.open_data_file_writer()
    with pytest.raises(ValueError, match="no row groups"):
        empty.close()


def test_compaction_commit_with_entries_never_reads_the_outputs_back():
    src = f"{LOCATION}/data/f1.parquet"
    ds, _storage, mem_io = _seed_dataset({src: list(range(10))})

    writer = ds.open_data_file_writer(sorted_by="i")
    writer.write_row_group(_morsel(list(range(5))))
    writer.write_row_group(_morsel(list(range(5, 10))))
    entry = writer.close()

    mem_io.reads.clear()
    ds.compaction_commit(entries=[entry.to_dict()], retired_files=[src], author="tester")

    # The manifest was read to retire the input; the OUTPUT never was.
    assert entry.file_path not in mem_io.reads

    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == [entry.file_path]
    assert entries[0]["record_count"] == 10
    snap = ds.snapshot(None)
    assert snap.operation_type == "compact"
    assert snap.summary["added-data-files"] == 1
    assert snap.summary["deleted-data-files"] == 1


def test_compaction_commit_entries_still_enforce_the_row_count_invariant():
    from opteryx_catalog.exceptions import CompactionInvariantError

    src = f"{LOCATION}/data/f1.parquet"
    ds, _storage, _mem_io = _seed_dataset({src: list(range(10))})

    writer = ds.open_data_file_writer()
    writer.write_row_group(_morsel(list(range(3))))
    entry = writer.close()

    with pytest.raises(CompactionInvariantError, match="10 live rows in, 3 rows out"):
        ds.compaction_commit(entries=[entry.to_dict()], retired_files=[src], author="tester")

    with pytest.raises(ValueError, match="entries OR files"):
        ds.compaction_commit(
            files=[entry.file_path], entries=[entry.to_dict()], retired_files=[src], author="tester"
        )
    with pytest.raises(ValueError, match="no file_path"):
        ds.compaction_commit(entries=[{"record_count": 1}], retired_files=[src], author="tester")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])


# ---------------------------------------------------------------------------
# 4. The other commits take entries too: append, replace, merge
# ---------------------------------------------------------------------------


def _streamed(ds, values):
    writer = ds.open_data_file_writer()
    writer.write_row_group(_morsel(values))
    return writer.close().to_dict()


def test_add_files_with_entries_appends_without_reading_the_output():
    src = f"{LOCATION}/data/f1.parquet"
    ds, _storage, mem_io = _seed_dataset({src: [1, 2, 3]})
    entry = _streamed(ds, [4, 5])

    mem_io.reads.clear()
    ds.add_files(entries=[entry], author="tester", read_sources=[])

    assert entry["file_path"] not in mem_io.reads
    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == [src, entry["file_path"]]
    assert ds.snapshot(None).summary["added-records"] == 2
    with pytest.raises(ValueError, match="entries OR files"):
        ds.add_files(files=[src], entries=[entry], author="tester", read_sources=[])


def test_truncate_and_add_files_with_entries_replaces_without_reading_the_output():
    src = f"{LOCATION}/data/f1.parquet"
    ds, _storage, mem_io = _seed_dataset({src: [1, 2, 3]})
    entry = _streamed(ds, [9, 8, 7, 6])

    mem_io.reads.clear()
    ds.truncate_and_add_files(entries=[entry], author="tester", read_sources=[])

    assert entry["file_path"] not in mem_io.reads
    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == [entry["file_path"]]
    assert ds.snapshot(None).summary["total-records"] == 4
    with pytest.raises(ValueError, match="entries OR files"):
        ds.truncate_and_add_files(files=[src], entries=[entry], author="tester", read_sources=[])


def test_merge_commit_with_entries_adds_and_deletes_in_one_snapshot():
    src = f"{LOCATION}/data/f1.parquet"
    ds, _storage, mem_io = _seed_dataset({src: [1, 2, 3]})
    entry = _streamed(ds, [20])

    mem_io.reads.clear()
    ds.merge_commit(entries=[entry], positions={src: [1]}, author="tester", read_sources=[])

    assert entry["file_path"] not in mem_io.reads
    entries = _current_entries(ds)
    assert [e["file_path"] for e in entries] == [src, entry["file_path"]]
    assert ds.delete_vectors() == {src: [1]}
    with pytest.raises(ValueError, match="entries OR files"):
        ds.merge_commit(files=[src], entries=[entry], positions={}, author="tester", read_sources=[])
    with pytest.raises(ValueError, match="requires files to add"):
        ds.merge_commit(entries=[], positions={}, author="tester", read_sources=[])
