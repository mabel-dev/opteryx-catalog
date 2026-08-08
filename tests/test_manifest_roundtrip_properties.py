"""Property test for the manifest encode/decode round-trip.

Every statistic the planner uses is written into a Parquet manifest and read
back out, through a nested ``ARRAY<ARRAY<...>>`` encoding and a mix of signed
and unsigned integer leaves. A value that survives the round-trip changed is
worse than one that fails to write: nothing raises, and the planner prunes
against corrupted statistics.

The boundary that matters most is ``min_k_hashes``. Those are full-range xxhash
uint64 values, so anything above ``INT64_MAX`` - about half of them - reads back
negative if the leaf is signed, which silently reorders the KMV sketch. Example
tests don't naturally reach 2**63; generated ones do.
"""

import io
import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from opteryx_catalog import OpteryxCatalog
from opteryx_catalog.catalog.manifest import read_manifest_rows

UINT64_MASK = 0xFFFFFFFFFFFFFFFF
INT64_MAX = (1 << 63) - 1
INT64_MIN = -(1 << 63)

# Per-column statistics arrays, all indexed in lock-step with field_ids.
_FLAT_INT_ARRAY_COLUMNS = (
    "column_uncompressed_sizes_in_bytes",
    "null_counts",
    "min_lengths",
    "max_lengths",
    "field_ids",
    "char_total_bytes",
)
_NESTED_INT_ARRAY_COLUMNS = ("histogram_counts", "char_class_counts")
_NULL_FILLED_SCALARS = ("record_count", "file_size_in_bytes", "uncompressed_size_in_bytes")


class _MemIO:
    """Minimal FileIO that keeps written objects in a dict."""

    def __init__(self, mapping: dict):
        self._mapping = mapping

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

    # Reuse the real implementation so this fixture can't drift from it.
    write_parquet_manifest = OpteryxCatalog.write_parquet_manifest


def _round_trip(entries: list) -> list:
    mapping: dict = {}
    catalog = _FakeCatalog(_MemIO(mapping))
    path = catalog.write_parquet_manifest(1, entries, "mem://data")
    return read_manifest_rows(mapping[path])


def _expected(entry: dict) -> dict:
    """What the writer's documented normalization should turn ``entry`` into.

    Mirrors write_parquet_manifest step for step: absent numeric scalars become
    0 rather than SQL NULL, absent arrays become empty, min/max are coerced to
    int, and every hash is masked to its true unsigned 64-bit value (a no-op for
    correct values, and the repair for legacy negatives).
    """
    e = dict(entry)

    for key in _NULL_FILLED_SCALARS:
        if e.get(key) is None:
            e[key] = 0
    for key in _FLAT_INT_ARRAY_COLUMNS + _NESTED_INT_ARRAY_COLUMNS + ("min_k_hashes",):
        e.setdefault(key, [])
    e.setdefault("histogram_bins", 0)

    e["min_values"] = [int(v) if v is not None else None for v in (e.get("min_values") or [])]
    e["max_values"] = [int(v) if v is not None else None for v in (e.get("max_values") or [])]

    hashes = e["min_k_hashes"]
    e["min_k_hashes"] = (
        None
        if hashes is None
        else [
            None if col is None else [None if h is None else int(h) & UINT64_MASK for h in col]
            for col in hashes
        ]
    )
    return e


# Hashes deliberately span the full unsigned range, including values above
# INT64_MAX, plus negatives standing in for legacy signed-stored manifests.
_hash = st.one_of(
    st.integers(min_value=0, max_value=UINT64_MASK),
    st.sampled_from([0, 1, INT64_MAX, INT64_MAX + 1, 1 << 63, UINT64_MASK, -1, INT64_MIN]),
)


@st.composite
def _manifest_entry(draw):
    columns = draw(st.integers(min_value=0, max_value=3))

    entry = {
        "file_path": draw(st.text(max_size=40)),
        "file_format": draw(st.sampled_from(["PARQUET", "parquet", ""])),
        "record_count": draw(st.one_of(st.integers(min_value=0, max_value=1 << 40), st.none())),
        "file_size_in_bytes": draw(
            st.one_of(st.integers(min_value=0, max_value=1 << 40), st.none())
        ),
        "uncompressed_size_in_bytes": draw(
            st.one_of(st.integers(min_value=0, max_value=1 << 40), st.none())
        ),
        "histogram_bins": draw(st.integers(min_value=0, max_value=64)),
        "min_values": draw(
            st.lists(
                st.one_of(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), st.none()),
                min_size=columns,
                max_size=columns,
            )
        ),
        "max_values": draw(
            st.lists(
                st.one_of(st.integers(min_value=INT64_MIN, max_value=INT64_MAX), st.none()),
                min_size=columns,
                max_size=columns,
            )
        ),
        "min_k_hashes": [draw(st.lists(_hash, max_size=6)) for _ in range(columns)],
        "histogram_counts": [
            draw(st.lists(st.integers(min_value=0, max_value=1 << 32), max_size=6))
            for _ in range(columns)
        ],
        "char_class_counts": [
            draw(st.lists(st.integers(min_value=0, max_value=1 << 32), min_size=8, max_size=8))
            for _ in range(columns)
        ],
    }
    for name in _FLAT_INT_ARRAY_COLUMNS:
        entry[name] = draw(
            st.lists(
                st.integers(min_value=0, max_value=1 << 40), min_size=columns, max_size=columns
            )
        )

    # Entries written before a column existed simply lack the key; the writer
    # fills those, and that fill is part of what this test pins.
    for name in draw(st.lists(st.sampled_from(sorted(entry)), max_size=3, unique=True)):
        if name not in ("file_path", "file_format"):
            del entry[name]
    return entry


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(entries=st.lists(_manifest_entry(), min_size=1, max_size=4))
def test_manifest_entries_survive_the_round_trip(entries):
    rows = _round_trip(entries)

    assert len(rows) == len(entries), "row count must be preserved"
    for row, entry in zip(rows, entries):
        expected = _expected(entry)
        for key, want in expected.items():
            assert row[key] == want, f"{key}: wrote {want!r}, read back {row[key]!r}"


@settings(max_examples=25, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(hashes=st.lists(_hash, min_size=1, max_size=32))
def test_min_k_hashes_keep_their_unsigned_value_and_order(hashes):
    """The KMV sketch is meaningless if hash ordering changes across a write.

    A signed leaf reads values above INT64_MAX back as negative, which does not
    raise - it just reorders "smallest", quietly changing every distinct-count
    estimate derived from the sketch.
    """
    entry = {"file_path": "mem://data/f.parquet", "min_k_hashes": [hashes]}

    (row,) = _round_trip([entry])
    read_back = row["min_k_hashes"][0]
    unsigned = [h & UINT64_MASK for h in hashes]

    assert read_back == unsigned
    assert all(0 <= h <= UINT64_MASK for h in read_back)
    # Ordering by value must be stable across the encoding, in both directions.
    assert sorted(read_back) == sorted(unsigned)


def test_empty_manifest_round_trips_to_no_rows():
    assert _round_trip([]) == []
