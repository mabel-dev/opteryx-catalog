"""
Merge-on-read delete vectors.

A delete commit does not rewrite data files. It records which row ordinals of
which data files are gone, in a sidecar Parquet file written next to the
manifests:

    <dataset_location>/metadata/deletes-<snapshot_id>.parquet

One row per data file that has any deleted rows, holding that file's COMPLETE
delete state as of the snapshot — merged, not incremental. Because manifests in
this catalog are cumulative and fully rewritten on every commit, a delete
commit merges new positions into the file's previous bitmap and writes one
fresh vector; there is never a stack of delete files to reconcile at read
time, and no applicability/sequence-number rules. Readers subtract the bitmap
from the file, and that is the whole contract.

The manifest references the sidecar per data file via two columns
(``delete_file_path``, ``deleted_record_count``), both reading as "no deletes"
when absent so pre-MOR manifests and readers are unaffected. See
MOR_DELETES_DESIGN.md.

Sidecar schema (all types already exercised by the manifest writer — the
bitmap is base64 in a VARCHAR because rugo's generic reader materialises
BYTE_ARRAY columns as UTF-8 strings, so raw binary does not round-trip):

    data_file_path       VARCHAR   the data file these positions belong to
    deleted_record_count INTEGER   cardinality of the bitmap (denormalised)
    bitmap               VARCHAR   base64 of the encoded ordinals (below)

Bitmap encoding — one header byte, then payload:

    0x00  sorted ascending ordinals as varint-encoded deltas (delta from the
          previous ordinal, first delta measured from -1, so every delta >= 1).
          Wins for sparse deletes, the common case.
    0x01  dense bitset over [0, record_count), LSB-first within each byte.
          Wins when much of the file is gone.

The writer picks whichever is smaller. Row ordinals are file-local, zero-based,
in the file's physical row order — the order a Parquet scan yields rows.
"""

from __future__ import annotations

import base64
import re
from collections.abc import Iterable

# Manifest columns carrying the per-data-file delete reference. Absent (None/0)
# means "no deletes" — the compatibility contract every consumer relies on.
DELETE_FILE_PATH_KEY = "delete_file_path"
DELETED_RECORD_COUNT_KEY = "deleted_record_count"

_ENC_VARINT_DELTAS = 0x00
_ENC_DENSE_BITSET = 0x01

# deletes-<snapshot_id>.parquet — the snapshot id doubles as a write timestamp
# (same convention as manifest-<snapshot_id>.parquet), which age-gated sweeps
# may parse from the name.
DELETE_VECTOR_FILENAME_RE = re.compile(r"deletes-(\d+)\.parquet$")


def delete_vector_path(dataset_location: str, snapshot_id: int) -> str:
    """The sidecar path for a snapshot, beside the manifests under metadata/."""
    return f"{dataset_location}/metadata/deletes-{snapshot_id}.parquet"


def is_delete_vector_path(path: str) -> bool:
    """True for paths shaped like a delete-vector sidecar."""
    return bool(DELETE_VECTOR_FILENAME_RE.search(path or ""))


# ---------------------------------------------------------------------------
# Bitmap encoding
# ---------------------------------------------------------------------------


def encode_positions(positions: Iterable[int], record_count: int) -> bytes:
    """Encode a set of row ordinals into the smaller of the two encodings.

    ``record_count`` is the file's physical row count; every ordinal must lie
    in ``[0, record_count)``. Duplicates are tolerated (a set is taken).
    """
    ordered = sorted(set(int(p) for p in positions))
    if ordered and (ordered[0] < 0 or ordered[-1] >= record_count):
        bad = ordered[0] if ordered[0] < 0 else ordered[-1]
        raise ValueError(
            f"delete position {bad} out of range for a file of {record_count} rows"
        )

    # Varint deltas
    out = bytearray([_ENC_VARINT_DELTAS])
    prev = -1
    for p in ordered:
        delta = p - prev
        prev = p
        while True:
            byte = delta & 0x7F
            delta >>= 7
            if delta:
                out.append(byte | 0x80)
            else:
                out.append(byte)
                break
    varint_form = bytes(out)

    # Dense bitset
    bits = bytearray((record_count + 7) // 8)
    for p in ordered:
        bits[p >> 3] |= 1 << (p & 7)
    dense_form = bytes([_ENC_DENSE_BITSET]) + bytes(bits)

    return varint_form if len(varint_form) <= len(dense_form) else dense_form


def decode_positions(blob: bytes) -> list[int]:
    """Decode an encoded bitmap back to a sorted list of row ordinals."""
    if not blob:
        return []
    header = blob[0]
    payload = memoryview(blob)[1:]

    if header == _ENC_VARINT_DELTAS:
        positions: list[int] = []
        value = 0
        shift = 0
        prev = -1
        for byte in payload:
            value |= (byte & 0x7F) << shift
            if byte & 0x80:
                shift += 7
            else:
                prev = prev + value
                positions.append(prev)
                value = 0
                shift = 0
        if shift != 0:
            raise ValueError("truncated varint in delete bitmap")
        return positions

    if header == _ENC_DENSE_BITSET:
        positions = []
        for byte_index, byte in enumerate(payload):
            if not byte:
                continue
            base = byte_index << 3
            for bit in range(8):
                if byte & (1 << bit):
                    positions.append(base + bit)
        return positions

    raise ValueError(f"unknown delete bitmap encoding 0x{header:02x}")


# ---------------------------------------------------------------------------
# Sidecar file IO
# ---------------------------------------------------------------------------


def write_delete_vector_file(io, path: str, vectors: dict[str, list[int]]) -> None:
    """Write a sidecar holding the complete delete state for a snapshot.

    ``vectors`` maps each data file's path to the row ordinals deleted from it
    (each list non-empty — a file with no deletes simply has no row). Positions
    are encoded per file; the record_count bound is not re-validated here, the
    commit path validated ordinals against the manifest before calling.
    """
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    from ..iops.fileio import WRITE_PARQUET_OPTIONS

    paths = sorted(vectors)
    encoded = []
    counts = []
    for file_path in paths:
        ordered = sorted(set(int(p) for p in vectors[file_path]))
        if not ordered:
            raise ValueError(f"empty delete vector for {file_path}; omit the file instead")
        # Encode against an open bound: the true record_count lives on the
        # manifest entry and was enforced by the commit path.
        blob = encode_positions(ordered, ordered[-1] + 1)
        encoded.append(base64.b64encode(blob).decode("ascii"))
        counts.append(len(ordered))

    morsel = Morsel()
    morsel.append_vector("data_file_path", vector_from_sequence(paths, dtype="VARCHAR"))
    morsel.append_vector("deleted_record_count", vector_from_sequence(counts, dtype="INTEGER"))
    morsel.append_vector("bitmap", vector_from_sequence(encoded, dtype="VARCHAR"))

    data = write_parquet(morsel, **WRITE_PARQUET_OPTIONS)
    out = io.new_output(path).create()
    out.write(data)
    # close() is where the upload happens (the GCS output buffers and flushes
    # on close) — same contract as the manifest writer; let failures raise.
    out.close()

    # The sidecar is read through the parsed-manifest LRU (see
    # read_delete_vector_file); a rewrite at the same path must not be served
    # from a stale cache entry.
    from .manifest import invalidate_parsed_manifest

    invalidate_parsed_manifest(path)


def read_delete_vector_file(io, path: str) -> dict[str, list[int]]:
    """Read a sidecar back to ``{data_file_path: sorted ordinals}``.

    Reads through ``get_parsed_manifest`` — the sidecar is an ordinary small
    Parquet file, so it shares the manifest LRU cache and its fail-loud
    missing-file semantics (a missing sidecar raises rather than reading as
    "no deletes": the manifest said these files HAVE deletes, and serving the
    undeleted rows would be silent data resurrection).
    """
    from .manifest import get_parsed_manifest

    rows = get_parsed_manifest(io, path)
    vectors: dict[str, list[int]] = {}
    for row in rows:
        file_path = row.get("data_file_path")
        bitmap_b64 = row.get("bitmap")
        if not file_path or not bitmap_b64:
            continue
        positions = decode_positions(base64.b64decode(bitmap_b64))
        declared = row.get("deleted_record_count")
        if declared is not None and int(declared) != len(positions):
            raise ValueError(
                f"delete vector for {file_path} in {path} declares {declared} "
                f"positions but decodes to {len(positions)}"
            )
        vectors[file_path] = positions
    return vectors


def read_delete_vectors_for_entries(io, entries: Iterable) -> dict[str, list[int]]:
    """Resolve the delete state for a manifest's entries.

    Groups entries by their ``delete_file_path`` (normally a single sidecar —
    the current snapshot's — but append commits carry parent rows forward
    verbatim, so rows may reference older sidecars), reads each sidecar once,
    and returns ``{data_file_path: sorted ordinals}`` for exactly the files the
    manifest says have deletes.

    Raises if a referenced sidecar is missing or holds no vector for a file
    the manifest attributes deletes to — both mean rows would silently
    resurrect if the read continued.
    """
    by_sidecar: dict[str, list] = {}
    for entry in entries:
        sidecar = entry.get(DELETE_FILE_PATH_KEY)
        if sidecar and int(entry.get(DELETED_RECORD_COUNT_KEY) or 0) > 0:
            by_sidecar.setdefault(sidecar, []).append(entry)

    resolved: dict[str, list[int]] = {}
    for sidecar, sidecar_entries in by_sidecar.items():
        vectors = read_delete_vector_file(io, sidecar)
        for entry in sidecar_entries:
            file_path = entry.get("file_path")
            positions = vectors.get(file_path)
            if positions is None:
                raise ValueError(
                    f"manifest attributes {entry.get(DELETED_RECORD_COUNT_KEY)} deleted rows "
                    f"of {file_path} to {sidecar}, which holds no vector for it"
                )
            resolved[file_path] = positions
    return resolved


# ---------------------------------------------------------------------------
# Materialisation (compaction support)
# ---------------------------------------------------------------------------


def drop_deleted_rows_from_morsels(morsels: list, positions: list[int]) -> list:
    """Subtract file-global deleted ordinals from a file's decoded row groups.

    ``morsels`` are the file's row groups IN PHYSICAL ORDER (the order a
    parquet reader yields them — ordinal space is defined by that order);
    ``positions`` is the file's sorted delete vector. Returns the surviving
    morsels: untouched objects where a group has no deletes, row-filtered
    copies where it has some, nothing where every row is deleted.
    """
    from bisect import bisect_left

    if not positions:
        return list(morsels)
    out = []
    offset = 0
    for morsel in morsels:
        nrows = morsel.num_rows
        lo = bisect_left(positions, offset)
        hi = bisect_left(positions, offset + nrows)
        if lo == hi:
            out.append(morsel)
        elif hi - lo < nrows:
            deleted_local = {positions[i] - offset for i in range(lo, hi)}
            keep = [i for i in range(nrows) if i not in deleted_local]
            out.append(morsel.take(keep))
        # hi - lo == nrows: whole group deleted, emit nothing
        offset += nrows
    return out


def materialise_live_parquet(data: bytes, positions: list[int]) -> bytes:
    """Re-encode a parquet file's bytes with its deleted rows dropped.

    The chokepoint compaction uses: rewriting the SOURCE bytes once means
    every downstream consumer — predicate reads, sort-column projection,
    row-group streaming, row balance — sees a physically-live file and needs
    no ordinal awareness of its own. Costs one decode+encode per
    delete-bearing input, paid by a pass that was rewriting the data anyway.
    """
    from rugo.parquet import read_parquet
    from rugo.parquet import write_parquet

    from ..iops.fileio import WRITE_PARQUET_OPTIONS

    if not positions:
        return data
    with read_parquet(bytes(data)) as reader:
        morsels = list(reader)
    live = drop_deleted_rows_from_morsels(morsels, positions)
    if not live:
        raise ValueError(
            "every row of the file is deleted; an all-deleted file must have been "
            "dropped from the manifest at delete-commit time, not materialised"
        )
    combined = live[0] if len(live) == 1 else live[0].combine(live)
    return write_parquet(combined, **WRITE_PARQUET_OPTIONS)
