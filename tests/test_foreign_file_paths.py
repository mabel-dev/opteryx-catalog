"""A commit may only register files under the dataset's own location.

Scans read every file a manifest names with the engine's storage credentials,
so a manifest entry pointing at `gs://other-bucket/...` would let a dataset
read anything those credentials reach (confused deputy). Every commit path that
admits new files - read-back (`files=`) or prebuilt (`entries=`/`manifest=`) -
refuses a foreign path before reading it and before committing anything.

The one exception is a fork's clone/resync, whose borrowed entries are handed
over as `borrowed_paths` by the catalog itself.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from test_provenance_commits import LOCATION
from test_provenance_commits import _dataset
from test_provenance_commits import _stage

from opteryx_catalog.catalog.manifest import build_parquet_manifest_entry_from_bytes
from opteryx_catalog.catalog.ownership import is_admissible_path
from opteryx_catalog.exceptions import ForeignFilePathError
from opteryx_catalog.opteryx_catalog import _borrowed_paths

OWN = f"{LOCATION}/data/own.parquet"
FOREIGN = "mem://other-bucket/secrets/payroll.parquet"


@pytest.mark.parametrize(
    "path",
    [
        f"{LOCATION}/data/f.parquet",
        f"{LOCATION}/f.parquet",  # worker.opteryx saves results directly under the location
        f"{LOCATION}/data/contract-1/part-0.parquet",  # upload.opteryx's contract prefix
        "f.parquet",  # bare fixture name
    ],
)
def test_admissible(path):
    assert is_admissible_path(LOCATION, path)


@pytest.mark.parametrize(
    "path",
    [
        FOREIGN,
        "gs://elsewhere/ws/y/data/f.parquet",
        f"{LOCATION}_v2/data/f.parquet",  # sibling dataset sharing a name prefix
        f"{LOCATION}/data/../../z/data/f.parquet",  # traversal out of the location
        f"{LOCATION}/./data/f.parquet",
        "other-bucket/secret.parquet",  # the GCS IO reads the first segment as a bucket
        "/etc/passwd.parquet",
        "../f.parquet",
        "",
        None,
    ],
)
def test_inadmissible(path):
    assert not is_admissible_path(LOCATION, path)


LOCAL = "/srv/catalog/ws/coll/ds"


@pytest.mark.parametrize(
    "path, admitted",
    [
        (f"{LOCAL}/data/f.parquet", True),             # a local catalog's own file
        (f"{LOCAL}/f.parquet", True),
        (f"{LOCAL}_v2/data/f.parquet", False),         # sibling sharing a name prefix
        ("/srv/catalog/ws/coll/other/f.parquet", False),
        ("/etc/passwd.parquet", False),
        (f"{LOCAL}/data/../../x/f.parquet", False),    # traversal
        ("gs://elsewhere/ws/coll/ds/data/f.parquet", False),
    ],
)
def test_a_local_location_admits_only_its_own_absolute_paths(path, admitted):
    assert is_admissible_path(LOCAL, path) is admitted


def test_a_uri_location_admits_no_local_absolute_path():
    assert not is_admissible_path(LOCATION, "/srv/catalog/ws/coll/ds/data/f.parquet")


def test_unknown_location_admits_no_absolute_path():
    assert not is_admissible_path(None, f"{LOCATION}/data/f.parquet")
    assert not is_admissible_path("", f"{LOCATION}/data/f.parquet")


class _SpyIO:
    """Records every path opened, so a test can say a foreign file was never read."""

    def __init__(self, inner):
        self._inner = inner
        self.opened: list[str] = []

    def new_input(self, path):
        self.opened.append(path)
        return self._inner.new_input(path)

    def new_output(self, path):
        return self._inner.new_output(path)


def _world():
    ds, storage = _dataset()
    ds.io = _SpyIO(ds.io)
    _stage(storage, OWN, [1, 2, 3])
    _stage(storage, FOREIGN, [99])
    return ds, storage


def _entry(storage, path):
    return build_parquet_manifest_entry_from_bytes(storage[path], path, len(storage[path])).to_dict()


# ---------------------------------------------------------------------------
# read-back (`files=`)
# ---------------------------------------------------------------------------


def test_add_files_refuses_a_foreign_path_without_reading_it():
    ds, _ = _world()
    with pytest.raises(ForeignFilePathError, match="payroll"):
        ds.add_files([FOREIGN], author="tester")
    assert FOREIGN not in ds.io.opened
    assert ds.metadata.current_snapshot_id is None


def test_one_foreign_path_refuses_the_whole_commit():
    ds, _ = _world()
    with pytest.raises(ForeignFilePathError):
        ds.add_files([OWN, FOREIGN], author="tester")
    assert ds.metadata.current_snapshot_id is None


def test_add_files_accepts_its_own_path():
    ds, _ = _world()
    ds.add_files([OWN], author="tester")
    assert ds.snapshot(None).summary["total-records"] == 3


def test_truncate_and_add_files_refuses_a_foreign_path():
    ds, _ = _world()
    with pytest.raises(ForeignFilePathError):
        ds.truncate_and_add_files([FOREIGN], author="tester")
    assert FOREIGN not in ds.io.opened


def test_merge_commit_refuses_a_foreign_path():
    ds, _ = _world()
    with pytest.raises(ForeignFilePathError):
        ds.merge_commit([FOREIGN], {}, author="tester")
    assert FOREIGN not in ds.io.opened


def test_compaction_commit_refuses_a_foreign_path():
    ds, _ = _world()
    ds.add_files([OWN], author="tester")
    before = ds.metadata.current_snapshot_id
    with pytest.raises(ForeignFilePathError):
        ds.compaction_commit(files=[FOREIGN], retired_files=[OWN], author="tester")
    assert ds.metadata.current_snapshot_id == before


# ---------------------------------------------------------------------------
# prebuilt (`entries=`)
# ---------------------------------------------------------------------------


def test_add_files_refuses_a_foreign_prebuilt_entry():
    ds, storage = _world()
    with pytest.raises(ForeignFilePathError):
        ds.add_files(entries=[_entry(storage, FOREIGN)], author="tester")
    assert ds.metadata.current_snapshot_id is None


def test_a_foreign_delete_vector_is_refused_too():
    ds, storage = _world()
    entry = _entry(storage, OWN)
    entry["delete_file_path"] = "mem://other-bucket/deletes/dv.bin"
    with pytest.raises(ForeignFilePathError, match="delete file"):
        ds.add_files(entries=[entry], author="tester")


def test_truncate_and_add_files_refuses_foreign_entries_unless_borrowed():
    ds, storage = _world()
    entry = _entry(storage, FOREIGN)
    with pytest.raises(ForeignFilePathError):
        ds.truncate_and_add_files(entries=[entry], author="tester")
    assert ds.metadata.current_snapshot_id is None

    # The fork exemption: what clone/resync hands over, and only that.
    ds.truncate_and_add_files(
        entries=[entry], borrowed_paths=_borrowed_paths([entry]), author="tester"
    )
    assert [e["file_path"] for e in ds._parent_manifest_entries(ds.snapshot(None))] == [FOREIGN]


def test_borrowing_one_path_does_not_admit_another():
    ds, storage = _world()
    elsewhere = "mem://other-bucket/secrets/other.parquet"
    _stage(storage, elsewhere, [7])
    borrowed = _entry(storage, FOREIGN)
    with pytest.raises(ForeignFilePathError, match="other.parquet"):
        ds.truncate_and_add_files(
            entries=[borrowed, _entry(storage, elsewhere)],
            borrowed_paths=_borrowed_paths([borrowed]),
            author="tester",
        )


def test_borrowed_paths_cover_data_files_and_delete_vectors():
    entries = [
        {"file_path": "a.parquet", "delete_file_path": "a.dv"},
        {"file_path": "b.parquet", "delete_file_path": None},
    ]
    assert _borrowed_paths(entries) == frozenset({"a.parquet", "a.dv", "b.parquet"})
