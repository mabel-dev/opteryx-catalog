from __future__ import annotations

import time

from opteryx_catalog.catalog.dropped_sweep import DroppedDatasetSweep

DAY_MS = 24 * 60 * 60 * 1000


class _FakeIO:
    """FileIO that records deletions; `fail_on` paths report failure."""

    def __init__(self, fail_on=None):
        self.deleted = []
        self.fail_on = set(fail_on or [])

    def delete(self, path):
        if path in self.fail_on:
            raise OSError(f"permission denied: {path}")
        self.deleted.append(path)


class _FakeCatalog:
    def __init__(self, tombstones, files, io=None):
        self._tombstones = tombstones
        self._files = files
        self.io = io or _FakeIO()
        self.deleted_tombstones = []

    def list_dropped_datasets(self):
        return list(self._tombstones)

    def delete_tombstone(self, tombstone_id):
        self.deleted_tombstones.append(tombstone_id)


def _tombstone(name="tbl", age_ms=2 * DAY_MS, location="gs://bucket/ws/coll/tbl"):
    return {
        "id": f"coll.{name}",
        "name": name,
        "collection": "coll",
        "workspace": "ws",
        "location": location,
        "dropped-at-ms": int(time.time() * 1000) - age_ms,
        "dropped-by": "alice",
    }


def _sweep_for(catalog):
    sweep = DroppedDatasetSweep(catalog)
    # Stub the storage listing; the real one talks to GCS.
    sweep._deep_clean.get_all_physical_files = lambda location: set(
        catalog._files.get(location, [])
    )
    return sweep


def test_reclaims_files_and_clears_tombstone():
    """An aged tombstone has its whole prefix deleted, then is cleared."""
    files = {
        "gs://bucket/ws/coll/tbl": [
            "gs://bucket/ws/coll/tbl/a.parquet",
            "gs://bucket/ws/coll/tbl/metadata/m.parquet",
        ]
    }
    catalog = _FakeCatalog([_tombstone()], files)

    result = _sweep_for(catalog).sweep(dry_run=False)

    assert result["reclaimed"] == 1
    assert result["files_deleted"] == 2
    assert sorted(catalog.io.deleted) == sorted(files["gs://bucket/ws/coll/tbl"])
    assert catalog.deleted_tombstones == ["coll.tbl"]
    assert result["details"][0]["tombstone_cleared"] is True


def test_respects_grace_period():
    """A freshly dropped dataset is left alone until the grace period elapses."""
    catalog = _FakeCatalog([_tombstone(age_ms=60 * 1000)], {})

    result = _sweep_for(catalog).sweep(dry_run=False)

    assert result["skipped"] == 1
    assert result["files_deleted"] == 0
    assert catalog.io.deleted == []
    assert catalog.deleted_tombstones == []
    assert result["details"][0]["reason"] == "within-grace"


def test_dry_run_deletes_nothing():
    """Dry run reports what it would reclaim and touches neither files nor tombstone."""
    files = {"gs://bucket/ws/coll/tbl": ["gs://bucket/ws/coll/tbl/a.parquet"]}
    catalog = _FakeCatalog([_tombstone()], files)

    result = _sweep_for(catalog).sweep(dry_run=True)

    assert result["files_deleted"] == 1
    assert catalog.io.deleted == []
    assert catalog.deleted_tombstones == []
    assert result["details"][0]["reason"] == "dry-run"


def test_partial_failure_keeps_tombstone():
    """A failed delete must not clear the tombstone, or the rest is stranded."""
    paths = ["gs://bucket/ws/coll/tbl/a.parquet", "gs://bucket/ws/coll/tbl/b.parquet"]
    files = {"gs://bucket/ws/coll/tbl": paths}
    catalog = _FakeCatalog([_tombstone()], files, io=_FakeIO(fail_on=[paths[1]]))

    result = _sweep_for(catalog).sweep(dry_run=False)

    assert result["errors"] == 1
    assert catalog.deleted_tombstones == []
    detail = result["details"][0]
    assert detail["reason"] == "partial-delete"
    assert detail["files_deleted"] == 1
    assert detail["files_failed"] == 1


def test_missing_location_is_reported_not_silently_dropped():
    catalog = _FakeCatalog([_tombstone(location=None)], {})

    result = _sweep_for(catalog).sweep(dry_run=False)

    assert result["errors"] == 1
    assert result["details"][0]["reason"] == "no-location"
    assert catalog.deleted_tombstones == []


def test_missing_drop_time_never_becomes_eligible():
    """Without a drop time the grace period cannot be proven to have elapsed."""
    tombstone = _tombstone()
    del tombstone["dropped-at-ms"]
    catalog = _FakeCatalog([tombstone], {})

    result = _sweep_for(catalog).sweep(dry_run=False)

    assert result["errors"] == 1
    assert result["details"][0]["reason"] == "no-dropped-at-ms"
    assert catalog.io.deleted == []


def test_empty_workspace_sweeps_cleanly():
    catalog = _FakeCatalog([], {})
    result = _sweep_for(catalog).sweep(dry_run=False)
    assert result == {
        "tombstones": 0,
        "reclaimed": 0,
        "skipped": 0,
        "errors": 0,
        "files_deleted": 0,
        "dry_run": False,
        "details": [],
    }
