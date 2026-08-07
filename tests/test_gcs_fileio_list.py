def test_gcsfileio_delegates_list_files(monkeypatch):
    from opteryx_catalog.iops.fileio import GcsFileIO

    # construct without calling __init__ to avoid importing external dependency
    g = object.__new__(GcsFileIO)

    class FakeImpl:
        def list_files(self, prefix):
            return [f"{prefix}/a", f"{prefix}/b"]

    g._impl = FakeImpl()
    res = g.list_files("gs://bucket/prefix")
    assert res == ["gs://bucket/prefix/a", "gs://bucket/prefix/b"]


def test_fileio_base_has_list_files_and_ls_alias():
    from opteryx_catalog.iops.fileio import FileIO

    f = FileIO()
    # base class provides a safe default (no warnings will be emitted)
    assert callable(getattr(f, "list_files", None))
    assert f.list_files("anything") == []
    # Compare on the class: attribute access on an *instance* builds a fresh
    # bound method each time, so `is` between two instance lookups is never true
    # even when both wrap the same function. The sibling GcsFileIO alias tests
    # already compare this way.
    assert FileIO.ls is FileIO.list_files


def test_gcsfileio_ls_alias_exists():
    from opteryx_catalog.iops.fileio import GcsFileIO

    g = object.__new__(GcsFileIO)
    # make sure attribute exists
    g._impl = type("X", (), {"list_files": lambda self, p: []})()
    assert callable(getattr(g, "ls", None))
    # class-level alias should point to same function object
    from opteryx_catalog.iops import fileio as _fio_mod

    assert _fio_mod.GcsFileIO.ls is _fio_mod.GcsFileIO.list_files
    # runtime call parity
    assert g.ls("x") == g.list_files("x")


def test_http_gcsfileio_list_files_uses_storage_client(monkeypatch):
    from opteryx_catalog.iops.gcs import GcsFileIO

    # Construct without running __init__ to avoid network/credentials setup
    g = object.__new__(GcsFileIO)

    class FakeBlob:
        def __init__(self, name):
            self.name = name

    class FakeBucket:
        def __init__(self, names):
            self._names = names

        def list_blobs(self, prefix=None):
            return [FakeBlob(n) for n in self._names if (prefix is None or n.startswith(prefix))]

    class FakeClient:
        def __init__(self, names):
            self._names = names

        def list_blobs(self, bucket, prefix=None):
            # ignore bucket param for test isolation
            return FakeBucket(self._names).list_blobs(prefix=prefix)

    # Patch google.cloud.storage.Client at its source: gcs.list_files imports
    # `storage` lazily inside the call, so there is no module-level
    # `opteryx_catalog.iops.gcs.storage` attribute to patch instead.
    from google.cloud import storage as gcs_storage

    monkeypatch.setattr(
        gcs_storage,
        "Client",
        lambda: FakeClient(["prefix/a", "prefix/b", "other/c"]),
    )

    res = g.list_files("gs://bucket/prefix")
    assert res == ["gs://bucket/prefix/a", "gs://bucket/prefix/b"]


def test_http_gcsfileio_ls_alias_exists():
    from opteryx_catalog.iops.gcs import GcsFileIO

    g = object.__new__(GcsFileIO)
    assert callable(getattr(g, "ls", None))
    assert GcsFileIO.ls is GcsFileIO.list_files


def test_deep_clean_uses_io_list_files():
    from types import SimpleNamespace

    from opteryx_catalog.catalog.deep_clean import DatasetDeepClean
    from opteryx_catalog.iops.gcs import GcsFileIO

    # fake catalog with GcsFileIO instance (avoid __init__ side effects).
    # get_all_physical_files normalizes the prefix to end with "/" before
    # listing (a path-boundary guard against e.g. "tweets" prefix-matching
    # "tweets_512"), so the fake mimics a real listing off that prefix.
    fake_io = object.__new__(GcsFileIO)
    fake_io.list_files = lambda prefix: [f"{prefix}a", f"{prefix}b"]

    fake_catalog = SimpleNamespace(io=fake_io)
    cleaner = DatasetDeepClean(fake_catalog)

    files = cleaner.get_all_physical_files("gs://bucket/prefix")
    assert files == {"gs://bucket/prefix/a", "gs://bucket/prefix/b"}
