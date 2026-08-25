"""Snapshot tags: a name bound to one snapshot, holding it alive.

A tag is immutable (the binding never changes), immortal (nothing ages it out -
it is dropped or it stays) and charged (the storage it pins is billed). The
name is the small half of the feature; keeping the snapshot alive is the point,
so most of what is worth testing here is the pin.

Tags live in a `tags` subcollection under the dataset document, keyed by the
NORMALIZED tag name. That placement is deliberate and is itself tested below:
the document id doing the naming is what makes uniqueness Firestore's problem
rather than ours, and it keeps tags out of the dataset document, which
`save_dataset_metadata` writes whole with `set()`.
"""

from __future__ import annotations

import json
import time

import pytest

from opteryx_catalog.catalog.expiration import SnapshotExpiration
from opteryx_catalog.catalog.metadata import SNAPSHOT_EXPIRED_AT_KEY
from opteryx_catalog.catalog.metadata import DatasetMetadata
from opteryx_catalog.catalog.metadata import Snapshot
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import SnapshotMissingError
from opteryx_catalog.exceptions import TagAlreadyExists
from opteryx_catalog.exceptions import TagLimitExceeded
from opteryx_catalog.exceptions import TagNotFound
from opteryx_catalog.opteryx_catalog import MAX_TAGS_PER_DATASET
from opteryx_catalog.opteryx_catalog import TAGS_SUBCOLLECTION
from opteryx_catalog.opteryx_catalog import OpteryxCatalog

_DAY_MS = 24 * 60 * 60 * 1000


# --- an in-memory Firestore ---------------------------------------------


class _Doc:
    def __init__(self, id_, data, exists):
        self.id = id_
        self.exists = exists
        self._data = dict(data)

    def to_dict(self):
        return dict(self._data)


class _DocRef:
    def __init__(self, id_):
        self.id = id_
        self._data = {}
        self._exists = False
        self._subcollections = {}

    # `transaction` is accepted and ignored: these fakes are single-threaded,
    # so a transactional read sees the same state an ordinary one would. What
    # the transaction is here to prove is the ORDER (every read before any
    # write), which the real client enforces and `_Transaction` below mimics.
    def get(self, transaction=None):
        return _Doc(self.id, self._data, self._exists)

    def set(self, data, merge=False):
        self._data = {**self._data, **data} if merge else dict(data)
        self._exists = True

    def delete(self):
        self._data = {}
        self._exists = False

    def collection(self, name):
        if name not in self._subcollections:
            self._subcollections[name] = _Collection()
        return self._subcollections[name]


class _Collection:
    def __init__(self):
        self._docs = {}

    def document(self, doc_id):
        if doc_id not in self._docs:
            self._docs[doc_id] = _DocRef(doc_id)
        return self._docs[doc_id]

    def stream(self, transaction=None):
        return [ref.get() for ref in self._docs.values() if ref._exists]


class _Transaction:
    """Enough of a Firestore transaction for `@firestore.transactional`.

    The decorator drives four private methods on whatever it is handed
    (`_clean_up`, `_begin`, `_commit`, plus `_read_only`/`_max_attempts`/`_id`),
    so a double has to answer them. They are implemented here rather than
    mocked away because the point of the double is to keep the read-before-write
    rule honest: `set` after a `get` in the same transaction is what the real
    client allows, and the reverse is what it refuses.
    """

    _read_only = False
    _max_attempts = 1
    _id = b"fake-txn"

    def __init__(self):
        self.writes = []
        self.reads_done = False
        self.committed = False

    def _clean_up(self):
        self.writes = []

    def _begin(self, retry_id=None):
        return None

    def _rollback(self):
        # Reached whenever the wrapped callable raises - a refused create must
        # leave nothing behind. The decorator rolls back and re-raises the
        # ORIGINAL exception, so the typed failures below survive the wrapper.
        self.writes = []

    def _commit(self):
        for ref, data in self.writes:
            if data is None:
                ref.delete()
            else:
                ref.set(data)
        self.committed = True
        return []

    def set(self, ref, data):
        self.writes.append((ref, data))

    def delete(self, ref):
        self.writes.append((ref, None))


class _FirestoreClient:
    def __init__(self):
        self._collections = {}

    def collection(self, name):
        if name not in self._collections:
            self._collections[name] = _Collection()
        return self._collections[name]

    def transaction(self):
        return _Transaction()


def _catalog():
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "ws"
    catalog.io = None
    catalog._snapshot_cache = {}
    catalog._schema_cache = {}
    root = {}

    def datasets_collection(coll):
        return root.setdefault(coll, _Collection())

    catalog._datasets_collection = datasets_collection
    catalog._dataset_doc_ref = lambda c, n: datasets_collection(c).document(n)
    catalog._snapshots_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        "snapshots"
    )
    catalog._tags_collection = lambda c, n: catalog._dataset_doc_ref(c, n).collection(
        TAGS_SUBCOLLECTION
    )
    catalog.firestore_client = _FirestoreClient()
    catalog._catalog_ref = catalog.firestore_client.collection("ws")
    return catalog


def _dataset(catalog, identifier="reports.monthly"):
    coll, name = identifier.split(".", 1)
    catalog._dataset_doc_ref(coll, name).set({"name": name, "collection": coll})
    return identifier


def _snapshot(catalog, identifier, snapshot_id, *, files_size=0, expired=False):
    coll, name = identifier.split(".", 1)
    document = {
        "snapshot-id": snapshot_id,
        "timestamp-ms": int(time.time() * 1000),
        "summary": {"total-files-size": files_size, "total-data-size": files_size * 4},
    }
    if expired:
        document[SNAPSHOT_EXPIRED_AT_KEY] = int(time.time() * 1000)
    catalog._snapshots_collection(coll, name).document(str(snapshot_id)).set(document)
    return snapshot_id


def _audit(capsys):
    return [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]


# --- naming --------------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    [
        "2026_report",  # must start with a letter
        "report-202602",  # no hyphens
        "report.202602",  # no dots - the catalog's own separator
        "_report",  # underscore is not a letter
        "",
        "a" * 129,
    ],
)
def test_invalid_tag_names_are_refused(name):
    with pytest.raises(ValueError):
        OpteryxCatalog.normalize_tag_name(name)


def test_tag_names_normalize_to_lowercase():
    """One tag, one spelling: nothing downstream remembers what was typed."""
    assert OpteryxCatalog.normalize_tag_name("Report_202602") == "report_202602"
    assert OpteryxCatalog.normalize_tag_name("REPORT_202602") == "report_202602"


def test_the_document_id_is_the_normalized_name():
    """Document-id uniqueness and tag-name uniqueness are one constraint.

    If the id kept the typed casing, `MyTag` and `mytag` would be two documents
    for one name, and the uniqueness this design leans on would not hold.
    """
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)

    catalog.create_tag(identifier, "MyTag", 1, author="alice")

    ids = {doc.id for doc in catalog._tags_collection("reports", "monthly").stream()}
    assert ids == {"mytag"}


def test_a_tag_resolves_whatever_casing_is_used():
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 7)
    catalog.create_tag(identifier, "report_202602", 7, author="alice")

    assert catalog.resolve_tag(identifier, "REPORT_202602") == 7
    assert catalog.resolve_tag(identifier, "Report_202602") == 7


# --- creating ------------------------------------------------------------


def test_create_requires_an_author():
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    with pytest.raises(ValueError):
        catalog.create_tag(identifier, "t", 1)


def test_create_reports_the_bytes_it_pins():
    """The caller taking on an open-ended storage cost is told what it costs.

    The reported figure is the LOGICAL size, because that is what storage
    billing charges (deliberately - the compression spread is margin). Quoting
    the on-disk size would name a number far below the invoice. The fixture
    makes the two differ so a swap shows up here rather than on a bill.
    """
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1, files_size=4096)

    record = catalog.create_tag(identifier, "report_202602", 1, author="alice")

    # The fixture stores total-data-size as 4x total-files-size.
    assert record["pinned-bytes"] == 4096 * 4
    assert record["pinned-bytes-on-disk"] == 4096
    assert record["snapshot-id"] == 1
    assert record["created-by"] == "alice"


def test_a_tag_cannot_be_repointed():
    """Immutability: re-creating a name is refused, not silently rebound."""
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    _snapshot(catalog, identifier, 2)
    catalog.create_tag(identifier, "report_202602", 1, author="alice")

    with pytest.raises(TagAlreadyExists) as err:
        catalog.create_tag(identifier, "report_202602", 2, author="alice")

    # The message has to say how to move it, or immutability just reads as a bug.
    assert "drop" in str(err.value).lower()
    assert catalog.resolve_tag(identifier, "report_202602") == 1


def test_a_missing_snapshot_cannot_be_tagged():
    catalog = _catalog()
    identifier = _dataset(catalog)
    with pytest.raises(SnapshotMissingError):
        catalog.create_tag(identifier, "report_202602", 999, author="alice")


def test_an_expired_snapshot_cannot_be_tagged():
    """A tombstoned snapshot's files are already on their way out of storage.

    Tagging one would produce exactly the dangling tag that pinning exists to
    make impossible, so the liveness check reads the snapshot document itself
    rather than trusting that it was listed.
    """
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 5, expired=True)

    with pytest.raises(SnapshotMissingError):
        catalog.create_tag(identifier, "report_202602", 5, author="alice")


def test_the_cap_is_enforced():
    """Nothing ages a tag out, so the cap is the only bound on pinned history."""
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    for index in range(MAX_TAGS_PER_DATASET):
        catalog.create_tag(identifier, f"t{index}", 1, author="alice")

    with pytest.raises(TagLimitExceeded) as err:
        catalog.create_tag(identifier, "one_too_many", 1, author="alice")
    assert str(MAX_TAGS_PER_DATASET) in str(err.value)


def test_creating_a_tag_is_audited(capsys):
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 3, files_size=512)
    catalog.create_tag(identifier, "report_202602", 3, author="alice")

    record = _audit(capsys)[-1]
    assert record["action"] == "create_tag"
    assert record["author"] == "alice"
    assert record["detail"]["tag"] == "report_202602"
    assert record["detail"]["snapshot_id"] == 3
    assert record["detail"]["pinned_bytes"] == 512 * 4
    assert record["detail"]["pinned_bytes_on_disk"] == 512


# --- dropping ------------------------------------------------------------


def test_drop_requires_an_author():
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    catalog.create_tag(identifier, "t", 1, author="alice")
    with pytest.raises(ValueError):
        catalog.drop_tag(identifier, "t")


def test_dropping_an_absent_tag_raises_unless_allowed():
    catalog = _catalog()
    identifier = _dataset(catalog)
    with pytest.raises(TagNotFound):
        catalog.drop_tag(identifier, "nope", author="alice")
    catalog.drop_tag(identifier, "nope", author="alice", missing_ok=True)


def test_drop_removes_the_tag_and_is_audited(capsys):
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 4)
    catalog.create_tag(identifier, "report_202602", 4, author="alice")
    capsys.readouterr()

    catalog.drop_tag(identifier, "report_202602", author="bob")

    assert catalog.list_tags(identifier) == []
    record = _audit(capsys)[-1]
    assert record["action"] == "drop_tag"
    assert record["author"] == "bob"
    assert record["detail"]["snapshot_id"] == 4


def test_the_name_is_reusable_after_a_drop():
    """Dropping and re-creating is the sanctioned way to move a name."""
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    _snapshot(catalog, identifier, 2)
    catalog.create_tag(identifier, "latest_report", 1, author="alice")
    catalog.drop_tag(identifier, "latest_report", author="alice")
    catalog.create_tag(identifier, "latest_report", 2, author="alice")

    assert catalog.resolve_tag(identifier, "latest_report") == 2


# --- listing and resolving ----------------------------------------------


def test_list_tags_is_ordered_by_name():
    catalog = _catalog()
    identifier = _dataset(catalog)
    _snapshot(catalog, identifier, 1)
    for name in ("zulu", "alpha", "mike"):
        catalog.create_tag(identifier, name, 1, author="alice")

    assert [tag["name"] for tag in catalog.list_tags(identifier)] == ["alpha", "mike", "zulu"]


def test_resolving_an_unknown_tag_raises():
    """Not None: an absent tag is a question with no answer, not an answer."""
    catalog = _catalog()
    identifier = _dataset(catalog)
    with pytest.raises(TagNotFound):
        catalog.resolve_tag(identifier, "nope")


# --- the pin -------------------------------------------------------------


def _snap(sid, seq, age_days):
    return Snapshot(
        snapshot_id=sid,
        timestamp_ms=int(time.time() * 1000) - int(age_days * _DAY_MS),
        sequence_number=seq,
        user_created=True,
        manifest_list=f"manifest-{sid}.parquet",
    )


class _FakeDataset:
    def __init__(self, snapshots):
        self.metadata = DatasetMetadata(
            dataset_identifier="reports.monthly", location="mem://", schema=None, properties={}
        )
        self.metadata.snapshots = list(snapshots)
        self.metadata.current_snapshot_id = snapshots[-1].snapshot_id
        self.metadata.maintenance_policy = {"retained-snapshot-age-days": 7}


class _PinCatalog:
    def __init__(self, dataset, tags):
        self._dataset = dataset
        self._tags = tags

    def load_dataset(self, identifier, load_history=False):
        return self._dataset

    def list_tags(self, identifier):
        return list(self._tags)


def _expire(snapshots, tags):
    """(kept_ids, deleted_ids) for one expiration pass."""
    captured = {}
    expirer = SnapshotExpiration(_PinCatalog(_FakeDataset(snapshots), tags))

    def _capture(identifier, dataset, snapshots_to_delete, snapshots_to_keep, **kwargs):
        captured["keep"] = {s.snapshot_id for s in snapshots_to_keep}
        captured["delete"] = {s.snapshot_id for s in snapshots_to_delete}
        return {}

    expirer._execute_expiration = _capture
    expirer.expire_dataset("reports.monthly", dry_run=False)
    return captured.get("keep", set()), captured.get("delete", set())


def test_a_tagged_snapshot_survives_its_retention_window():
    """The whole feature. Retention is 7 days; the tagged snapshot is 90 old."""
    snapshots = [_snap(1, 1, age_days=90), _snap(2, 2, age_days=60), _snap(3, 3, age_days=0)]

    keep, delete = _expire(snapshots, tags=[{"name": "report_202602", "snapshot-id": 1}])

    assert 1 in keep, "a tagged snapshot was expired"
    assert 1 not in delete
    # Nothing else is protected by association - the tag pins one snapshot.
    assert 2 in delete


def test_dropping_the_tag_releases_the_snapshot():
    """Dropping a tag IS how you agree to lose the data it was holding."""
    snapshots = [_snap(1, 1, age_days=90), _snap(2, 2, age_days=60), _snap(3, 3, age_days=0)]

    keep, _ = _expire(snapshots, tags=[{"name": "report_202602", "snapshot-id": 1}])
    assert 1 in keep

    _, delete = _expire(snapshots, tags=[])
    assert 1 in delete


def test_several_tags_pin_several_snapshots():
    snapshots = [_snap(i, i, age_days=90 - i) for i in range(1, 6)] + [_snap(9, 9, age_days=0)]

    keep, _ = _expire(
        snapshots,
        tags=[{"name": "a", "snapshot-id": 2}, {"name": "b", "snapshot-id": 4}],
    )

    assert {2, 4}.issubset(keep)


def test_an_unreadable_tag_list_stops_expiration_rather_than_deleting():
    """"No tags" and "the tags could not be read" are not the same answer.

    Degrading to the empty set here would delete precisely the snapshots the
    tags exist to protect, which is the one outcome this feature cannot have.

    It ABORTS rather than quietly returning nothing to do: an unestablishable
    protected set is already `ManifestProtectionError` everywhere else in
    expiration, and that is what `expire_collection` catches, alerts on and
    records in `datasets_skipped_unprotectable`. Returning None instead would
    make an unprotectable dataset indistinguishable from a clean one.
    """
    snapshots = [_snap(1, 1, age_days=90), _snap(2, 2, age_days=0)]

    class _BrokenCatalog(_PinCatalog):
        def list_tags(self, identifier):
            raise ConnectionError("firestore unavailable")

    captured = {}
    expirer = SnapshotExpiration(_BrokenCatalog(_FakeDataset(snapshots), []))
    expirer._execute_expiration = lambda *a, **k: captured.setdefault("ran", True)

    with pytest.raises(ManifestProtectionError):
        expirer.expire_dataset("reports.monthly", dry_run=False)

    assert "ran" not in captured, "expiration proceeded without knowing what was tagged"
