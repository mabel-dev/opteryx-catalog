"""`scripts/backfill_task_reads.py` - deriving `reads` for tasks registered
before the field existed, from the statement each one now runs.

The derivation is the engine's own AST walk, so these tests run it for real
rather than faking the parser: what is under test is the qualification and
exclusion applied on top of it, and a fake parser would only prove the fake.
The engine must be importable (`PYTHONPATH=<catalog>:<opteryx-core>`).

The fake Firestore addresses documents by path, with a collection group
selected by the name of the collection a document sits in, extended with the
two things this script does that a plain read never does: follow a task to
its `statement` subcollection, and `update`.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# The derivation borrows the engine's parser (see the script's docstring for
# why it is not reimplemented here). Without the engine on the path these
# tests cannot run, and "the catalog suite needs opteryx-core" would be a new
# dependency nothing else in this repo has - so they skip rather than fail.
_parser = pytest.importorskip("opteryx.utils.query_parser")
if not hasattr(_parser, "extract_write_targets"):
    # An installed engine wheel that predates the write-target split: the
    # derivation cannot run against it, and the script says so at run time.
    pytest.skip("installed opteryx predates extract_write_targets", allow_module_level=True)

_SCRIPT = Path(__file__).parent.parent / "scripts" / "backfill_task_reads.py"
_spec = importlib.util.spec_from_file_location("backfill_task_reads", _SCRIPT)
backfill = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = backfill
_spec.loader.exec_module(backfill)


class _Doc:
    def __init__(self, reference, data):
        self.id = reference.path.split("/")[-1]
        self.reference = reference
        self.exists = data is not None
        self._data = dict(data or {})

    def to_dict(self):
        return dict(self._data) if self.exists else None


class _Ref:
    """A document reference that knows its path and resolves against the client
    at read time, so a write made through one reference is seen by the next."""

    def __init__(self, client, path):
        self._client = client
        self.path = path

    def get(self):
        return _Doc(self, self._client._docs.get(self.path))

    def collection(self, name):
        return _Collection(self._client, f"{self.path}/{name}")

    def update(self, fields):
        assert self.path in self._client._docs, "update on a document that does not exist"
        self._client.updates.append((self.path, dict(fields)))
        self._client._docs[self.path].update(fields)


class _Collection:
    def __init__(self, client, path):
        self._client = client
        self.path = path

    def document(self, doc_id):
        return _Ref(self._client, f"{self.path}/{doc_id}")


class _Query:
    def __init__(self, docs):
        self._docs = docs

    def stream(self):
        return list(self._docs)


class _Client:
    """Documents by full path; `update` calls recorded in order, with the exact
    field map each one carried."""

    def __init__(self):
        self._docs: dict[str, dict] = {}
        self.updates: list[tuple[str, dict]] = []

    def add(self, path, data):
        self._docs[path] = dict(data)
        return self

    def task(self, path, sql, **fields):
        """A task document plus the statement it points at, laid out the way
        `create_task` writes them. `reads` is absent unless passed; pass
        `reads=[]` for the shape a `reads=None` registration left behind."""
        record = {"name": path.split("/")[-1], "statement-id": "s1", "writes": []}
        record.update(fields)
        self.add(path, record)
        self.add(f"{path}/statement/s1", {"sql": sql})
        return self

    def collection_group(self, name):
        return _Query(
            _Doc(_Ref(self, path), data)
            for path, data in list(self._docs.items())
            if path.split("/")[-2] == name
        )


INSERT_FROM_RAW = "INSERT INTO billing.events SELECT * FROM ingest.raw"


# --- derivation ---------------------------------------------------------------


def test_two_part_names_are_qualified_with_the_tasks_own_workspace():
    reads = backfill.derive_reads(
        "INSERT INTO billing.events SELECT * FROM ingest.raw JOIN other.ws.lookup USING (id)",
        "ops",
    )

    # The two-part name is relative to the task's workspace; the three-part one
    # already says whose it is and is kept as written.
    assert reads == ["ops.ingest.raw", "other.ws.lookup"]


def test_virtual_and_information_schema_relations_are_not_reads():
    reads = backfill.derive_reads(
        "INSERT INTO billing.events SELECT * FROM ingest.raw, $planets, information_schema.tables",
        "ops",
    )

    assert reads == ["ops.ingest.raw"]


@pytest.mark.parametrize(
    "sql",
    [
        (
            "MERGE INTO billing.events t USING ingest.raw s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET x = s.x"
        ),
        "UPDATE billing.events SET x = 1 WHERE id IN (SELECT id FROM ingest.raw)",
        "DELETE FROM billing.events WHERE id IN (SELECT id FROM ingest.raw)",
    ],
)
def test_a_write_target_is_not_a_read(sql):
    """The table a MERGE, UPDATE or DELETE lands on is in the AST walk like any
    other relation; counting it would make every writer a reader of itself."""
    assert backfill.derive_reads(sql, "ops") == ["ops.ingest.raw"]


def test_a_statement_reading_no_catalog_relation_derives_an_empty_list():
    assert backfill.derive_reads("INSERT INTO billing.events VALUES (1, 2)", "ops") == []


def test_reads_are_sorted_and_distinct():
    reads = backfill.derive_reads(
        "INSERT INTO billing.events "
        "SELECT * FROM ingest.zulu z JOIN ingest.alpha a ON 1=1 JOIN ops.ingest.zulu q ON 1=1",
        "ops",
    )

    assert reads == ["ops.ingest.alpha", "ops.ingest.zulu"]


def test_an_unparseable_statement_raises_rather_than_reading_as_empty():
    with pytest.raises(ValueError):
        backfill.derive_reads("SELEC nonsense FROM", "ops")


def test_a_missing_engine_is_a_clear_error(monkeypatch):
    """The parser is imported at call time so the catalog never depends on the
    engine at import; when it is absent the message says what to install."""
    # Every engine module already imported by an earlier test has to go, not
    # just the package: `from opteryx.third_party import sqloxide` is satisfied
    # straight from `sys.modules` without ever touching the parent.
    for name in list(sys.modules):
        if name == "opteryx" or name.startswith("opteryx."):
            monkeypatch.setitem(sys.modules, name, None)

    with pytest.raises(RuntimeError, match="opteryx-core"):
        backfill.derive_reads("SELECT 1", "ops")


# --- the walk ---------------------------------------------------------------


def test_the_stored_and_derived_lists_are_compared_per_task():
    client = (
        _Client()
        .task("ops/ingest/tasks/agrees", INSERT_FROM_RAW, reads=["ops.ingest.raw"])
        .task("ops/ingest/tasks/never_asked", INSERT_FROM_RAW)
        .task(
            "ops/ingest/tasks/stale",
            "INSERT INTO billing.events SELECT * FROM ingest.other",
            reads=["ops.ingest.raw"],
        )
    )

    actions = {a["task"]: a for a in backfill.plan(backfill.collect(client))}

    assert actions["ops.ingest.agrees"]["action"] == "current"
    assert actions["ops.ingest.never_asked"]["action"] == "update"
    assert actions["ops.ingest.never_asked"]["stored"] is None
    assert actions["ops.ingest.stale"]["action"] == "update"
    assert actions["ops.ingest.stale"]["derived"] == ["ops.ingest.other"]


def test_an_unparseable_statement_is_reported_and_skipped():
    client = _Client().task("ops/ingest/tasks/broken", "SELEC nonsense FROM")

    lines = []
    counts = backfill.run(client, None, apply_changes=True, out=lines.append)

    assert counts["unparseable"] == 1
    assert counts["updated"] == 0
    assert client.updates == []
    assert any(line.startswith("[SKIPPED] ops.ingest.broken") for line in lines)


def test_a_dry_run_writes_nothing():
    client = _Client().task("ops/ingest/tasks/never_asked", INSERT_FROM_RAW)

    lines = []
    counts = backfill.run(client, None, apply_changes=False, out=lines.append)

    assert counts["updated"] == 1
    assert client.updates == []
    assert any(line.startswith("[would]   ops.ingest.never_asked") for line in lines)


def test_apply_updates_only_reads_and_only_where_it_differs():
    client = (
        _Client()
        .task("ops/ingest/tasks/agrees", INSERT_FROM_RAW, reads=["ops.ingest.raw"])
        .task(
            "ops/ingest/tasks/never_asked",
            "INSERT INTO billing.events SELECT * FROM ingest.raw JOIN other.ws.lookup USING (id)",
        )
        .task(
            "platform/reports/tasks/stale",
            "MERGE INTO billing.events t USING ingest.raw s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET x = s.x",
            reads=["platform.billing.events", "platform.ingest.raw"],
            trigger={"source": "platform.ingest.raw", "name": "on_raw"},
        )
    )

    counts = backfill.run(client, None, apply_changes=True, out=lambda _: None)

    assert counts == {"seen": 3, "unchanged": 1, "updated": 2, "unparseable": 0, "skipped": 0}
    # Exactly the two that differed, each an update of `reads` alone: the
    # trigger back-pointer and everything else on the document is untouched.
    assert client.updates == [
        ("ops/ingest/tasks/never_asked", {"reads": ["ops.ingest.raw", "other.ws.lookup"]}),
        ("platform/reports/tasks/stale", {"reads": ["platform.ingest.raw"]}),
    ]
    assert client._docs["platform/reports/tasks/stale"]["trigger"] == {
        "source": "platform.ingest.raw",
        "name": "on_raw",
    }


def test_an_absent_key_is_written_even_when_the_statement_reads_nothing():
    """Never asked and reads-nothing are different facts (S2.4): the first is
    replaced by the second, and the second is left alone."""
    client = (
        _Client()
        .task("ops/ingest/tasks/never_asked", "INSERT INTO billing.events VALUES (1)")
        .task("ops/ingest/tasks/answered", "INSERT INTO billing.events VALUES (1)", reads=[])
    )

    counts = backfill.run(client, None, apply_changes=True, out=lambda _: None)

    assert counts["updated"] == 1
    assert counts["unchanged"] == 1
    assert client.updates == [("ops/ingest/tasks/never_asked", {"reads": []})]


def test_named_workspaces_limit_the_walk():
    client = (
        _Client()
        .task("ops/ingest/tasks/mine", INSERT_FROM_RAW)
        .task("platform/reports/tasks/theirs", INSERT_FROM_RAW)
    )

    assert [t["task"] for t in backfill.collect(client, {"platform"})] == [
        "platform.reports.theirs"
    ]
    assert [t["task"] for t in backfill.collect(client)] == [
        "ops.ingest.mine",
        "platform.reports.theirs",
    ]


def test_a_statement_redefined_since_the_plan_is_not_overwritten():
    """A redefinition through a current engine has already recorded the right
    `reads` from the new text; a write derived from the old text would undo it."""
    client = _Client().task("ops/ingest/tasks/moving", INSERT_FROM_RAW)
    (action,) = backfill.plan(backfill.collect(client))
    client._docs["ops/ingest/tasks/moving"]["statement-id"] = "s2"

    refused = backfill.apply(action)

    assert refused is not None
    assert client.updates == []
