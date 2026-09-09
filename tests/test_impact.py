"""Who is downstream of a dataset: impact, and the forensic version.

Two questions with two different right answers, and the tests exist mostly to
keep them apart. `find_impacted` is about CURRENT content and must forget a
consumer that has been rebuilt from something else; `find_readers` is about
what commits actually read, and must remember it.

The fake Firestore models the one behaviour both rest on: a collection-group
query matching `array_contains` on a flat scalar array, with each document
carrying the full path it sits at - the workspace is written on neither
document and is read off that path.
"""

from __future__ import annotations

import pytest

from opteryx_catalog.impact import find_impacted
from opteryx_catalog.impact import find_readers

SRC = "ops.ingest.stdout_log"


class _Ref:
    def __init__(self, path):
        self.path = path


class _Doc:
    def __init__(self, doc_id, data, path):
        self.id = doc_id
        self._data = dict(data)
        self.reference = _Ref(path)

    def to_dict(self):
        return dict(self._data)


class _Query:
    def __init__(self, docs, filters=()):
        self._docs = list(docs)
        self._filters = list(filters)

    def where(self, filter=None):
        return _Query(self._docs, self._filters + [filter])

    def _matches(self, doc, f):
        # Backticks are field-path GRAMMAR, not part of the name - the real
        # service strips them to resolve the field, and a fake that matched
        # the literal string would pass a query the server refuses.
        value = doc.to_dict().get(f.field_path.strip("`"))
        if f.op_string == "array_contains":
            return isinstance(value, (list, tuple)) and f.value in value
        raise AssertionError(f"unexpected operator in this query: {f.op_string}")

    def stream(self):
        return [doc for doc in self._docs if all(self._matches(doc, f) for f in self._filters)]


class _Client:
    def __init__(self):
        self._docs = []

    def add(self, path, data):
        self._docs.append(_Doc(path.split("/")[-1], data, path))
        return self

    def dataset(self, path, sources, complete=True):
        return self.add(path, {"sources": list(sources), "sources-complete": complete})

    def commit(self, path, reads, produced_by=None, expired=False, at=None):
        """One snapshot whose receipt read `reads`, as (dataset, version)."""
        entries = [
            {"dataset": d, "snapshot-id": v, "resolved-by": "current"} for d, v in reads
        ]
        keys = []
        for d, v in reads:
            if d not in keys:
                keys.append(d)
        keys += [f"{d}@{v}" for d, v in reads if v is not None]
        snapshot_id = int(path.split("/")[-1])
        doc = {
            "snapshot-id": snapshot_id,
            "timestamp-ms": at if at is not None else snapshot_id,
            "read-sources": entries,
            "read-source-keys": keys,
        }
        if produced_by:
            doc["produced-by"] = produced_by
        if expired:
            doc["expired-at-ms"] = 1
        return self.add(path, doc)

    def collection_group(self, name):
        return _Query([d for d in self._docs if d.reference.path.split("/")[-2] == name])


# ── impact: who is built from this now ───────────────────────────────────────


def test_a_consumer_in_another_workspace_is_named_in_full():
    """The decision this whole read rests on: an impact answer that will not
    say who is affected has not answered the question."""
    client = _Client().dataset("platform/billing/datasets/events", [SRC])

    answer = find_impacted(client, SRC)

    assert answer["consumers"] == [
        {"dataset": "platform.billing.events", "workspace": "platform", "sources_complete": True}
    ]
    assert answer["truncated"] is False


def test_a_dataset_built_from_something_else_is_not_impacted():
    client = _Client().dataset("platform/billing/datasets/events", ["ops.ingest.other"])

    assert find_impacted(client, SRC)["consumers"] == []


def test_an_incomplete_consumer_says_so():
    """Its list may be missing names, which qualifies this answer as much as
    it qualifies the consumer's own."""
    client = _Client().dataset("platform/billing/datasets/events", [SRC], complete=False)

    assert find_impacted(client, SRC)["consumers"][0]["sources_complete"] is False


def test_consumers_come_back_in_a_stable_order():
    client = (
        _Client()
        .dataset("zeta/z/datasets/last", [SRC])
        .dataset("alpha/a/datasets/first", [SRC])
    )

    assert [row["dataset"] for row in find_impacted(client, SRC)["consumers"]] == [
        "alpha.a.first",
        "zeta.z.last",
    ]


def test_the_answer_is_capped_and_says_when_the_cap_bit():
    client = _Client()
    for index in range(70):
        client.dataset(f"ws{index:03d}/c/datasets/d", [SRC])

    answer = find_impacted(client, SRC, limit=64)

    assert len(answer["consumers"]) == 64
    assert answer["truncated"] is True


@pytest.mark.parametrize("bad", ["", "events", "billing.events"])
def test_an_unqualified_dataset_is_refused(bad):
    for lookup in (find_impacted, find_readers):
        with pytest.raises(ValueError):
            lookup(_Client(), bad)


# ── forensic: whose commits read this ────────────────────────────────────────


def test_commits_are_folded_to_one_row_per_consuming_dataset():
    """A table refreshed hourly would otherwise bury every other consumer."""
    client = (
        _Client()
        .commit("platform/billing/datasets/events/snapshots/100", [(SRC, 10)])
        .commit("platform/billing/datasets/events/snapshots/200", [(SRC, 11)], produced_by="task:platform.billing.ingest")
    )

    rows = find_readers(client, SRC)["consumers"]

    assert len(rows) == 1
    assert rows[0]["dataset"] == "platform.billing.events"
    assert rows[0]["commits"] == 2
    assert rows[0]["latest_snapshot_id"] == 200
    assert rows[0]["versions_read"] == [10, 11]
    # The producer of the LATEST commit; a task can be repointed.
    assert rows[0]["produced_by"] == "task:platform.billing.ingest"


def test_a_reader_that_has_since_moved_on_is_still_a_reader():
    """The difference from `find_impacted`: this one remembers. The consumer's
    standing list no longer names the source, but its commit did read it."""
    client = (
        _Client()
        .dataset("platform/billing/datasets/events", ["ops.ingest.other"])
        .commit("platform/billing/datasets/events/snapshots/100", [(SRC, 10)])
    )

    assert find_impacted(client, SRC)["consumers"] == []
    assert [r["dataset"] for r in find_readers(client, SRC)["consumers"]] == [
        "platform.billing.events"
    ]


def test_asking_about_one_version_finds_only_the_commits_that_read_it():
    client = (
        _Client()
        .commit("platform/billing/datasets/events/snapshots/100", [(SRC, 10)])
        .commit("platform/reports/datasets/daily/snapshots/200", [(SRC, 11)])
    )

    rows = find_readers(client, SRC, snapshot_id=11)["consumers"]

    assert [r["dataset"] for r in rows] == ["platform.reports.daily"]
    assert rows[0]["versions_read"] == [11]


def test_an_expired_commit_is_not_a_reader():
    client = _Client().commit(
        "platform/billing/datasets/events/snapshots/100", [(SRC, 10)], expired=True
    )

    assert find_readers(client, SRC)["consumers"] == []


def test_a_commit_that_read_something_else_is_not_a_reader():
    client = _Client().commit(
        "platform/billing/datasets/events/snapshots/100", [("ops.ingest.other", 10)]
    )

    assert find_readers(client, SRC)["consumers"] == []


def test_a_source_read_with_no_commits_yet_is_matched_by_its_bare_name():
    """`read-source-keys` carries the bare name as well as name@version, which
    is the only way to ask "at any version" - `array_contains` is equality
    only, with no prefix matching."""
    client = _Client().commit(
        "platform/billing/datasets/events/snapshots/100", [(SRC, None)]
    )

    rows = find_readers(client, SRC)["consumers"]

    assert rows[0]["versions_read"] == [None]


def test_readers_are_capped_and_say_when_the_cap_bit():
    client = _Client()
    for index in range(70):
        client.commit(f"ws{index:03d}/c/datasets/d/snapshots/{index + 1}", [(SRC, 10)])

    answer = find_readers(client, SRC, limit=64)

    assert len(answer["consumers"]) == 64
    assert answer["truncated"] is True
