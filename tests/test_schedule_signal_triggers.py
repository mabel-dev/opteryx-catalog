"""Clock and signal triggers: task-held, event-plus-identity, windowed OVER.

A trigger is an EVENT plus a `runs-as`. A commit trigger is held by the dataset
whose commits fire it; a schedule or signal trigger has no dataset, so it is
held by the task it fires. These cover the catalog side (holder, record,
one-trigger rule across holders, the tick claim) and the firing side
(`fire_signal`, `fire_due_schedules`, the OVER window, the windowless check).

The Firestore stand-ins are the materialized-view tests' ones.
"""

from __future__ import annotations

import contextlib
from unittest.mock import patch

import pytest
from test_materialized_views import _add_dataset
from test_materialized_views import _catalog

from opteryx_catalog import trigger_firing
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import MaterializedViewError
from opteryx_catalog.exceptions import TaskNotFound
from opteryx_catalog.exceptions import TriggerNotFound
from opteryx_catalog.opteryx_catalog import SCHEDULE_EVENT_KIND
from opteryx_catalog.opteryx_catalog import SIGNAL_EVENT_KIND
from opteryx_catalog.opteryx_catalog import TASK_HOLDER
from opteryx_catalog.schedules import next_due_ms
from opteryx_catalog.schedules import occurrences_between
from opteryx_catalog.schedules import validate_schedule
from opteryx_catalog.trigger_firing import fire_due_schedules
from opteryx_catalog.trigger_firing import fire_signal

HOUR_MS = 60 * 60 * 1000
# 2026-09-02T10:00:00Z
T0 = 1788343200000


def _task(catalog, identifier="ops.rollup", sql="INSERT INTO ops.out SELECT 1", author="alice"):
    catalog.create_task(identifier, sql=sql, author=author)
    return identifier


def _schedule(catalog, task="ops.rollup", name="hourly", schedule="0 * * * *", **kwargs):
    catalog.create_trigger(
        task,
        name,
        target_task=task,
        kind="task",
        author=kwargs.pop("author", "alice"),
        holder_kind=TASK_HOLDER,
        event_kind=SCHEDULE_EVENT_KIND,
        schedule=schedule,
        **kwargs,
    )


def _signal(catalog, task="ops.rollup", name="on_demand", **kwargs):
    catalog.create_trigger(
        task,
        name,
        target_task=task,
        kind="task",
        author=kwargs.pop("author", "alice"),
        holder_kind=TASK_HOLDER,
        event_kind=SIGNAL_EVENT_KIND,
        **kwargs,
    )


def _held(catalog, task="ops.rollup"):
    triggers = catalog.list_triggers(task, holder_kind=TASK_HOLDER)
    assert len(triggers) <= 1, "a task holds at most one trigger"
    return triggers[0] if triggers else None


# --- the schedule module ----------------------------------------------------------


def test_next_due_is_strictly_after_and_in_the_zone():
    # 10:00Z on the hour: the next hourly instant is 11:00Z, not 10:00Z again.
    assert next_due_ms("0 * * * *", "UTC", T0) == T0 + HOUR_MS
    # 09:00 London in September is 08:00Z; from 10:00Z that is tomorrow.
    assert next_due_ms("0 9 * * *", "Europe/London", T0) == T0 + 22 * HOUR_MS


def test_schedules_are_five_fields_in_a_known_zone():
    assert validate_schedule("  0   *  * * *  ", None) == ("0 * * * *", "UTC")
    with pytest.raises(ValueError, match="five-field"):
        validate_schedule("* * * * * *")
    with pytest.raises(ValueError, match="not a valid cron"):
        validate_schedule("99 * * * *")
    with pytest.raises(ValueError, match="unknown time zone"):
        validate_schedule("0 * * * *", "Mars/Olympus")
    with pytest.raises(ValueError, match="requires a cron"):
        validate_schedule("")


def test_occurrences_between_excludes_the_start_slot():
    # (10:00, 10:00:30] holds nothing; (10:00, 11:00:30] holds 11:00 only.
    assert occurrences_between("0 * * * *", "UTC", T0, T0 + 30_000) == 0
    assert occurrences_between("0 * * * *", "UTC", T0, T0 + HOUR_MS + 30_000) == 1
    assert occurrences_between("0 * * * *", "UTC", T0, T0) == 0


# --- the record and the holder ------------------------------------------------------


def test_a_schedule_trigger_is_held_by_its_task():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog, schedule="0 * * * *", time_zone="Europe/London")

    trigger = _held(catalog)
    assert trigger["event-kind"] == "schedule"
    assert trigger["holder"] == "ws.ops.rollup"
    assert trigger["holder-kind"] == "task"
    assert trigger["schedule"] == "0 * * * *"
    assert trigger["time-zone"] == "Europe/London"
    assert trigger["next-due-at-ms"] == T0 + HOUR_MS
    assert trigger["window-source"] is None
    # Event plus identity: the trigger carries the run's identity, the task none.
    assert trigger["runs-as"] == "alice"
    assert trigger["target-task"] == "ws.ops.rollup"
    task = catalog.get_task("ops.rollup")
    assert "runs-as" not in task
    assert task["trigger"] == {"source": "ws.ops.rollup", "name": "hourly"}


def test_a_signal_trigger_has_no_schedule():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    trigger = _held(catalog)
    assert trigger["event-kind"] == "signal"
    assert trigger["schedule"] is None and trigger["next-due-at-ms"] is None

    with pytest.raises(ValueError, match="signal trigger has no schedule"):
        _signal(catalog, name="other", schedule="0 * * * *")


def test_the_event_and_the_holder_must_agree():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _task(catalog)
    # A schedule on a dataset holder: the commit path would never read it.
    with pytest.raises(ValueError, match="held by the task it fires"):
        catalog.create_trigger(
            "src.a", "t", target_task="ops.rollup", kind="task", author="alice",
            event_kind=SCHEDULE_EVENT_KIND, schedule="0 * * * *",
        )
    with pytest.raises(ValueError, match="belong to a schedule or signal trigger"):
        catalog.create_trigger(
            "src.a", "t", target_task="ops.rollup", kind="task", author="alice",
            window_source="src.a",
        )
    # A commit event on a task holder: nothing commits to a task.
    with pytest.raises(ValueError, match="held by the dataset whose commits fire it"):
        catalog.create_trigger(
            "ops.rollup", "t", target_task="ops.rollup", kind="task", author="alice",
            holder_kind=TASK_HOLDER, event_kind="commit",
        )
    # A task-held trigger runs its holder and nothing else.
    _task(catalog, "ops.other")
    with pytest.raises(ValueError, match="runs that task, not"):
        _schedule(catalog, task="ops.rollup", target_task_override="ops.other") if False else catalog.create_trigger(
            "ops.rollup", "t", target_task="ops.other", kind="task", author="alice",
            holder_kind=TASK_HOLDER, event_kind=SIGNAL_EVENT_KIND,
        )
    with pytest.raises(ValueError, match="unknown trigger event kind"):
        catalog.create_trigger(
            "ops.rollup", "t", target_task="ops.rollup", kind="task", author="alice",
            holder_kind=TASK_HOLDER, event_kind="every",
        )


def test_the_holder_task_must_exist():
    catalog = _catalog()
    with pytest.raises(TaskNotFound):
        _signal(catalog, task="ops.missing")


def test_a_platform_identity_cannot_arm_a_clock_either():
    from opteryx_catalog.exceptions import PlatformIdentityOwnerRefused

    catalog = _catalog()
    _task(catalog, author="alice")
    with pytest.raises(PlatformIdentityOwnerRefused):
        _schedule(catalog, author="federator")
    assert _held(catalog) is None


# --- the window -----------------------------------------------------------------------


def test_a_sourceless_clock_refuses_a_windowed_statement():
    catalog = _catalog()
    _task(catalog, sql="INSERT INTO ops.out SELECT * FROM src.a WHERE v > :parent_version")
    with pytest.raises(ValueError, match=r"consumes a window \(:parent_version\).*OVER <table>"):
        _schedule(catalog)
    assert _held(catalog) is None, "a refused arming leaves nothing behind"


def test_over_binds_the_window_to_a_dataset():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _task(catalog, sql="INSERT INTO ops.out SELECT * FROM src.a WHERE v > :parent_version")
    _schedule(catalog, window_source="src.a")
    assert _held(catalog)["window-source"] == "ws.src.a"


def test_the_over_dataset_must_exist_here():
    catalog = _catalog()
    _task(catalog)
    with pytest.raises(DatasetNotFound, match="window source not found"):
        _signal(catalog, window_source="src.missing")
    with pytest.raises(MaterializedViewError, match="belongs to workspace other"):
        _signal(catalog, window_source="other.src.a")
    assert _held(catalog) is None


# --- the one-trigger rule, across holders ---------------------------------------------


def test_a_task_has_one_trigger_whichever_holder():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _task(catalog)
    catalog.create_trigger("src.a", "on_commit", target_task="ops.rollup", kind="task", author="alice")

    with pytest.raises(MaterializedViewError, match="already fired by on_commit ON ws.src.a"):
        _schedule(catalog)
    assert _held(catalog) is None

    # And the other way round.
    catalog.drop_trigger("src.a", "on_commit", author="alice")
    _schedule(catalog)
    with pytest.raises(MaterializedViewError, match="already fired by hourly ON ws.ops.rollup"):
        catalog.create_trigger("src.a", "on_commit", target_task="ops.rollup", kind="task", author="alice")
    with pytest.raises(MaterializedViewError, match="already fired by hourly"):
        _signal(catalog)

    # Dropping through the task holder releases the pointer.
    catalog.drop_trigger("ops.rollup", "hourly", author="alice", holder_kind=TASK_HOLDER)
    assert catalog.get_task("ops.rollup")["trigger"] is None
    _signal(catalog)
    assert _held(catalog)["name"] == "on_demand"


def test_or_replace_recomputes_the_due_instant_and_keeps_the_identity():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog, schedule="0 * * * *")
    catalog.set_trigger_owner("ops.rollup", "hourly", "bob", author="alice", holder_kind=TASK_HOLDER)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog, schedule="30 * * * *")
    trigger = _held(catalog)
    assert trigger["schedule"] == "30 * * * *"
    assert trigger["next-due-at-ms"] == T0 + 30 * 60 * 1000
    assert trigger["runs-as"] == "bob", "re-registration never transfers whose authority the work runs with"


# --- the trigger methods through a task holder -----------------------------------------


def test_suspend_and_resume_through_the_task_holder():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)
    catalog.set_trigger_suspended("ops.rollup", "hourly", True, author="alice", holder_kind=TASK_HOLDER)
    assert _held(catalog)["suspended-by"] == "alice"

    # RESUME recomputes from now: a week later, the due instant is the next
    # occurrence after now, not the slot it was paused on.
    later = T0 / 1000 + 7 * 24 * 3600 + 600
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=later):
        catalog.set_trigger_suspended("ops.rollup", "hourly", False, author="alice", holder_kind=TASK_HOLDER)
    trigger = _held(catalog)
    assert trigger["suspended-at-ms"] is None
    assert trigger["next-due-at-ms"] == T0 + 7 * 24 * HOUR_MS + HOUR_MS

    catalog.mark_trigger_fired("ops.rollup", "hourly", status="enqueued", holder_kind=TASK_HOLDER)
    assert _held(catalog)["last-fired-status"] == "enqueued"
    with pytest.raises(TriggerNotFound):
        catalog.set_trigger_owner("ops.rollup", "nope", "bob", author="alice", holder_kind=TASK_HOLDER)
    with pytest.raises(ValueError, match="unknown trigger holder kind"):
        catalog.list_triggers("ops.rollup", holder_kind="view")


# --- the pre-shared URL's salt -----------------------------------------------------------


def test_a_signal_trigger_can_be_armed_rotated_and_disarmed_for_url_firing():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    assert _held(catalog).get("signal-salt") is None, "no URL until an owner asks for one"

    catalog.set_trigger_signal_salt("ops.rollup", "on_demand", "salt-1", author="alice")
    trigger = _held(catalog)
    assert trigger["signal-salt"] == "salt-1"
    assert trigger["signal-salt-rotated-by"] == "alice"
    assert trigger["signal-salt-rotated-at-ms"] is not None

    catalog.set_trigger_signal_salt("ops.rollup", "on_demand", "salt-2", author="alice")
    assert _held(catalog)["signal-salt"] == "salt-2"

    catalog.set_trigger_signal_salt("ops.rollup", "on_demand", None, author="alice")
    trigger = _held(catalog)
    assert trigger["signal-salt"] is None and trigger["signal-salt-rotated-at-ms"] is None


def test_only_a_signal_trigger_can_be_fired_by_url():
    catalog = _catalog()
    _task(catalog)
    _schedule(catalog)
    with pytest.raises(ValueError, match="not a signal trigger"):
        catalog.set_trigger_signal_salt("ops.rollup", "hourly", "salt", author="alice")
    with pytest.raises(TriggerNotFound):
        catalog.set_trigger_signal_salt("ops.rollup", "nope", "salt", author="alice")


# --- the tick claim ------------------------------------------------------------------------


def test_claim_advances_past_now_and_refuses_a_second_claim():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)  # due at 11:00

    early = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + 30 * 60 * 1000)
    assert not early.granted

    claim = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + HOUR_MS + 5_000)
    assert claim.granted
    assert claim.previous_due_ms == T0 + HOUR_MS
    assert claim.next_due_ms == T0 + 2 * HOUR_MS
    assert claim.skipped_occurrences == 0
    assert _held(catalog)["next-due-at-ms"] == T0 + 2 * HOUR_MS

    # The same tick again: another loop reading the same scan finds it moved.
    again = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + HOUR_MS + 5_000)
    assert not again.granted


def test_a_missed_slot_fires_once_and_counts_the_rest():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)  # due at 11:00

    # The clock returns at 14:00:10 after an outage: fires once, skips 12:00
    # and 13:00 and 14:00, and is next due at 15:00.
    claim = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + 4 * HOUR_MS + 10_000)
    assert claim.granted
    assert claim.skipped_occurrences == 3
    assert claim.next_due_ms == T0 + 5 * HOUR_MS


def test_release_restores_only_this_claims_advance():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)
    claim = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + HOUR_MS + 5_000)
    catalog.release_schedule_tick("ops.rollup", "hourly", claim)
    assert _held(catalog)["next-due-at-ms"] == T0 + HOUR_MS

    # A later claim moved it on; a stale release must not put the old slot back.
    later = catalog.claim_schedule_tick("ops.rollup", "hourly", T0 + 2 * HOUR_MS + 5_000)
    assert later.granted
    catalog.release_schedule_tick("ops.rollup", "hourly", claim)
    assert _held(catalog)["next-due-at-ms"] == later.next_due_ms


def test_only_a_schedule_can_be_ticked():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    with pytest.raises(ValueError, match="not a schedule"):
        catalog.claim_schedule_tick("ops.rollup", "on_demand", T0)
    with pytest.raises(TriggerNotFound):
        catalog.claim_schedule_tick("ops.rollup", "nope", T0)


# --- fire_signal ---------------------------------------------------------------------------


@contextlib.contextmanager
def _submitting():
    """jobs, the audit log and the alert sink, mocked - fresh for every `with`."""
    with (
        patch.object(trigger_firing, "_post_job", return_value={"execution_id": "x1"}) as post,
        patch.object(trigger_firing, "write_audit_record") as audit,
        patch.object(trigger_firing, "_alert") as alert,
    ):
        yield post, audit, alert


def test_a_signal_fires_the_task_as_the_triggers_identity():
    catalog = _catalog()
    _task(catalog, author="alice")
    _signal(catalog, author="alice")
    catalog.set_trigger_owner("ops.rollup", "on_demand", "olive", author="alice", holder_kind=TASK_HOLDER)

    with _submitting() as (post, _audit, alert):
        outcome = fire_signal(catalog, "ops.rollup", caller="hook-bot")

    assert outcome == {
        "status": "enqueued", "execution_id": "x1", "trigger": "on_demand",
        "task": "ws.ops.rollup", "detail": None, "channel": "bearer",
    }
    payload = post.call_args[0][0]
    # Windowless: no USING.
    assert payload["sql_text"] == "EXECUTE ws.ops.rollup"
    provenance = payload["client_info"]["trigger"]
    assert provenance["holder"] == "ws.ops.rollup"
    assert provenance["holder_kind"] == "task"
    assert provenance["event_kind"] == "signal"
    assert provenance["source_dataset"] is None
    # The caller is the EVENT, recorded as such; the identity is nowhere in the
    # submission - jobs reads it from the trigger.
    assert provenance["fired_by"] == "hook-bot"
    assert "olive" not in str(payload)
    assert _held(catalog)["last-fired-status"] == "enqueued"
    alert.assert_not_called()


def test_a_signal_windowed_over_a_dataset_binds_its_head():
    catalog = _catalog()
    _add_dataset(catalog, "src.a", **{"current-snapshot-id": 500})
    _task(catalog, sql="INSERT INTO ops.out SELECT * FROM src.a WHERE v > :parent_version")
    # No floor: this fires three times in a row on purpose.
    _signal(catalog, window_source="src.a", minimum_interval_seconds=0)

    with _submitting() as (post, _audit, _alert):
        first = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert first["status"] == "enqueued"
    assert post.call_args[0][0]["sql_text"] == (
        "EXECUTE ws.ops.rollup USING 1 AS parent_version, 500 AS current_version"
    )

    # The worker stamps the window on success; the next signal starts there.
    catalog.mark_task_fired("ops.rollup", status="succeeded", window_to=500)
    with _submitting() as (post, _audit, _alert):
        quiet = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert quiet["status"] == "superseded"
    post.assert_not_called()
    assert catalog.get_task("ops.rollup")["last-fired-status"] == "superseded"

    _add_dataset(catalog, "src.a", **{"current-snapshot-id": 720})
    with _submitting() as (post, _audit, _alert):
        fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert "USING 500 AS parent_version, 720 AS current_version" in post.call_args[0][0]["sql_text"]


def test_a_signal_over_an_empty_dataset_is_superseded_not_an_error():
    catalog = _catalog()
    _add_dataset(catalog, "src.a")
    _task(catalog)
    _signal(catalog, window_source="src.a")
    with _submitting() as (post, _audit, alert):
        outcome = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert outcome["status"] == "superseded"
    post.assert_not_called()
    alert.assert_not_called()


def test_a_statement_that_grew_a_window_after_arming_is_refused_at_fire_time():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    catalog.create_task(
        "ops.rollup", sql="INSERT INTO ops.out SELECT * FROM src.a WHERE v > :current_version",
        author="alice", update_if_exists=True,
    )
    with _submitting() as (post, _audit, alert):
        outcome = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert outcome["status"] == "window-unbound"
    assert "OVER <table>" in outcome["detail"]
    post.assert_not_called()
    alert.assert_called_once()
    assert _held(catalog)["last-fired-status"] == "window-unbound"


def test_suspended_and_throttled_signals_are_outcomes_not_errors():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog, minimum_interval_seconds=600)
    with _submitting() as (post, _audit, alert):
        first = fire_signal(catalog, "ops.rollup", caller="hook-bot")
        second = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert first["status"] == "enqueued"
    assert second["status"] == "throttled" and second["execution_id"] is None
    # One real submission, plus a throttled bookkeeping record so this fire is
    # not invisible to run-history.js (see _submit_throttled_record).
    assert post.call_count == 2
    throttled_call = post.call_args_list[-1].args[0]
    assert throttled_call["throttled"] is True
    throttled_trigger = throttled_call["client_info"]["trigger"]
    assert throttled_trigger["trigger_name"] == "on_demand"
    assert throttled_trigger["target_task"]
    # A task-held trigger has no source dataset - see _submit_throttled_record.
    assert "source_dataset" not in throttled_trigger

    catalog.set_trigger_suspended("ops.rollup", "on_demand", True, author="alice", holder_kind=TASK_HOLDER)
    with _submitting() as (post, _audit, alert):
        paused = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert paused["status"] == "suspended"
    post.assert_not_called()
    alert.assert_not_called()


def test_a_signal_names_something_to_fire_or_is_not_found():
    catalog = _catalog()
    with pytest.raises(TaskNotFound):
        fire_signal(catalog, "ops.rollup", caller="hook-bot")
    _task(catalog)
    with pytest.raises(TriggerNotFound, match="has no trigger"):
        fire_signal(catalog, "ops.rollup", caller="hook-bot")
    _schedule(catalog)
    with pytest.raises(TriggerNotFound, match="is a schedule trigger, not a signal"):
        fire_signal(catalog, "ops.rollup", caller="hook-bot")
    # A commit trigger elsewhere is reported as what it is.
    catalog.drop_trigger("ops.rollup", "hourly", author="alice", holder_kind=TASK_HOLDER)
    _add_dataset(catalog, "src.a")
    catalog.create_trigger("src.a", "on_commit", target_task="ops.rollup", kind="task", author="alice")
    with pytest.raises(TriggerNotFound, match="a commit trigger, not a signal"):
        fire_signal(catalog, "ops.rollup", caller="hook-bot")


def test_a_failed_signal_is_recorded_alerted_and_returned():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    with (
        patch.object(trigger_firing, "_post_job", side_effect=RuntimeError("jobs down")),
        patch.object(trigger_firing, "write_audit_record"),
        patch.object(trigger_firing, "_alert") as alert,
    ):
        outcome = fire_signal(catalog, "ops.rollup", caller="hook-bot")
    assert outcome["status"] == "error"
    assert "jobs down" in outcome["detail"]
    alert.assert_called_once()
    assert _held(catalog)["last-fired-status"] == "error"


# --- fire_due_schedules -------------------------------------------------------------------


class _Snapshot:
    def __init__(self, path, data):
        self.reference = type("Ref", (), {"path": path})()
        self._data = data

    def to_dict(self):
        return dict(self._data)


class _Query:
    def __init__(self, rows):
        self._rows = rows
        self.filters = []

    def where(self, filter=None):
        self.filters.append((filter.field_path, filter.op_string, filter.value))
        return self

    def stream(self):
        return list(self._rows)


class _Client:
    """A Firestore client whose collection group is whatever the test says."""

    def __init__(self, rows):
        self.query = _Query(rows)

    def collection_group(self, name):
        assert name == "triggers"
        return self.query


def _due_rows(catalog, task="ops.rollup"):
    trigger = _held(catalog, task)
    return [_Snapshot(f"ws/ops/tasks/{task.split('.', 1)[1]}/triggers/{trigger['name']}", trigger)]


def test_a_due_schedule_fires_once_and_advances():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)
    client = _Client(_due_rows(catalog))

    with _submitting() as (post, audit, alert):
        outcomes = fire_due_schedules(
            client, now_ms=T0 + HOUR_MS + 5_000, catalog_factory=lambda ws: catalog
        )

    assert client.query.filters == [
        ("event-kind", "==", "schedule"),
        ("next-due-at-ms", "<=", T0 + HOUR_MS + 5_000),
    ]
    assert len(outcomes) == 1
    assert outcomes[0]["status"] == "enqueued"
    assert outcomes[0]["workspace"] == "ws"
    assert outcomes[0]["task"] == "ws.ops.rollup"
    assert outcomes[0]["skipped_occurrences"] == 0
    provenance = post.call_args[0][0]["client_info"]["trigger"]
    assert provenance["fired_by"] == "schedule"
    assert provenance["holder_kind"] == "task"
    assert _held(catalog)["next-due-at-ms"] == T0 + 2 * HOUR_MS
    assert _held(catalog)["last-fired-status"] == "enqueued"
    alert.assert_not_called()
    ticked = [c[0][0] for c in audit.call_args_list if c[0][0]["event"] == "schedule.ticked"]
    assert ticked and ticked[0]["due_at_ms"] == T0 + HOUR_MS

    # The same scan handed to a second loop: claimed elsewhere, nothing fired.
    with _submitting() as (post, audit, alert):
        again = fire_due_schedules(
            client, now_ms=T0 + HOUR_MS + 5_000, catalog_factory=lambda ws: catalog
        )
    assert again[0]["status"] == "claimed-elsewhere"
    post.assert_not_called()


def test_a_tick_whose_fire_raises_hands_the_slot_back():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)
    client = _Client(_due_rows(catalog))
    with (
        patch.object(trigger_firing, "_post_job", side_effect=RuntimeError("jobs down")),
        patch.object(trigger_firing, "write_audit_record"),
        patch.object(trigger_firing, "_alert") as alert,
    ):
        outcomes = fire_due_schedules(
            client, now_ms=T0 + HOUR_MS + 5_000, catalog_factory=lambda ws: catalog
        )
    assert outcomes[0]["status"] == "error"
    assert _held(catalog)["next-due-at-ms"] == T0 + HOUR_MS, "the slot was not consumed"
    alert.assert_called_once()


def test_a_suspended_schedule_is_stamped_and_not_advanced():
    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog)
    catalog.set_trigger_suspended("ops.rollup", "hourly", True, author="alice", holder_kind=TASK_HOLDER)
    client = _Client(_due_rows(catalog))
    with _submitting() as (post, _audit, _alert):
        outcomes = fire_due_schedules(
            client, now_ms=T0 + HOUR_MS + 5_000, catalog_factory=lambda ws: catalog
        )
    assert outcomes[0]["status"] == "suspended"
    assert _held(catalog)["next-due-at-ms"] == T0 + HOUR_MS
    assert _held(catalog)["last-fired-status"] == "suspended"
    post.assert_not_called()


def test_the_tick_never_raises_and_one_bad_trigger_does_not_stop_the_rest():
    catalog = _catalog()
    _task(catalog, "ops.good")
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog, task="ops.good", name="hourly")
    misfiled = _Snapshot("ws/ops/datasets/x/triggers/nope", {"event-kind": "schedule"})
    gone = _Snapshot("ws/ops/tasks/vanished/triggers/hourly", {"name": "hourly", "event-kind": "schedule"})
    client = _Client([misfiled, gone, *_due_rows(catalog, "ops.good")])

    with _submitting() as (_post, _audit, alert):
        outcomes = fire_due_schedules(
            client, now_ms=T0 + HOUR_MS + 5_000, catalog_factory=lambda ws: catalog
        )
    statuses = {o["task"]: o["status"] for o in outcomes}
    assert statuses["ws.ops.good"] == "enqueued"
    assert statuses["ws.ops.vanished"] == "error"
    assert len(outcomes) == 2, "the misfiled record is skipped, not fired"
    alert.assert_called_once()

    # A scan that fails fires nothing and says so.
    class _Broken:
        def collection_group(self, name):
            raise RuntimeError("firestore down")

    with _submitting() as (_post, _audit, alert):
        assert fire_due_schedules(_Broken(), now_ms=T0, catalog_factory=lambda ws: catalog) == []
    alert.assert_called_once()


def test_the_kill_switch_stops_the_clock_too(monkeypatch):
    monkeypatch.setenv("OPTERYX_TRIGGER_FIRING", "off")
    catalog = _catalog()

    class _Untouchable:
        def collection_group(self, name):
            raise AssertionError("must not be read while firing is off")

    assert fire_due_schedules(_Untouchable(), now_ms=T0, catalog_factory=lambda ws: catalog) == []


# --- integrity ------------------------------------------------------------------------------


def test_integrity_sees_task_held_triggers():
    from opteryx_catalog.integrity import audit_workspace

    catalog = _catalog()
    _task(catalog)
    with patch("opteryx_catalog.opteryx_catalog.time.time", return_value=T0 / 1000):
        _schedule(catalog, window_source=None)
    client = catalog.firestore_client

    # `list_documents` is what the sweep walks with; the stand-ins stream only.
    def _list_documents(collection):
        return list(collection._docs.values())

    for coll in [client.collection("ws")] + [
        ref.collection("datasets") for ref in client.collection("ws")._docs.values()
    ] + [ref.collection("tasks") for ref in client.collection("ws")._docs.values()]:
        coll.list_documents = lambda c=coll: _list_documents(c)

    two_days_on = T0 + 2 * 24 * HOUR_MS
    with patch("opteryx_catalog.integrity.time.time", return_value=two_days_on / 1000):
        kinds = {f["kind"] for f in audit_workspace(client, "ws")}
    assert "stale-schedule" in kinds

    # Advance it as the clock would, and the finding goes away.
    catalog.claim_schedule_tick("ops.rollup", "hourly", two_days_on)
    with patch("opteryx_catalog.integrity.time.time", return_value=two_days_on / 1000):
        kinds = {f["kind"] for f in audit_workspace(client, "ws")}
    assert "stale-schedule" not in kinds


# --- signed URLs ------------------------------------------------------------------------


def test_a_signing_key_binds_a_signature_to_one_task_and_one_identity():
    from opteryx_catalog.trigger_firing import signal_signature
    from opteryx_catalog.trigger_firing import signal_signature_matches

    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    key = catalog.rotate_signal_token("ops.rollup", author="alice")
    assert len(key) >= 40
    stored = _held(catalog)
    assert stored["signal-token"] == key and stored["signal-token-rotated-at-ms"]

    signature = signal_signature(key, "ws.ops.rollup", "github-actions")
    assert signal_signature_matches(key, "ws.ops.rollup", "github-actions", signature)
    assert signal_signature_matches(key, "ws.ops.rollup", "github-actions", signature.upper())
    # Another task, another identity, or a different key: no.
    assert not signal_signature_matches(key, "ws.ops.other", "github-actions", signature)
    assert not signal_signature_matches(key, "ws.ops.rollup", "someone-else", signature)
    assert not signal_signature_matches("other-key", "ws.ops.rollup", "github-actions", signature)
    assert not signal_signature_matches(None, "ws.ops.rollup", "github-actions", signature)
    assert not signal_signature_matches(key, "ws.ops.rollup", "github-actions", None)
    # HMAC with a separator: the boundary between task and identity matters.
    assert signal_signature(key, "ws.ops.rollup", "ab") != signal_signature(key, "ws.ops.rollupa", "b")


def test_rotating_invalidates_every_url_and_clearing_revokes_them_all():
    from opteryx_catalog.trigger_firing import signal_signature
    from opteryx_catalog.trigger_firing import signal_signature_matches

    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    first = catalog.rotate_signal_token("ops.rollup", author="alice")
    old_url = signal_signature(first, "ws.ops.rollup", "github-actions")
    second = catalog.rotate_signal_token("ops.rollup", author="alice")
    assert second != first
    assert not signal_signature_matches(_held(catalog)["signal-token"], "ws.ops.rollup", "github-actions", old_url)

    catalog.clear_signal_token("ops.rollup", author="alice")
    assert _held(catalog)["signal-token"] is None
    assert not signal_signature_matches(
        _held(catalog)["signal-token"], "ws.ops.rollup", "github-actions",
        signal_signature(second, "ws.ops.rollup", "github-actions"),
    )


def test_only_a_signal_trigger_takes_a_key():
    catalog = _catalog()
    with pytest.raises(TaskNotFound):
        catalog.rotate_signal_token("ops.rollup", author="alice")
    _task(catalog)
    with pytest.raises(TriggerNotFound, match="no trigger"):
        catalog.rotate_signal_token("ops.rollup", author="alice")
    _schedule(catalog)
    with pytest.raises(TriggerNotFound, match="not a signal trigger"):
        catalog.rotate_signal_token("ops.rollup", author="alice")
    with pytest.raises(ValueError, match="author"):
        catalog.rotate_signal_token("ops.rollup")


def test_a_signed_fire_records_its_channel():
    catalog = _catalog()
    _task(catalog)
    _signal(catalog)
    with _submitting() as (post, _audit, _alert):
        outcome = fire_signal(catalog, "ops.rollup", caller="github-actions", channel="signed-url")
    assert outcome["channel"] == "signed-url"
    assert post.call_args[0][0]["client_info"]["trigger"]["fired_by"] == "github-actions"
