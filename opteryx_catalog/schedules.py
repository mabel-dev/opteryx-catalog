"""Cron schedules for clock-fired triggers.

A schedule trigger's event is a cron expression evaluated in a named time zone.
Everything that turns the expression into instants lives here, so the catalog
(which validates and stamps `next-due-at-ms` at creation) and the firing path
(which advances it when a tick is claimed) cannot drift on what "next" means.

Five-field expressions only - minute, hour, day-of-month, month, day-of-week.
croniter also accepts a sixth seconds field; refused, because a trigger that
fires more than once a minute is a trigger the once-a-minute clock in
dispatch.opteryx cannot honour, and storing one would be storing a promise the
platform cannot keep.

Evaluation is in the trigger's own zone, on aware datetimes, so a daily `0 9 *
* *` in `Europe/London` fires at nine local time on both sides of a DST change
rather than drifting an hour twice a year. `UTC` when none is given.
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

DEFAULT_TIME_ZONE = "UTC"

# `occurrences_between` walks the expression forward one firing at a time. A
# trigger that has been due for a very long time (a service down for a week on
# a per-minute schedule) is reported as "at least this many", not walked to the
# end: the number is for the audit record, and ten thousand already says
# "outage" as clearly as the exact figure would.
MAX_OCCURRENCES_COUNTED = 10_000


def _croniter():
    try:
        from croniter import croniter
    except ImportError as exc:  # pragma: no cover - dependency declared in pyproject
        raise RuntimeError(
            "schedule triggers require the `croniter` package, which is a declared "
            "dependency of opteryx-catalog; the environment is incomplete"
        ) from exc
    return croniter


def validate_schedule(schedule: str | None, time_zone: str | None = None) -> tuple[str, str]:
    """The normalized `(expression, time_zone)` pair, or a ValueError saying why not.

    Normalized so the stored form is canonical: runs of whitespace collapse to
    one space, and a missing zone becomes `UTC` explicitly rather than staying
    None and meaning UTC by convention nobody wrote down.
    """
    if not isinstance(schedule, str) or not schedule.strip():
        raise ValueError("a schedule trigger requires a cron expression")
    expression = " ".join(schedule.split())
    if len(expression.split(" ")) != 5:
        raise ValueError(
            "a schedule is a five-field cron expression - minute, hour, day-of-month, "
            f"month, day-of-week - not {schedule!r}"
        )
    if not _croniter().is_valid(expression):
        raise ValueError(f"not a valid cron expression: {schedule!r}")

    zone = time_zone or DEFAULT_TIME_ZONE
    try:
        ZoneInfo(zone)
    except (ZoneInfoNotFoundError, ValueError, TypeError) as exc:
        raise ValueError(f"unknown time zone: {time_zone!r}; use an IANA name such as 'Europe/London'") from exc
    return expression, zone


def next_due_ms(schedule: str, time_zone: str | None, after_ms: int) -> int:
    """The first firing instant strictly after `after_ms`, as epoch milliseconds.

    Strictly after, so a claim taken exactly on a boundary advances past it:
    a tick at 10:00:00.000 on an hourly schedule is due at 11:00, not 10:00
    again.
    """
    expression, zone = validate_schedule(schedule, time_zone)
    start = datetime.fromtimestamp(int(after_ms) / 1000, tz=ZoneInfo(zone))
    following = _croniter()(expression, start).get_next(datetime)
    return int(following.timestamp() * 1000)


def occurrences_between(schedule: str, time_zone: str | None, start_ms: int, end_ms: int) -> int:
    """How many firing instants fall in `(start_ms, end_ms]`, capped.

    The claim uses it to report how many slots an overdue trigger skipped: the
    slot at `start_ms` is the one being fired, so it is excluded, and anything
    the clock should have reached since is counted.
    """
    if end_ms <= start_ms:
        return 0
    expression, zone = validate_schedule(schedule, time_zone)
    walker = _croniter()(expression, datetime.fromtimestamp(int(start_ms) / 1000, tz=ZoneInfo(zone)))
    count = 0
    while count < MAX_OCCURRENCES_COUNTED:
        following = int(walker.get_next(datetime).timestamp() * 1000)
        if following > end_ms:
            break
        count += 1
    return count
