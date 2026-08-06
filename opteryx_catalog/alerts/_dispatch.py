"""Deduplication, the queue, and the worker that drains it.

Cooloff is applied HERE, before any sink sees the alert, so every sink agrees on
identity and none of them floods. The hourly create cap lives in `GitHubSink`
instead, because it exists to protect the GitHub API rather than to suppress
alerts - stdout has no such limit and needs none.

Alerts are deduplicated; logs are not. Fire sites keep their existing
`logger.*` call and add a report, so the log stream still answers "how often,
and to which datasets" while the alert stream answers "is this happening".
"""

from __future__ import annotations

import logging
import queue
import threading
import time

logger = logging.getLogger(__name__)

QUEUE_SIZE = 100  # reports buffered before new ones are dropped
MAX_SEEN = 2000  # hard cap on the dedupe table

# fingerprint -> last time we delivered it
_seen: dict = {}
_state_lock = threading.Lock()

_worker = None
_worker_lock = threading.Lock()
_pending: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
_atexit_registered = False


def _now() -> float:
    """Indirection so tests can drive the clock without sleeping."""
    return time.time()


def reset() -> None:
    """Clear every piece of module state. For tests."""
    with _state_lock:
        _seen.clear()


def _expire(now: float, cooloff_seconds: float) -> None:
    """Drop entries that can no longer suppress anything.

    The table used to grow forever, keyed on distinct fingerprints - a slow leak
    in a long-lived worker. An entry past twice the cooloff cannot suppress a
    delivery, so dropping it changes no behaviour.
    """
    horizon = max(cooloff_seconds * 2, 1.0)
    for key in [key for key, seen_at in _seen.items() if now - seen_at > horizon]:
        _seen.pop(key, None)


def _cap() -> None:
    """Backstop for a pathological spread of distinct fingerprints.

    Runs AFTER the insert, so `MAX_SEEN` is the size the table actually settles
    at rather than one more than that. Oldest first: the newest entries are the
    ones still inside their cooloff and therefore still doing work.
    """
    if len(_seen) > MAX_SEEN:
        for key in sorted(_seen, key=_seen.get)[: len(_seen) - MAX_SEEN]:
            _seen.pop(key, None)


def _claim(fingerprint: str, cooloff_seconds: float) -> bool:
    """Whether this alert should be delivered now.

    Claims the fingerprint before returning True, so concurrent reports of the
    same failure - the normal case once a platform failure starts firing on
    every request - don't race each other into duplicate deliveries.
    """
    now = _now()
    with _state_lock:
        _expire(now, cooloff_seconds)
        last = _seen.get(fingerprint)
        if last is not None and now - last < cooloff_seconds:
            return False
        _seen[fingerprint] = now
        _cap()
        return True


def forget(fingerprint: str) -> None:
    """Drop a fingerprint so the next occurrence is delivered again.

    Used when a sink could not deliver: being deduped against a ticket that was
    never created would silence the failure entirely.
    """
    with _state_lock:
        _seen.pop(fingerprint, None)


def _deliver_to(alert, sinks) -> bool:
    """Hand one alert to each sink. Returns whether any accepted it.

    A sink that declines on severity counts as having handled the alert: it was
    routed correctly, to nowhere. Treating it as a failure would make the
    dispatcher forget the fingerprint and re-deliver on the next occurrence,
    turning a quiet channel into the noisiest thing in the system.
    """
    from .sinks import accepts

    delivered_any = False
    for sink in sinks:
        if not accepts(sink, alert):
            delivered_any = True
            continue
        try:
            sink.deliver(alert)
            delivered_any = True
        except Exception as exc:
            # One sink failing must not stop the others - the stdout line is the
            # guarantee and it is cheap, so it should survive GitHub being down.
            if type(exc).__name__ == "_NotFiled":
                logger.warning("alerts: %s did not file %s", type(sink).__name__, alert.fingerprint)
            else:
                logger.warning("alerts: sink %s failed: %s", type(sink).__name__, exc)
    return delivered_any


def submit(alert, sinks, cooloff_seconds: float, blocking: bool) -> None:
    """Deduplicate once, then split delivery by how expensive each sink is.

    Sinks that write locally deliver INLINE, so the record exists before the
    caller continues and survives the process being killed a moment later - an
    alert queued behind a network call and then lost to an OOM kill is worthless
    precisely when it matters most. Only sinks that talk to something remote are
    deferred to the worker, so a maintenance pass never waits on GitHub.

    Cooloff is claimed here, once, before either group - so both see the same
    identity and a suppressed alert costs nothing anywhere.
    """
    if not _claim(alert.fingerprint, cooloff_seconds):
        return

    # Default to synchronous for an unrecognised sink: delivering inline is the
    # predictable failure (a slow caller), deferring is the silent one (a lost
    # alert). A sink opts into the queue explicitly.
    immediate = [sink for sink in sinks if getattr(sink, "synchronous", True)]
    deferred = [sink for sink in sinks if not getattr(sink, "synchronous", True)]

    delivered_any = _deliver_to(alert, immediate)

    if not deferred:
        if not delivered_any:
            forget(alert.fingerprint)
        return

    if blocking:
        delivered_any = _deliver_to(alert, deferred) or delivered_any
        if not delivered_any:
            forget(alert.fingerprint)
        return

    _ensure_worker()
    try:
        _pending.put_nowait((alert, deferred))
    except queue.Full:
        logger.warning("alerts: queue full, dropping deferred delivery of '%s'", alert.title)


def _ensure_worker() -> None:
    """Start the single background worker on first use.

    One thread, not one per report: a failure that fires on every request would
    otherwise spawn threads faster than they finish. The atexit hook is
    registered here rather than at import, so merely importing the package does
    not add a five-second flush to every consumer's shutdown.
    """
    global _worker, _atexit_registered
    with _worker_lock:
        if not _atexit_registered:
            import atexit

            atexit.register(_flush_at_exit)
            _atexit_registered = True
        if _worker is not None and _worker.is_alive():
            return
        _worker = threading.Thread(target=_drain, name="opteryx-catalog-alerts", daemon=True)
        _worker.start()


def _drain() -> None:
    while True:
        item = _pending.get()
        try:
            if item is None:
                return
            alert, sinks = item
            if not _deliver_to(alert, sinks):
                # Nothing accepted it - forget the fingerprint so the next
                # occurrence is delivered rather than deduped against a ticket
                # that was never created.
                forget(alert.fingerprint)
        except Exception as exc:  # a reporter must never take the process down
            logger.warning("alerts: reporting failed: %s", exc)
        finally:
            _pending.task_done()


def flush(timeout: float = 10.0) -> None:
    """Block until queued reports have been delivered, or `timeout` elapses.

    Worth calling at the end of a batch job, whose process would otherwise exit
    before the daemon worker reaches the queue. Polls rather than using
    `queue.join()` so it can time out instead of hanging.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _pending.unfinished_tasks == 0:
            return
        time.sleep(0.05)


def _flush_at_exit() -> None:
    try:
        flush(timeout=5.0)
    except Exception:
        pass
