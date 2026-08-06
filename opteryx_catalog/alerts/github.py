"""Filing alerts as GitHub issues.

An addition to the stdout sink, never a replacement, and it makes weaker
promises: a queued report dies with the process on a hard crash (OOM kill,
SIGKILL) because `atexit` never runs, and reports are dropped by design when the
queue is full or the hourly create cap is reached. That is acceptable only
because the stdout line has already landed by then. Do not treat the queue as
durable.

Listing is used rather than the search API deliberately: search is eventually
consistent, and during the first minute of an incident that inconsistency is
exactly how you end up with forty identical tickets.

Closed issues are deliberately not matched. If a failure was fixed, closed, and
has come back, that is new information and deserves its own ticket - so a
recurrence after a close opens a fresh issue rather than commenting on a dead
one.
"""

from __future__ import annotations

import logging
import threading
import time as _time

import requests

from . import _secrets
from ._identity import context_block
from ._identity import marker
from .sinks import Alert

logger = logging.getLogger(__name__)

# Every issue this sink opens carries this label, and the dedupe listing filters
# on it. Changing it orphans every existing open issue.
TRACKING_LABEL = "platform-failure"

HTTP_TIMEOUT = 15  # seconds
MAX_ISSUES_PER_HOUR = 20  # backstop against a storm opening hundreds of tickets
LIST_PAGES = 3  # pages of open issues searched for a duplicate


def normalize_repo(value):
    """Accept 'owner/repo', or any GitHub URL pointing at one, and return the slug."""
    if not value:
        return None
    value = str(value).strip().rstrip("/")
    if "github.com" in value:
        # https://github.com/mabel-dev/opteryx.app/issues -> mabel-dev/opteryx.app
        tail = value.split("github.com", 1)[1].lstrip(":/")
        parts = [p for p in tail.split("/") if p]
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
        return None
    parts = [p for p in value.split("/") if p]
    if len(parts) == 2:
        return f"{parts[0]}/{parts[1]}"
    return None


# --------------------------------------------------------------------------
# credentials
# --------------------------------------------------------------------------


def reset_token_cache() -> None:
    """Forget every cached credential lookup.

    Kept as a name because `configure()` and `reset()` call it; the cache itself
    moved to `_secrets` once a second sink needed the same env-then-Secret-
    Manager resolution with the same sticky negative result.
    """
    _secrets.reset_cache()


def _token(token_secret: str):
    """The GitHub token: environment first (dev), then Secret Manager (deployed)."""
    return _secrets.resolve("GITHUB_TOKEN", token_secret)


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "opteryx-catalog-alerts",
    }


class GitHubSink:
    """Files, or folds into, one GitHub issue per distinct failure."""

    # Deferred to the worker: up to four HTTP round trips with a 15s timeout
    # each, which no maintenance pass should wait on.
    synchronous = False

    def __init__(self, *, repo, api_url="https://api.github.com", token_secret="GITHUB_TOKEN"):
        self.repo = normalize_repo(repo)
        self.api_url = (api_url or "https://api.github.com").rstrip("/")
        self.token_secret = token_secret or "GITHUB_TOKEN"
        self._lock = threading.Lock()
        self._created_at: list = []  # timestamps of issues opened, for the hourly cap

    # -- rate cap -------------------------------------------------------

    def _rate_limited(self, now: float) -> bool:
        """Sliding one-hour window over issues CREATED.

        Comments and dedupe hits are free: the cap protects the GitHub API from
        a storm opening hundreds of tickets, not from talking to it at all.
        """
        with self._lock:
            while self._created_at and now - self._created_at[0] > 3600:
                self._created_at.pop(0)
            return len(self._created_at) >= MAX_ISSUES_PER_HOUR

    # -- HTTP -----------------------------------------------------------

    def _find_open_issue(self, token: str, digest: str):
        """The number of the OPEN issue carrying this fingerprint, if any."""
        wanted = marker(digest)
        for page in range(1, LIST_PAGES + 1):
            try:
                response = requests.get(
                    f"{self.api_url}/repos/{self.repo}/issues",
                    headers=_headers(token),
                    params={
                        "state": "open",
                        "labels": TRACKING_LABEL,
                        "per_page": 100,
                        "page": page,
                    },
                    timeout=HTTP_TIMEOUT,
                )
            except requests.RequestException as exc:
                logger.warning("alerts: listing issues failed: %s", exc)
                return None
            if response.status_code != 200:
                logger.warning(
                    "alerts: listing issues returned %s: %s",
                    response.status_code,
                    response.text[:200],
                )
                return None
            try:
                issues = response.json()
            except ValueError:
                return None
            if not issues:
                return None
            for issue in issues:
                if "pull_request" in issue:
                    continue
                if wanted in (issue.get("body") or ""):
                    return issue.get("number")
            if len(issues) < 100:
                return None
        return None

    def _create_issue(self, token: str, alert: Alert):
        """Open the issue. Labels that don't exist yet are created by GitHub for us.

        Returns the issue number, or None. A 429 or 5xx is retried exactly once
        after honouring `Retry-After`; beyond that the caller forgets the
        fingerprint so the next occurrence tries again, which is a better
        backoff than a tight retry loop on a path that only runs when something
        is already broken.
        """
        payload = {"title": alert.title, "body": alert.body, "labels": list(alert.labels)}
        for attempt in (1, 2):
            try:
                response = requests.post(
                    f"{self.api_url}/repos/{self.repo}/issues",
                    headers=_headers(token),
                    json=payload,
                    timeout=HTTP_TIMEOUT,
                )
            except requests.RequestException as exc:
                logger.warning("alerts: creating issue failed: %s", exc)
                return None
            if response.status_code in (200, 201):
                try:
                    return response.json().get("number")
                except ValueError:
                    return None
            retryable = response.status_code == 429 or response.status_code >= 500
            if retryable and attempt == 1:
                delay = _retry_after_seconds(response)
                logger.warning(
                    "alerts: creating issue returned %s, retrying once in %ss",
                    response.status_code,
                    delay,
                )
                _sleep(delay)
                continue
            logger.warning(
                "alerts: creating issue returned %s: %s",
                response.status_code,
                response.text[:300],
            )
            return None
        return None

    def _comment(self, token: str, number: int, body: str) -> None:
        try:
            response = requests.post(
                f"{self.api_url}/repos/{self.repo}/issues/{number}/comments",
                headers=_headers(token),
                json={"body": body},
                timeout=HTTP_TIMEOUT,
            )
            if response.status_code not in (200, 201):
                logger.warning(
                    "alerts: commenting on #%s returned %s", number, response.status_code
                )
        except requests.RequestException as exc:
            logger.warning("alerts: commenting on #%s failed: %s", number, exc)

    # -- Sink -----------------------------------------------------------

    def deliver(self, alert: Alert) -> None:
        if not self.repo:
            return
        token = _token(self.token_secret)
        if not token:
            return

        existing = self._find_open_issue(token, alert.fingerprint)
        if existing:
            comment = (
                f"Still happening as of {_utc_stamp()} on "
                f"`{alert.component}` / `{alert.environment}` (severity `{alert.severity}`)."
            )
            if alert.context:
                comment += f"\n\n```json\n{context_block(alert.context)}\n```"
            self._comment(token, existing, comment)
            return

        if self._rate_limited(_now()):
            logger.warning(
                "alerts: hourly cap reached, not filing '%s' (fingerprint %s)",
                alert.title,
                alert.fingerprint,
            )
            raise _NotFiled(alert.fingerprint)

        number = self._create_issue(token, alert)
        if number:
            with self._lock:
                self._created_at.append(_now())
            logger.info("alerts: filed %s#%s - %s", self.repo, number, alert.title)
        else:
            # Filing failed. Tell the dispatcher, so it forgets the fingerprint
            # and the next occurrence tries again rather than being deduped
            # against an issue that was never created.
            raise _NotFiled(alert.fingerprint)


class _NotFiled(Exception):
    """Raised by GitHubSink when an alert was not filed and should not be remembered."""


# Indirections so tests can drive the clock and skip the sleep.


def _now() -> float:
    return _time.time()


def _sleep(seconds: float) -> None:
    _time.sleep(seconds)


def _utc_stamp() -> str:
    return _time.strftime("%Y-%m-%d %H:%M:%SZ", _time.gmtime())


def _retry_after_seconds(response) -> float:
    """`Retry-After` in seconds, clamped so a hostile value can't wedge the worker."""
    raw = (response.headers or {}).get("Retry-After") if hasattr(response, "headers") else None
    try:
        return max(0.0, min(float(raw), 60.0))
    except (TypeError, ValueError):
        return 1.0
