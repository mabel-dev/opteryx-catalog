"""The GitHub sink.

All blocking, so no thread is involved, and the clock is driven rather than
slept through. Patching is done where the symbol is used, per the house pattern.
"""

import os
import sys
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import alerts
from opteryx_catalog.alerts import github as github_module
from opteryx_catalog.alerts.github import GitHubSink
from opteryx_catalog.alerts.github import normalize_repo
from opteryx_catalog.exceptions import ManifestProtectionError


def _response(status=200, payload=None, headers=None):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = payload if payload is not None else {}
    response.text = ""
    response.headers = headers or {}
    return response


def _setup(sink_mode="github", **overrides):
    alerts.reset()
    os.environ["GITHUB_TOKEN"] = "test-token"
    alerts.configure(
        component="expiration",
        environment="production",
        repo="mabel-dev/opteryx.app",
        sink=sink_mode,
        **overrides,
    )


def _teardown():
    os.environ.pop("GITHUB_TOKEN", None)
    alerts.reset()


def _report(**kwargs):
    alerts.report(ManifestProtectionError("unreadable"), blocking=True, **kwargs)


def test_no_http_at_all_without_a_repo():
    alerts.reset()
    alerts.configure(component="expiration", repo=None, sink="github")
    with patch("opteryx_catalog.alerts.github.requests.post") as post, patch(
        "opteryx_catalog.alerts.github.requests.get"
    ) as get:
        _report()
        post.assert_not_called()
        get.assert_not_called()
    alerts.reset()


def test_create_payload_and_labels():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [])
            post.return_value = _response(201, {"number": 7})
            _report(fingerprint=("gc-unprotectable", "landing.http"))

            payload = post.call_args.kwargs["json"]
            assert set(payload) == {"title", "body", "labels"}
            assert payload["title"].startswith("[expiration] ")
            assert "platform-failure" in payload["labels"]
            assert "expiration" in payload["labels"]
            assert "production" in payload["labels"]
            assert "severity:critical" in payload["labels"]
            assert "data-loss-risk" in payload["labels"]
    finally:
        _teardown()


def test_body_carries_the_fingerprint_marker():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [])
            post.return_value = _response(201, {"number": 7})
            _report()
            body = post.call_args.kwargs["json"]["body"]
            assert body.startswith("<!-- platform-incident: ")
    finally:
        _teardown()


def test_existing_open_issue_is_commented_not_recreated():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            post.return_value = _response(201, {"number": 7})

            # First call files; capture the marker it used.
            get.return_value = _response(200, [])
            _report(fingerprint=("gc-unprotectable", "landing.http"))
            marker = post.call_args.kwargs["json"]["body"].splitlines()[0]

            # Second occurrence, past cooloff, finds the open issue.
            alerts.configure(
                component="expiration",
                environment="production",
                repo="mabel-dev/opteryx.app",
                sink="github",
                cooloff_seconds=0,
            )
            get.return_value = _response(200, [{"number": 7, "body": marker}])
            post.reset_mock()
            _report(fingerprint=("gc-unprotectable", "landing.http"))

            assert post.call_count == 1
            assert post.call_args.args[0].endswith("/issues/7/comments")
            assert "Still happening" in post.call_args.kwargs["json"]["body"]
    finally:
        _teardown()


def test_a_closed_issue_is_not_matched():
    """A failure that was fixed, closed, and came back deserves its own ticket."""
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [])
            post.return_value = _response(201, {"number": 9})
            _report()
            # The listing filters state=open, so a closed issue never reaches us.
            assert get.call_args.kwargs["params"]["state"] == "open"
            assert post.call_args.args[0].endswith("/issues")
    finally:
        _teardown()


def test_pull_requests_in_the_listing_are_skipped():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            post.return_value = _response(201, {"number": 11})
            get.return_value = _response(200, [])
            _report()
            marker = post.call_args.kwargs["json"]["body"].splitlines()[0]

            alerts.configure(
                component="expiration",
                environment="production",
                repo="mabel-dev/opteryx.app",
                sink="github",
                cooloff_seconds=0,
            )
            # A PR carrying the same marker must not be treated as the issue.
            get.return_value = _response(
                200, [{"number": 3, "body": marker, "pull_request": {}}]
            )
            post.reset_mock()
            _report()
            assert post.call_args.args[0].endswith("/issues")
    finally:
        _teardown()


def test_listing_stops_paging_when_a_page_is_short():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [{"number": 1, "body": "unrelated"}])
            post.return_value = _response(201, {"number": 12})
            _report()
            assert get.call_count == 1
    finally:
        _teardown()


def test_listing_pages_through_full_pages():
    _setup()
    try:
        full_page = [{"number": n, "body": "unrelated"} for n in range(100)]
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, full_page)
            post.return_value = _response(201, {"number": 13})
            _report()
            assert get.call_count == github_module.LIST_PAGES
    finally:
        _teardown()


def test_hourly_cap_drops_further_creates():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [])
            post.return_value = _response(201, {"number": 1})
            for n in range(github_module.MAX_ISSUES_PER_HOUR):
                _report(fingerprint=("cap", str(n)))
            assert post.call_count == github_module.MAX_ISSUES_PER_HOUR

            post.reset_mock()
            _report(fingerprint=("cap", "over-the-line"))
            post.assert_not_called()
    finally:
        _teardown()


def test_hourly_cap_releases_once_the_window_moves():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post, patch("opteryx_catalog.alerts.github._now") as now:
            get.return_value = _response(200, [])
            post.return_value = _response(201, {"number": 1})
            now.return_value = 1000.0
            for n in range(github_module.MAX_ISSUES_PER_HOUR):
                _report(fingerprint=("cap", str(n)))

            post.reset_mock()
            now.return_value = 1000.0 + 3601
            _report(fingerprint=("cap", "after-the-window"))
            assert post.call_count == 1
    finally:
        _teardown()


def test_a_failed_create_is_retried_next_occurrence():
    """Being deduped against an issue that was never created silences the failure."""
    _setup(cooloff_seconds=3600)
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post:
            get.return_value = _response(200, [])
            post.return_value = _response(500, {})
            _report(fingerprint=("retry", "landing.http"))

            post.reset_mock()
            post.return_value = _response(201, {"number": 21})
            _report(fingerprint=("retry", "landing.http"))
            assert post.call_count >= 1
    finally:
        _teardown()


def test_429_backs_off_once_then_gives_up():
    _setup()
    try:
        with patch("opteryx_catalog.alerts.github.requests.get") as get, patch(
            "opteryx_catalog.alerts.github.requests.post"
        ) as post, patch("opteryx_catalog.alerts.github._sleep") as sleep:
            get.return_value = _response(200, [])
            post.return_value = _response(429, {}, headers={"Retry-After": "3"})
            _report()

            assert post.call_count == 2  # one attempt, one retry, then stop
            sleep.assert_called_once_with(3.0)
    finally:
        _teardown()


def test_retry_after_is_clamped():
    assert github_module._retry_after_seconds(_response(429, headers={"Retry-After": "99999"})) == 60.0
    assert github_module._retry_after_seconds(_response(429, headers={"Retry-After": "junk"})) == 1.0
    assert github_module._retry_after_seconds(_response(429)) == 1.0


def test_dedupe_table_is_bounded():
    """It used to grow forever, keyed on distinct fingerprints."""
    from opteryx_catalog.alerts import _dispatch

    alerts.reset()
    alerts.configure(component="expiration", sink=alerts.ListSink())
    for n in range(3000):
        _report(fingerprint=("bound", str(n)))
    assert len(_dispatch._seen) <= _dispatch.MAX_SEEN
    alerts.reset()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("mabel-dev/opteryx.app", "mabel-dev/opteryx.app"),
        ("https://github.com/mabel-dev/opteryx.app", "mabel-dev/opteryx.app"),
        ("https://github.com/mabel-dev/opteryx.app/issues", "mabel-dev/opteryx.app"),
        ("git@github.com:mabel-dev/opteryx.app", "mabel-dev/opteryx.app"),
        ("not-a-repo", None),
        ("a/b/c/d", None),
        ("", None),
        (None, None),
    ],
)
def test_normalize_repo(value, expected):
    assert normalize_repo(value) == expected


def test_sink_is_inert_without_a_repo_object():
    sink = GitHubSink(repo=None)
    with patch("opteryx_catalog.alerts.github.requests.post") as post:
        sink.deliver(MagicMock())
        post.assert_not_called()


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
