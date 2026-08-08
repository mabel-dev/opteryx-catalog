"""Identity: which occurrences are the same failure, and what the ticket says.

Getting this wrong is expensive in both directions. Too coarse and one ticket
hides several distinct bugs; too fine and one bug files a ticket per dataset,
which is what the excluded exception message would do.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog.alerts import _identity


def _raise_a(message):
    raise ValueError(message)


def _raise_b(message):
    raise ValueError(message)


def _caught(fn, message):
    try:
        fn(message)
    except Exception as exc:  # noqa: BLE001 - capturing whatever the fixture raised
        return exc
    raise AssertionError("expected an exception")


def test_message_does_not_change_identity():
    """The message carries dataset names and ids - the varying part."""
    one = _identity.auto_fingerprint(_caught(_raise_a, "landing.scan_metadata failed"))
    two = _identity.auto_fingerprint(_caught(_raise_a, "landing.http failed"))
    assert one == two


def test_different_call_path_changes_identity():
    one = _identity.auto_fingerprint(_caught(_raise_a, "boom"))
    two = _identity.auto_fingerprint(_caught(_raise_b, "boom"))
    assert one != two


def test_exception_type_is_in_the_material():
    """Two different exception types at one site must not collide."""

    def raise_type_error(message):
        raise TypeError(message)

    value_error = _identity.auto_fingerprint(_caught(_raise_a, "boom"))
    type_error = _identity.auto_fingerprint(_caught(raise_type_error, "boom"))
    assert value_error[0] == "ValueError"
    assert type_error[0] == "TypeError"
    assert value_error != type_error


def test_component_salts_the_hash():
    """The same bug in two services is two tickets - different owners, different fixes."""
    parts = ["ValueError", "a.py:f"]
    assert _identity.fingerprint(parts, "expiration") != _identity.fingerprint(parts, "upload")


def test_string_and_single_element_list_agree():
    assert _identity.fingerprint("solo", "c") == _identity.fingerprint(["solo"], "c")


def test_wrapped_exception_follows_the_root_cause():
    try:
        try:
            raise ValueError("the real problem")
        except ValueError as inner:
            raise RuntimeError("boundary wrapper") from inner
    except RuntimeError as exc:
        outer = exc

    assert _identity.root_cause(outer).args[0] == "the real problem"
    assert _identity.auto_fingerprint(outer)[0] == "ValueError"
    assert "ValueError: the real problem" in _identity.auto_title(outer)

    body = _identity.render_body(
        digest="deadbeef",
        exc=outer,
        note="",
        summary="",
        context={},
        component="c",
        environment="e",
        severity="ERROR",
    )
    assert "Surfaced as `RuntimeError`" in body


def test_title_truncates_but_keeps_the_origin():
    exc = _caught(_raise_a, "x" * 500)
    title = _identity.auto_title(exc)
    assert len(title) <= _identity.MAX_TITLE
    assert title.endswith("test_alerts_identity._raise_a")


def test_constructed_never_raised_fingerprints_from_the_fire_site():
    """Most catalog conditions are detected, not caught - no traceback at all."""
    exc = ValueError("detected, not raised")
    assert exc.__traceback__ is None
    parts = _identity.auto_fingerprint(exc)
    assert parts[0] == "ValueError"
    assert "test_alerts_identity" in parts[1]


def test_no_alerts_package_frame_reaches_the_identity():
    """Guards the split of this package across several modules.

    The frame filter used to compare `basename(__file__)`, which quietly stopped
    working the moment the reporter became more than one file - every alert
    would then have fingerprinted against our own plumbing, silently orphaning
    every open issue. Matching by directory is what makes that safe.
    """
    # The property that matters: EVERY module of this package is recognised as
    # ours, not just the one that happens to define the filter. Asserted
    # directly, because calling auto_fingerprint() from a test puts no alerts
    # frame on the stack - only a real report() does, and by then the path is
    # already hashed and cannot be inspected.
    package_dir = os.path.dirname(os.path.abspath(_identity.__file__))
    for module in ("_identity.py", "_dispatch.py", "sinks.py", "github.py", "__init__.py"):
        assert _identity._is_ours(os.path.join(package_dir, module)), module
    assert not _identity._is_ours(__file__)

    exc = ValueError("detected, not raised")
    path = _identity.auto_fingerprint(exc)[1]
    # Compare frame filenames exactly; a substring check matches this test file's
    # own name. `__init__.py` is deliberately not checked here - pytest has
    # several of its own, so the name alone cannot say whose it is.
    filenames = {frame.split(":", 1)[0] for frame in path.split(">")}
    for module in ("_identity.py", "_dispatch.py", "sinks.py", "github.py"):
        assert module not in filenames, f"{module} leaked into the fingerprint: {path}"

    text = _identity.format_traceback(exc)
    assert "constructed, not raised" in text
    assert os.path.join("alerts", "_identity.py") not in text


def test_unserialisable_context_does_not_lose_the_alert():
    class Awkward:
        def __repr__(self):
            return "<awkward>"

    rendered = _identity.context_block({"obj": Awkward(), "n": 1})
    assert "<awkward>" in rendered
    assert '"n": 1' in rendered


def test_body_carries_the_marker_and_severity():
    body = _identity.render_body(
        digest="abc123",
        exc=_caught(_raise_a, "boom"),
        note="a note",
        summary="a summary",
        context={"dataset": "landing.http"},
        component="expiration",
        environment="production",
        severity="CRITICAL",
    )
    assert body.startswith("<!-- platform-incident: abc123 -->")
    assert "| Severity | `CRITICAL` |" in body
    assert "a summary" in body
    assert "a note" in body
    assert "landing.http" in body


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
