"""The Discord sink.

All blocking, so no thread is involved, and the sleep is patched rather than
slept through. Patching is done where the symbol is used, per the house pattern.
"""

import os
import sys
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import alerts
from opteryx_catalog.alerts import discord as discord_module
from opteryx_catalog.alerts.discord import DiscordSink
from opteryx_catalog.alerts.discord import _allowed_mentions
from opteryx_catalog.alerts.sinks import Alert
from opteryx_catalog.exceptions import AlertSeverity
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import SummaryInconsistencyError

WEBHOOK = "https://discord.com/api/webhooks/123/abc"


def _response(status=204, payload=None, headers=None):
    response = MagicMock()
    response.status_code = status
    if payload is None:
        response.json.side_effect = ValueError("no body")
    else:
        response.json.return_value = payload
    response.text = ""
    response.headers = headers or {}
    return response


def _alert(severity=AlertSeverity.CRITICAL, **kwargs):
    defaults = {
        "fingerprint": "abc1234567890def",
        "severity": severity,
        "title": "[expiration] ManifestProtectionError: unreadable in expiration.get",
        "body": "github-shaped body",
        "labels": ("platform-failure", "expiration"),
        "context": {"dataset": "ichnos.landing.scan_metadata"},
        "component": "expiration",
        "environment": "production",
        "exc_type": "ManifestProtectionError",
        "origin": "expiration._get_file_sizes_in_snapshots",
    }
    defaults.update(kwargs)
    return Alert(**defaults)


# -- configuration ------------------------------------------------------


def test_no_webhook_means_no_http():
    sink = DiscordSink(webhook_url=None, webhook_secret=None)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        sink.deliver(_alert())
        post.assert_not_called()


def test_the_secret_name_defaults_to_the_one_that_exists():
    assert DiscordSink().webhook_secret == "DISCORD_NOTIFICATION_WEBHOOK"


def test_defaults_to_critical_only():
    """Discord interrupts people; stdout keeps the complete record."""
    assert DiscordSink().min_severity == AlertSeverity.CRITICAL


def test_selected_by_name_in_the_sink_list():
    alerts.reset()
    alerts.configure(component="expiration", sink="stdout,discord")
    kinds = [type(s).__name__ for s in alerts._cfg().sinks]
    assert kinds == ["StdoutSink", "DiscordSink"]
    alerts.reset()


def test_both_is_still_stdout_plus_github():
    alerts.reset()
    alerts.configure(component="expiration", repo="a/b", sink="both")
    kinds = [type(s).__name__ for s in alerts._cfg().sinks]
    assert kinds == ["StdoutSink", "GitHubSink"]
    alerts.reset()


def test_an_unusable_sink_list_still_leaves_stdout():
    """A typo must not silence the platform's only self-report."""
    alerts.reset()
    alerts.configure(component="expiration", sink="disocrd")
    kinds = [type(s).__name__ for s in alerts._cfg().sinks]
    assert kinds == ["StdoutSink"]
    alerts.reset()


# -- payload ------------------------------------------------------------


def test_payload_shape():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert())

        payload = post.call_args.kwargs["json"]
        assert set(payload) == {"content", "embeds", "allowed_mentions"}
        embed = payload["embeds"][0]
        assert embed["title"].startswith("[expiration] ")
        assert "ManifestProtectionError" in embed["description"]
        assert "expiration._get_file_sizes_in_snapshots" in embed["description"]
        assert embed["footer"]["text"].endswith("abc1234567890def")


def test_context_becomes_embed_fields():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert(context={"dataset": "landing.http", "expected": 744}))

        fields = post.call_args.kwargs["json"]["embeds"][0]["fields"]
        by_name = {f["name"]: f["value"] for f in fields}
        assert by_name["dataset"] == "landing.http"
        assert by_name["expected"] == "744"


def test_colour_tracks_severity():
    sink = DiscordSink(webhook_url=WEBHOOK, min_severity=AlertSeverity.WARNING)
    for severity in (AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL):
        with patch("opteryx_catalog.alerts.discord.requests.post") as post:
            post.return_value = _response(204)
            sink.deliver(_alert(severity=severity))
            colour = post.call_args.kwargs["json"]["embeds"][0]["color"]
            assert colour == discord_module.SEVERITY_COLOURS[severity]


def test_oversized_values_are_clipped_not_rejected():
    """Exceeding a Discord maximum is a 400, which would lose the alert."""
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(
            _alert(title="T" * 5000, context={"k": "V" * 5000, "long" * 100: "x"})
        )

        embed = post.call_args.kwargs["json"]["embeds"][0]
        assert len(embed["title"]) <= discord_module.MAX_EMBED_TITLE
        for field in embed["fields"]:
            assert len(field["name"]) <= discord_module.MAX_FIELD_NAME
            assert len(field["value"]) <= discord_module.MAX_FIELD_VALUE


def test_at_most_ten_fields():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert(context={f"k{n}": n for n in range(50)}))
        assert len(post.call_args.kwargs["json"]["embeds"][0]["fields"]) <= discord_module.MAX_FIELDS


# -- mentions -----------------------------------------------------------


def test_no_mention_permits_no_pings():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert())
        payload = post.call_args.kwargs["json"]
        assert payload["content"] == ""
        assert payload["allowed_mentions"] == {"parse": []}


def test_role_mention_is_permitted_explicitly():
    sink = DiscordSink(webhook_url=WEBHOOK, mention="<@&987654321>")
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert())
        payload = post.call_args.kwargs["json"]
        assert payload["content"].startswith("<@&987654321>")
        assert payload["allowed_mentions"] == {"parse": [], "roles": ["987654321"]}


def test_data_that_looks_like_a_mention_cannot_ping():
    """Message text is built from data - a dataset name must not page a channel."""
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(204)
        sink.deliver(_alert(context={"dataset": "@everyone <@&111>"}))
        assert post.call_args.kwargs["json"]["allowed_mentions"] == {"parse": []}


@pytest.mark.parametrize(
    "mention,expected",
    [
        ("", {"parse": []}),
        ("<@&42>", {"parse": [], "roles": ["42"]}),
        ("<@99>", {"parse": [], "users": ["99"]}),
        ("<@!99>", {"parse": [], "users": ["99"]}),
        ("@here", {"parse": ["everyone"]}),
        ("@everyone", {"parse": ["everyone"]}),
        ("nonsense", {"parse": []}),
    ],
)
def test_allowed_mentions_table(mention, expected):
    assert _allowed_mentions(mention) == expected


# -- failure handling ---------------------------------------------------


def test_429_reads_retry_after_from_the_body():
    """Discord puts retry_after in the BODY; reading only the header misses it."""
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post, patch(
        "opteryx_catalog.alerts.discord._sleep"
    ) as sleep:
        post.side_effect = [_response(429, {"retry_after": 2.5}), _response(204)]
        sink.deliver(_alert())
        sleep.assert_called_once_with(2.5)
        assert post.call_count == 2


def test_429_falls_back_to_the_header():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post, patch(
        "opteryx_catalog.alerts.discord._sleep"
    ) as sleep:
        post.side_effect = [_response(429, None, {"Retry-After": "4"}), _response(204)]
        sink.deliver(_alert())
        sleep.assert_called_once_with(4.0)


def test_retry_after_is_clamped():
    assert discord_module._retry_after_seconds(_response(429, {"retry_after": 99999})) == 60.0
    assert discord_module._retry_after_seconds(_response(429, {"retry_after": "junk"})) == 1.0
    assert discord_module._retry_after_seconds(_response(429, None)) == 1.0


def test_a_persistent_failure_is_reported_as_not_delivered():
    """So the dispatcher forgets the fingerprint and the next occurrence retries."""
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post, patch(
        "opteryx_catalog.alerts.discord._sleep"
    ):
        post.return_value = _response(500)
        with pytest.raises(discord_module._NotDelivered):
            sink.deliver(_alert())
        assert post.call_count == 2  # one attempt, one retry, then give up


def test_a_4xx_is_not_retried():
    sink = DiscordSink(webhook_url=WEBHOOK)
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.return_value = _response(400)
        with pytest.raises(discord_module._NotDelivered):
            sink.deliver(_alert())
        assert post.call_count == 1


def test_a_failing_discord_does_not_cost_the_stdout_line(capsys):
    """The stdout record is the guarantee; a dead webhook must not take it down."""
    import json

    alerts.reset()
    alerts.configure(component="expiration", sink="stdout,discord")
    with patch("opteryx_catalog.alerts.discord.requests.post") as post:
        post.side_effect = Exception("discord is down")
        alerts.report(ManifestProtectionError("unreadable"), blocking=True)

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    records = [json.loads(line) for line in lines]
    assert len(records) == 1
    assert records[0]["severity"] == "CRITICAL"
    alerts.reset()


def test_warnings_never_reach_discord():
    alerts.reset()
    alerts.configure(component="expiration", sink="discord")
    with patch("opteryx_catalog.alerts.discord.requests.post") as post, patch.dict(
        os.environ, {"OPTERYX_ALERTS_DISCORD_WEBHOOK": WEBHOOK}
    ):
        alerts.report(SummaryInconsistencyError("totals disagree"), blocking=True)
        post.assert_not_called()

        alerts.report(ManifestProtectionError("unreadable"), blocking=True)
        assert post.call_count == 1
    alerts.reset()


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
