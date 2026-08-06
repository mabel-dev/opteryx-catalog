"""Fire one real alert through the configured sinks, to prove delivery works.

    OPTERYX_ALERTS_DISCORD_WEBHOOK='https://discord.com/api/webhooks/...' \
        python3 scripts/send_test_alert.py

The webhook URL is read from the environment and never printed. Where the
runtime can reach Secret Manager, omit it and the sink resolves
DISCORD_NOTIFICATION_WEBHOOK itself - which is the path worth testing on the
deployed service.

Sends CRITICAL, because that is what the Discord sink is gated to. No mention is
set unless OPTERYX_ALERTS_DISCORD_MENTION is exported, so by default this posts
to the channel without paging anyone.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from opteryx_catalog import alerts
from opteryx_catalog.exceptions import AlertSeverity
from opteryx_catalog.exceptions import CompactionInvariantError

os.environ.setdefault("OPTERYX_ALERTS_SINK", "stdout,discord")
os.environ.setdefault("OPTERYX_ALERTS_COMPONENT", "alerting-smoke-test")
os.environ.setdefault("OPTERYX_ALERTS_ENVIRONMENT", os.environ.get("ENVIRONMENT", "local"))

alerts.reset()
alerts.configure()

sinks = [type(s).__name__ for s in alerts._cfg()._build_sinks()]
print(f"sinks: {', '.join(sinks)}")
if "DiscordSink" not in sinks:
    print("!! DiscordSink not selected - set OPTERYX_ALERTS_SINK=stdout,discord")
    raise SystemExit(1)

discord_sink = next(s for s in alerts._cfg().sinks if type(s).__name__ == "DiscordSink")
if not discord_sink.webhook_url:
    print(
        "!! no webhook resolved. Either export OPTERYX_ALERTS_DISCORD_WEBHOOK, or run\n"
        "   somewhere that can read the "
        f"'{discord_sink.webhook_secret}' secret from Secret Manager."
    )
    raise SystemExit(1)
print("webhook: resolved")

exc = CompactionInvariantError(
    "TEST ALERT - alerting smoke test, no action needed. "
    "Simulated row-count mismatch: 744 input rows vs 5 written."
)

alerts.report(
    exc,
    severity=AlertSeverity.CRITICAL,
    fingerprint=("alerting-smoke-test", "no-such-dataset"),
    note="Delivery test for the new Discord sink. Safe to ignore and close.",
    context={
        "dataset": "no-such-dataset (this is a test)",
        "expected": 744,
        "actual": 5,
    },
    blocking=True,
)

print("delivered - check the channel")
print("(a second run inside the cooloff window is suppressed by design;")
print(" pass OPTERYX_ALERTS_COOLOFF_HOURS=0 to send again immediately)")
