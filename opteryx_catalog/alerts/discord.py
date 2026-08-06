"""Pushing alerts to a Discord channel.

The channel that interrupts someone, so it is severity-gated: `min_severity`
defaults to CRITICAL. Stdout keeps the complete record; this carries only the
failures worth waking up for.

It renders its own message rather than posting `Alert.body`. That body is
GitHub-shaped - an HTML comment marker, a `<details>` block, a 20k traceback -
none of which Discord renders, and all of which blows the 2000-character content
limit. The notification carries what identifies the failure; the detail lives in
the log record and the GitHub issue.

Delivery is best-effort, like the GitHub sink: queued, dropped when the queue is
full, lost on a hard crash. The stdout line is the guarantee.
"""

from __future__ import annotations

import logging
import time as _time

import requests

from ..exceptions import AlertSeverity
from . import _secrets
from .sinks import Alert

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = 10  # seconds

# Discord's documented maxima. Exceeding any of them is a 400, which would lose
# the alert entirely, so everything is truncated to fit rather than trusted.
MAX_CONTENT = 2000
MAX_EMBED_TITLE = 256
MAX_EMBED_DESCRIPTION = 4096
MAX_FIELD_NAME = 256
MAX_FIELD_VALUE = 1024
MAX_FIELDS = 10

# Colour is the fastest signal when scanning a channel on a phone.
SEVERITY_COLOURS = {
    AlertSeverity.CRITICAL: 0xD7263D,  # red
    AlertSeverity.ERROR: 0xF46036,  # orange
    AlertSeverity.WARNING: 0xE8C547,  # yellow
}
DEFAULT_COLOUR = 0x8D99AE


def _clip(text, limit: int) -> str:
    text = "" if text is None else str(text)
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)] + "…"


def _allowed_mentions(mention: str) -> dict:
    """Permit exactly the mention we are sending, and nothing else.

    Discord suppresses pings unless the payload opts in. Defaulting to `parse:
    []` means a dataset name that happens to look like a mention cannot ping a
    channel, which is the failure mode of building message text from data.
    """
    if not mention:
        return {"parse": []}
    if mention.startswith("<@&") and mention.endswith(">"):
        return {"parse": [], "roles": [mention[3:-1]]}
    if mention.startswith("<@") and mention.endswith(">"):
        return {"parse": [], "users": [mention[2:-1].lstrip("!")]}
    if mention in ("@here", "@everyone"):
        return {"parse": ["everyone"]}
    return {"parse": []}


class DiscordSink:
    """Posts CRITICAL alerts to a Discord webhook."""

    # Deferred to the worker: a maintenance pass must not wait on Discord.
    synchronous = False

    def __init__(
        self,
        *,
        webhook_url=None,
        webhook_secret="DISCORD_NOTIFICATION_WEBHOOK",
        min_severity=AlertSeverity.CRITICAL,
        mention="",
    ):
        self._webhook_url = webhook_url
        self.webhook_secret = webhook_secret
        self.min_severity = min_severity
        self.mention = (mention or "").strip()

    @property
    def webhook_url(self):
        """Resolved lazily: an explicit URL, else the Secret Manager secret.

        Lazy because constructing the sink happens at import-time config, when a
        Secret Manager round trip would be paid by every consumer whether or not
        they ever alert.
        """
        if self._webhook_url:
            return self._webhook_url
        return _secrets.resolve("OPTERYX_ALERTS_DISCORD_WEBHOOK", self.webhook_secret)

    def _payload(self, alert: Alert) -> dict:
        fields = []
        for key, value in list(alert.context.items())[:MAX_FIELDS]:
            fields.append(
                {
                    "name": _clip(key, MAX_FIELD_NAME),
                    "value": _clip(value, MAX_FIELD_VALUE) or "—",
                    "inline": True,
                }
            )

        # Severity is carried by the embed colour and the content line, so it is
        # not repeated here.
        description = alert.exc_type or ""
        if alert.origin:
            description = f"`{alert.exc_type}` in `{alert.origin}`"

        embed = {
            "title": _clip(alert.title, MAX_EMBED_TITLE),
            "description": _clip(description, MAX_EMBED_DESCRIPTION),
            "color": SEVERITY_COLOURS.get(alert.severity, DEFAULT_COLOUR),
            "footer": {"text": f"{alert.component} / {alert.environment} · {alert.fingerprint}"},
        }
        if fields:
            embed["fields"] = fields

        content = f"{self.mention} **{alert.severity}**".strip() if self.mention else ""
        return {
            "content": _clip(content, MAX_CONTENT),
            "embeds": [embed],
            "allowed_mentions": _allowed_mentions(self.mention),
        }

    def deliver(self, alert: Alert) -> None:
        url = self.webhook_url
        if not url:
            return

        payload = self._payload(alert)
        for attempt in (1, 2):
            try:
                response = requests.post(url, json=payload, timeout=HTTP_TIMEOUT)
            except requests.RequestException as exc:
                logger.warning("alerts: posting to Discord failed: %s", exc)
                raise _NotDelivered(alert.fingerprint) from exc

            if 200 <= response.status_code < 300:
                return

            retryable = response.status_code == 429 or response.status_code >= 500
            if retryable and attempt == 1:
                delay = _retry_after_seconds(response)
                logger.warning(
                    "alerts: Discord returned %s, retrying once in %ss",
                    response.status_code,
                    delay,
                )
                _sleep(delay)
                continue

            logger.warning(
                "alerts: Discord returned %s: %s",
                response.status_code,
                getattr(response, "text", "")[:300],
            )
            raise _NotDelivered(alert.fingerprint)

        raise _NotDelivered(alert.fingerprint)


class _NotDelivered(Exception):
    """The alert did not reach Discord and must not be remembered as delivered."""


def _sleep(seconds: float) -> None:
    _time.sleep(seconds)


def _retry_after_seconds(response) -> float:
    """Seconds to wait, from the JSON body first and the header second.

    Discord puts `retry_after` in the response BODY as a float, as well as
    sending a `Retry-After` header. Reading only the header - which is what the
    GitHub sink does, correctly, for GitHub - would miss the more precise value
    and fall back to a flat default. Clamped so a hostile value can't wedge the
    worker thread.
    """
    for value in (_body_retry_after(response), _header_retry_after(response)):
        if value is not None:
            return max(0.0, min(value, 60.0))
    return 1.0


def _body_retry_after(response):
    try:
        payload = response.json()
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return float(payload["retry_after"])
    except (KeyError, TypeError, ValueError):
        return None


def _header_retry_after(response):
    headers = getattr(response, "headers", None) or {}
    try:
        return float(headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None
