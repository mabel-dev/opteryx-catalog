"""Turn a platform inconsistency into an alert a human will see.

    from opteryx_catalog import alerts

    except ManifestProtectionError as exc:
        logger.error("Skipping expiration of %s: %s", identifier, exc)
        alerts.report(exc, fingerprint=("gc-unprotectable", identifier),
                      context={"dataset": identifier})
        continue

You hand it the exception; it emits a structured record describing the failure
and folds later occurrences of the same failure into the same identity rather
than repeating.

This is for failures the caller cannot do anything about - the ones where the
system itself is wrong. Reporting a bad request would bury the real failures, so
call it from paths where the exception means something is broken, not from paths
where it means a caller sent something invalid. `exceptions.Alertable` marks
which exceptions qualify.

WHERE TO CALL IT: the library alerts exactly where it stops propagating.
Never at `raise` - an exception gets constructed, discarded, wrapped and
re-raised, and firing there would do IO on construction and alert on failures
the caller correctly handles. Call it where an alertable exception is ABSORBED,
or where a condition is detected and deliberately not raised.

DELIVERY: the stdout line is the guarantee - it is written synchronously, before
the process can die. GitHub delivery is an addition and is best-effort; see
`github.py`.

Configuration is environment variables. The `OPTERYX_ALERTS_*` names are
preferred; the legacy `PLATFORM_ISSUES_*` names are still read, because the
deployed service configuration lives outside the repo and a hard rename would
silently mute reporting with nothing in the repo for review to catch.

    OPTERYX_ALERTS_SINK           Comma-separated: 'stdout' (default), 'github',
                                  'discord'. e.g. 'stdout,discord'. 'both' is
                                  kept as an alias for 'stdout,github'.
    OPTERYX_ALERTS_ENABLED        'false' to silence alerting entirely.
    OPTERYX_ALERTS_REPO           'owner/repo' (a GitHub URL is also accepted).
                                  Required for the github sink; without it that
                                  sink is skipped and stdout still works.
    OPTERYX_ALERTS_COMPONENT      Which job is reporting. Prefixes the title,
                                  becomes a label, and salts the fingerprint.
    OPTERYX_ALERTS_TOKEN_SECRET   Secret Manager secret holding a GitHub token.
                                  Default 'GITHUB_TOKEN'. A GITHUB_TOKEN
                                  environment variable wins - that's the dev path.
    OPTERYX_ALERTS_ENVIRONMENT    Free text, e.g. 'production'.
    OPTERYX_ALERTS_LABELS         Extra labels, comma separated.
    OPTERYX_ALERTS_API_URL        Default 'https://api.github.com'.
    OPTERYX_ALERTS_COOLOFF_HOURS  How long a known failure stays quiet. Default 24.

    OPTERYX_ALERTS_DISCORD_WEBHOOK         The webhook URL directly - the dev path.
    OPTERYX_ALERTS_DISCORD_WEBHOOK_SECRET  Secret Manager secret holding it.
                                           Default 'DISCORD_NOTIFICATION_WEBHOOK'.
    OPTERYX_ALERTS_DISCORD_MIN_SEVERITY    Default 'CRITICAL'. Discord interrupts
                                           people, so it is gated; stdout keeps
                                           the complete record either way.
    OPTERYX_ALERTS_DISCORD_MENTION         '<@&ROLE_ID>', '@here', or empty. What
                                           turns a message into a phone push.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any

from ..exceptions import Alertable
from ..exceptions import AlertSeverity
from . import _dispatch
from . import _identity
from .discord import DiscordSink
from .github import TRACKING_LABEL
from .github import GitHubSink
from .github import normalize_repo
from .github import reset_token_cache
from .sinks import Alert
from .sinks import ListSink
from .sinks import StdoutSink

__all__ = [
    "Alert",
    "AlertSeverity",
    "DiscordSink",
    "GitHubSink",
    "ListSink",
    "StdoutSink",
    "configure",
    "flush",
    "is_enabled",
    "report",
    "reset",
]

logger = logging.getLogger(__name__)

_LEGACY_WARNED: set = set()


def _env(name: str, legacy: str):
    """Read `OPTERYX_ALERTS_*`, falling back to the legacy `PLATFORM_ISSUES_*` name."""
    value = os.environ.get(name)
    if value is not None:
        return value
    value = os.environ.get(legacy)
    if value is not None and legacy not in _LEGACY_WARNED:
        _LEGACY_WARNED.add(legacy)
        logger.warning("alerts: using legacy env var %s; rename it to %s", legacy, name)
    return value


class _Config:
    __slots__ = (
        "api_url",
        "component",
        "cooloff_seconds",
        "discord_mention",
        "discord_min_severity",
        "discord_webhook_secret",
        "enabled",
        "environment",
        "extra_labels",
        "repo",
        "sink",
        "sinks",
        "token_secret",
    )

    def __init__(self) -> None:
        self.repo = normalize_repo(_env("OPTERYX_ALERTS_REPO", "PLATFORM_ISSUES_REPO"))
        self.component = _env("OPTERYX_ALERTS_COMPONENT", "PLATFORM_ISSUES_COMPONENT") or "unknown"
        self.token_secret = (
            _env("OPTERYX_ALERTS_TOKEN_SECRET", "PLATFORM_ISSUES_TOKEN_SECRET") or "GITHUB_TOKEN"
        )
        self.environment = (
            _env("OPTERYX_ALERTS_ENVIRONMENT", "PLATFORM_ISSUES_ENVIRONMENT") or "unknown"
        )
        self.extra_labels = tuple(
            label.strip()
            for label in (_env("OPTERYX_ALERTS_LABELS", "PLATFORM_ISSUES_LABELS") or "").split(",")
            if label.strip()
        )
        self.api_url = (
            _env("OPTERYX_ALERTS_API_URL", "PLATFORM_ISSUES_API_URL") or "https://api.github.com"
        ).rstrip("/")
        try:
            hours = float(_env("OPTERYX_ALERTS_COOLOFF_HOURS", "PLATFORM_ISSUES_COOLOFF_HOURS") or 24)
        except ValueError:
            hours = 24.0
        self.cooloff_seconds = max(hours, 0.0) * 3600
        # Default ON, like auditing: observing that the platform is broken is not
        # opt-in. The stdout sink needs no configuration, so unlike the module
        # this came from, an unset repo no longer means "do nothing".
        flag = (_env("OPTERYX_ALERTS_ENABLED", "PLATFORM_ISSUES_ENABLED") or "").strip().lower()
        self.enabled = flag not in ("false", "0", "no", "off")
        self.discord_webhook_secret = (
            _env("OPTERYX_ALERTS_DISCORD_WEBHOOK_SECRET", "") or "DISCORD_NOTIFICATION_WEBHOOK"
        )
        self.discord_mention = _env("OPTERYX_ALERTS_DISCORD_MENTION", "") or ""
        self.discord_min_severity = (
            _env("OPTERYX_ALERTS_DISCORD_MIN_SEVERITY", "") or AlertSeverity.CRITICAL
        ).strip().upper()
        self.sink = (_env("OPTERYX_ALERTS_SINK", "") or "stdout").strip().lower()
        self.sinks = self._build_sinks()

    def _selected(self) -> list:
        """The sink names, as a list.

        Comma separated so channels compose - `stdout,discord` is the shape you
        want once there is more than one destination. `both` is kept as an alias
        for the original stdout+github pair so existing configuration keeps
        working.
        """
        if self.sink == "both":
            return ["stdout", "github"]
        return [name.strip() for name in self.sink.split(",") if name.strip()]

    def _build_sinks(self) -> list:
        selected = self._selected()
        sinks: list = []

        if "stdout" in selected:
            sinks.append(StdoutSink())

        if "github" in selected:
            if self.repo:
                sinks.append(
                    GitHubSink(
                        repo=self.repo, api_url=self.api_url, token_secret=self.token_secret
                    )
                )
            else:
                logger.warning("alerts: github sink selected but no repo configured")

        if "discord" in selected:
            sinks.append(
                DiscordSink(
                    webhook_secret=self.discord_webhook_secret,
                    min_severity=self.discord_min_severity,
                    mention=self.discord_mention,
                )
            )

        unknown = [name for name in selected if name not in ("stdout", "github", "discord")]
        if unknown:
            logger.warning("alerts: ignoring unknown sink(s) %s", ", ".join(unknown))

        if not sinks:
            # Never leave alerting with nowhere to go. A typo in the sink list
            # would otherwise silence the platform's only self-report.
            logger.warning("alerts: no usable sink from '%s', falling back to stdout", self.sink)
            sinks.append(StdoutSink())
        return sinks


_config_lock = threading.Lock()
_config: _Config | None = None


def _cfg() -> _Config:
    global _config
    with _config_lock:
        if _config is None:
            _config = _Config()
        return _config


def configure(**overrides: Any) -> None:
    """Override configuration programmatically; call before the first report.

    Accepts any config attribute, plus `sink=` to inject a sink object directly
    (which is how tests swap in a `ListSink`). Passing nothing re-reads the
    environment.
    """
    global _config
    with _config_lock:
        config = _Config()
        for key, value in overrides.items():
            if key == "repo":
                value = normalize_repo(value)
            if key == "sink" and not isinstance(value, str):
                # An injected sink object, or a list of them.
                config.sinks = list(value) if isinstance(value, (list, tuple)) else [value]
                continue
            if key in _Config.__slots__:
                setattr(config, key, value)
            else:
                logger.warning("alerts: configure() ignoring unknown option %r", key)
        if isinstance(overrides.get("sink"), str) or "repo" in overrides:
            config.sinks = config._build_sinks()
        _config = config
    # A sticky negative token lookup would otherwise survive a reconfiguration
    # that was made precisely to fix it.
    reset_token_cache()


def reset() -> None:
    """Clear all module state - config, dedupe table, token cache. For tests."""
    global _config
    with _config_lock:
        _config = None
    _dispatch.reset()
    reset_token_cache()
    _LEGACY_WARNED.clear()


def is_enabled() -> bool:
    return _cfg().enabled


def report(
    exc: BaseException,
    *,
    context: Mapping[str, Any] | None = None,
    note: str = "",
    fingerprint: Sequence[str] | str | None = None,
    title: str | None = None,
    severity: str | None = None,
    labels: Iterable[str] = (),
    blocking: bool = False,
) -> None:
    """Emit an alert for `exc`. Never raises.

    Local sinks (stdout) deliver inline, so the record exists before this
    returns; remote sinks (GitHub) are handed to a background worker unless
    `blocking` is set. That split is what makes the stdout line a guarantee
    rather than something queued behind a network call and lost to the crash it
    was reporting.

    Severity, most specific first: the `severity=` argument, then
    `exc.alert_severity` if the exception is `Alertable`, then ERROR.

    `context` is whatever a human needs to reproduce it - ids, names, paths -
    and is rendered into the alert and into each recurrence comment. Put the
    varying details there rather than in the fingerprint.

    Pass `fingerprint` to correct the grouping. Fire sites should include the
    dataset identifier: without it every occurrence across every dataset folds
    into one ticket that names no dataset and, after the first cooloff, never
    re-alerts.
    """
    try:
        cfg = _cfg()
        if not cfg.enabled:
            return
        if not isinstance(exc, BaseException):
            logger.warning("alerts: report() wants an exception, got %r", type(exc))
            return

        is_alertable = isinstance(exc, Alertable)

        if fingerprint is not None:
            parts = fingerprint
        else:
            parts = _identity.auto_fingerprint(exc)
            if is_alertable and exc.alert_fingerprint:
                parts = list(parts) + [str(p) for p in exc.alert_fingerprint]
        digest = _identity.fingerprint(parts, cfg.component)

        merged_context = dict(exc.alert_context()) if is_alertable else {}
        merged_context.update(dict(context or {}))

        resolved_severity = severity or (
            exc.alert_severity if is_alertable else AlertSeverity.ERROR
        )
        summary = exc.alert_summary if is_alertable else ""

        all_labels = [TRACKING_LABEL, cfg.component]
        if cfg.environment and cfg.environment != "unknown":
            all_labels.append(cfg.environment)
        all_labels.append(f"severity:{str(resolved_severity).lower()}")
        all_labels.extend(cfg.extra_labels)
        if is_alertable:
            all_labels.extend(exc.alert_labels)
        all_labels.extend(labels)

        seen_labels: set = set()
        clean_labels = []
        for label in all_labels:
            label = str(label).strip()[:50]
            if label and label not in seen_labels:
                seen_labels.add(label)
                clean_labels.append(label)

        alert = Alert(
            fingerprint=digest,
            severity=resolved_severity,
            title=f"[{cfg.component}] {title or _identity.auto_title(exc)}"[:250],
            body=_identity.render_body(
                digest=digest,
                exc=exc,
                note=note,
                summary=summary,
                context=merged_context,
                component=cfg.component,
                environment=cfg.environment,
                severity=resolved_severity,
            ),
            labels=tuple(clean_labels),
            context=merged_context,
            component=cfg.component,
            environment=cfg.environment,
            exc_type=type(_identity.root_cause(exc)).__name__,
            origin=_identity.origin(exc),
        )

        _dispatch.submit(alert, cfg.sinks, cfg.cooloff_seconds, blocking)
    except Exception as inner:  # reporting must never become the failure
        logger.warning("alerts: report() failed: %s", inner)


def flush(timeout: float = 10.0) -> None:
    """Block until queued reports have been delivered, or `timeout` elapses."""
    _dispatch.flush(timeout)
