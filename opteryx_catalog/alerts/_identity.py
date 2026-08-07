"""Deriving a ticket's identity and prose from an exception.

Everything here is transport-agnostic: what the failure is, where it came from,
and which other occurrences are the same failure. `sinks.py` decides how that is
delivered.

Deduplication is the load-bearing part, because a broken platform tends to break
on every request. Two exceptions are the same failure if they are the same type
raised through the same call path. The message is deliberately excluded - that is
where the varying details live (dataset names, ids, paths), and including it
would fragment one bug across a hundred tickets. Function names are used rather
than line numbers, so editing a file does not orphan its open issues.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import traceback
from collections.abc import Mapping
from typing import Any

MAX_TITLE = 200
MAX_TRACEBACK_CHARS = 20000  # GitHub caps issue bodies at 65536
MARKER_PREFIX = "platform-incident"

# Frames from inside this package describe how the stack was taken, not where the
# problem is. Matched by directory rather than by filename: the reporter used to
# be one module and checked `basename(__file__)`, which silently stopped working
# the moment it was split across several files - every alert would then have
# re-fingerprinted against our own plumbing. `tests/test_alerts_identity.py`
# asserts no frame from this package reaches the fingerprint material.
_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))


def _is_ours(filename: str) -> bool:
    try:
        return os.path.dirname(os.path.abspath(filename)) == _PACKAGE_DIR
    except Exception:
        return False


def frames(exc: BaseException) -> list:
    """The call path as (module basename, function) pairs, outermost first.

    Follows `__cause__`/`__context__` to the original failure, because that is
    where the real call path is: a wrapper raised at the boundary would
    otherwise fingerprint every distinct underlying failure identically.

    Basenames rather than full paths, so a container's `/app/...` and a
    developer's checkout agree.
    """
    collected: list = []
    seen: set = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        tb = current.__traceback__
        while tb is not None:
            code = tb.tb_frame.f_code
            collected.append((os.path.basename(code.co_filename), code.co_name))
            tb = tb.tb_next
        current = current.__cause__ or current.__context__
    return collected


def caller_frames() -> list:
    """The current stack, for an exception constructed but never raised.

    Reporting a condition you detected rather than caught is legitimate - most
    of the catalog's alertable conditions are detected, not raised - and such an
    exception arrives with no traceback at all. The stack at the report site is
    the right substitute: it names the code that noticed.
    """
    return [
        (os.path.basename(frame.filename), frame.name)
        for frame in traceback.extract_stack()
        if not _is_ours(frame.filename)
    ]


def root_cause(exc: BaseException) -> BaseException:
    current = exc
    seen = {id(current)}
    while True:
        nxt = current.__cause__ or current.__context__
        if nxt is None or id(nxt) in seen:
            return current
        seen.add(id(nxt))
        current = nxt


def origin(exc: BaseException) -> str:
    """A short 'where' for the title - the deepest frame available."""
    found = frames(exc) or caller_frames()
    if not found:
        return "unknown"
    filename, function = found[-1]
    return f"{filename.removesuffix('.py')}.{function}"


def auto_title(exc: BaseException) -> str:
    """`ExcType: message` from the root cause, plus where it came from.

    For a human scanning the issue list. It plays no part in identity, so it is
    free to carry the varying details the fingerprint excludes.
    """
    root = root_cause(exc)
    message = " ".join(str(root).split())
    head = type(root).__name__
    if message:
        head = f"{head}: {message}"
    where = origin(exc)
    title = f"{head} in {where}"
    if len(title) > MAX_TITLE:
        keep = MAX_TITLE - len(where) - len(" in ") - 1
        title = f"{head[: max(keep, 0)]}… in {where}"
    return title


def auto_fingerprint(exc: BaseException) -> list:
    """Identity: exception type plus the call path it came through.

    The type name is the first component, so two different exception types
    reported from the same site never collide. What this CANNOT separate is one
    type covering several conditions reachable from the same function - those
    sites must pass an explicit fingerprint naming the condition.
    """
    found = frames(exc) or caller_frames()
    path = ">".join(f"{filename}:{function}" for filename, function in found)
    return [type(root_cause(exc)).__name__, path]


def fingerprint(parts, component: str) -> str:
    """Hash identity parts into the 16 hex chars written into the ticket.

    `component` is salted in so the same bug in two services gets two tickets -
    they are usually owned by different people and fixed separately.
    """
    if isinstance(parts, str):
        parts = [parts]
    material = "|".join([component, *(str(p) for p in parts)])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def marker(digest: str) -> str:
    return f"<!-- {MARKER_PREFIX}: {digest} -->"


def jsonable(value: Any) -> Any:
    """Best-effort conversion, so a stray object in the context can't lose the alert."""
    if isinstance(value, Mapping):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def context_block(context: Mapping) -> str:
    try:
        return json.dumps(jsonable(context), indent=2, sort_keys=True)
    except Exception:
        return repr(context)


def format_traceback(exc: BaseException) -> str:
    try:
        text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    except Exception:
        text = f"{type(exc).__name__}: {exc}"
    if not exc.__traceback__:
        outside = [frame for frame in traceback.extract_stack() if not _is_ours(frame.filename)]
        text = text.rstrip() + "\n\n(constructed, not raised - stack at the report site:)\n"
        text += "".join(traceback.format_list(outside))
    if len(text) > MAX_TRACEBACK_CHARS:
        text = text[:MAX_TRACEBACK_CHARS] + "\n… truncated …"
    return text


def render_body(
    *,
    digest: str,
    exc: BaseException,
    note: str,
    summary: str,
    context: Mapping,
    component: str,
    environment: str,
    severity: str,
) -> str:
    """The markdown body. GitHub-shaped, but harmless as a field on a log record."""
    root = root_cause(exc)
    lines = [
        marker(digest),
        "",
        f"**`{type(root).__name__}`** raised in `{origin(exc)}`.",
        "",
        "```",
        " ".join(str(root).split()) or "(no message)",
        "```",
    ]

    if summary:
        lines += ["", summary]

    if root is not exc:
        # Something wrapped it on the way out. Title and fingerprint follow the
        # root cause, so show what the caller actually saw as well.
        lines += [
            "",
            f"Surfaced as `{type(exc).__name__}`: {' '.join(str(exc).split()) or '(no message)'}",
        ]

    if note:
        lines += ["", note.strip()]

    lines += [
        "",
        "| | |",
        "|---|---|",
        f"| Severity | `{severity}` |",
        f"| Component | `{component}` |",
        f"| Environment | `{environment}` |",
        f"| First seen | {time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime())} |",
        f"| Fingerprint | `{digest}` |",
    ]

    if context:
        lines += ["", "### Context", "", "```json", context_block(context), "```"]

    lines += [
        "",
        "### Traceback",
        "",
        "<details><summary>stack</summary>",
        "",
        "```python",
        format_traceback(exc).rstrip(),
        "```",
        "",
        "</details>",
        "",
        "---",
        "",
        (
            f"_Filed automatically by `{component}`. Recurrences are folded into this "
            "issue rather than filed separately; if this is fixed, close it - a fresh "
            "occurrence afterwards will open a new issue._"
        ),
    ]
    return "\n".join(lines)
