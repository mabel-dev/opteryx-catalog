"""Every queried Firestore field path is one Firestore will parse.

A field path in a `where()` is PARSED, not taken literally: an unquoted
segment must match `[a-zA-Z_][a-zA-Z_0-9]*`, and anything else must be wrapped
in backticks. Almost every field name in this catalog is hyphenated
(`target-view`, `event-kind`, `next-due-at-ms`, `read-source-keys`), so an
unquoted one is refused by the server with INVALID_ARGUMENT - before the query
is matched against any index, so no amount of index work makes it work.

This bit three separate modules independently, and each failure was invisible
to the unit tests, because the fake Firestore clients they run against parse
nothing. It is caught here instead, by reading the source: a filter whose
field path is a hyphenated literal is a query that cannot run.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

PACKAGE = pathlib.Path(__file__).resolve().parent.parent / "opteryx_catalog"

# What Firestore accepts unquoted, per the server's own error message.
_BARE = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9]*$")


def _field_filter_paths() -> list[tuple[str, int, str]]:
    """Every literal field path handed to a `FieldFilter`, with where it is.

    Read off the AST rather than by regex so a path split across lines, or
    built with an f-string around a constant, is still seen - the f-string
    case is exactly how `consumers.py` quotes its stored key.
    """
    found: list[tuple[str, int, str]] = []
    for path in sorted(PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", "")
            if name != "FieldFilter" or not node.args:
                continue
            first = node.args[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                found.append((path.name, node.lineno, first.value))
            elif isinstance(first, ast.JoinedStr):
                # An f-string: only the literal parts are visible, which is
                # enough to see whether it is backticked at both ends.
                literal = "".join(
                    part.value for part in first.values if isinstance(part, ast.Constant)
                )
                found.append((path.name, node.lineno, literal + "<expr>" if literal else "<expr>"))
    return found


def test_the_scan_finds_the_filters_we_know_about():
    """The guard is only worth having if it is actually reading the code."""
    paths = _field_filter_paths()
    assert len(paths) >= 8, paths
    assert any(module == "consumers.py" for module, _, _ in paths)
    assert any(module == "inbound_edges.py" for module, _, _ in paths)
    assert any(module == "trigger_firing.py" for module, _, _ in paths)


@pytest.mark.parametrize("module, lineno, field", _field_filter_paths())
def test_a_queried_field_path_is_bare_or_backticked(module, lineno, field):
    if field.startswith("`"):
        assert field.endswith(("`", "`<expr>")), (
            f"{module}:{lineno}: field path {field!r} opens a backtick and does not close it"
        )
        return
    assert _BARE.match(field), (
        f"{module}:{lineno}: field path {field!r} is not a bare Firestore identifier and is "
        "not backticked; Firestore refuses it with INVALID_ARGUMENT before consulting any index"
    )
