"""What is alertable, what is deliberately not, and which way the imports run."""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

from opteryx_catalog import exceptions as exc_module
from opteryx_catalog.exceptions import Alertable
from opteryx_catalog.exceptions import AlertSeverity
from opteryx_catalog.exceptions import CatalogError
from opteryx_catalog.exceptions import CollectionNotEmpty
from opteryx_catalog.exceptions import CompactionInvariantError
from opteryx_catalog.exceptions import DatasetLocked
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import GitHubAlertable
from opteryx_catalog.exceptions import ManifestProtectionError
from opteryx_catalog.exceptions import ManifestReadError
from opteryx_catalog.exceptions import ManifestRefreshError
from opteryx_catalog.exceptions import SnapshotMissingError
from opteryx_catalog.exceptions import SummaryInconsistencyError
from opteryx_catalog.exceptions import ViewNotFound

VALID_SEVERITIES = {AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL}

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _alertable_classes():
    return [
        obj
        for obj in vars(exc_module).values()
        if isinstance(obj, type) and issubclass(obj, Alertable) and obj is not Alertable
    ]


def test_the_requested_name_is_an_alias():
    assert GitHubAlertable is Alertable


def test_every_alertable_class_declares_a_valid_severity():
    classes = _alertable_classes()
    assert classes, "expected some alertable exceptions"
    for cls in classes:
        assert cls.alert_severity in VALID_SEVERITIES, cls.__name__


def test_data_loss_classes_are_critical():
    for cls in (
        ManifestReadError,
        ManifestProtectionError,
        CompactionInvariantError,
        SnapshotMissingError,
    ):
        assert cls.alert_severity == AlertSeverity.CRITICAL, cls.__name__

    assert ManifestRefreshError.alert_severity == AlertSeverity.ERROR
    assert SummaryInconsistencyError.alert_severity == AlertSeverity.WARNING


def test_caller_errors_are_never_alertable():
    """Ticketing these would file an issue on every 404.

    The mixin creeping onto a caller error is the failure mode that would make
    the whole alert stream worthless, so it is asserted rather than trusted.
    """
    for cls in (DatasetNotFound, ViewNotFound, CollectionNotEmpty, DatasetLocked):
        assert not issubclass(cls, Alertable), cls.__name__


def test_alertable_classes_are_still_catalog_errors():
    """Existing `except CatalogError` handlers must keep working."""
    for cls in _alertable_classes():
        assert issubclass(cls, CatalogError), cls.__name__


def test_mixin_composes_with_keyerror_derived_exceptions():
    """No __init__ on the mixin is what makes this work without MRO surgery."""

    class Composed(Alertable, KeyError, CatalogError):
        alert_severity = AlertSeverity.WARNING

    err = Composed("missing")
    assert isinstance(err, KeyError)
    assert isinstance(err, Alertable)
    assert err.alert_severity == AlertSeverity.WARNING


def test_context_is_chainable_and_merges():
    err = ManifestReadError("boom").with_alert_context(dataset="landing.http")
    assert isinstance(err, ManifestReadError)
    err.with_alert_context(snapshot=1785906332806)
    assert err.alert_context() == {"dataset": "landing.http", "snapshot": 1785906332806}


def test_context_defaults_to_empty_without_touching_the_class():
    assert ManifestReadError("boom").alert_context() == {}
    assert ManifestReadError("other").alert_context() == {}


def test_instance_overrides_the_class_default():
    err = ManifestRefreshError("boom")
    err.alert_severity = AlertSeverity.CRITICAL
    assert err.alert_severity == AlertSeverity.CRITICAL
    assert ManifestRefreshError.alert_severity == AlertSeverity.ERROR


# Layering is asserted from each module's own import statements rather than by
# importing it in a subprocess and inspecting sys.modules. `opteryx_catalog
# /__init__.py` eagerly re-exports the whole package, so importing ANY submodule
# drags in every other one - a runtime check would measure that and nothing else.


def _imported_names(path):
    """Every module named by an import statement in one file, absolute or relative."""
    import ast

    with open(path, "r", encoding="utf-8") as handle:
        tree = ast.parse(handle.read(), filename=path)

    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.append(("." * (node.level or 0)) + (node.module or ""))
    return names


def _alerts_modules():
    directory = os.path.join(REPO_ROOT, "opteryx_catalog", "alerts")
    return [
        os.path.join(directory, name)
        for name in sorted(os.listdir(directory))
        if name.endswith(".py")
    ]


def test_alerts_does_not_import_the_catalog_subpackage():
    """Strictly one-way: exceptions <- alerts <- catalog.

    If alerts ever imports catalog, the fire sites gain an import cycle and the
    subpackage stops being safe to import from anywhere.
    """
    modules = _alerts_modules()
    assert modules, "expected some alerts modules"
    for path in modules:
        for name in _imported_names(path):
            assert "catalog.catalog" not in name.replace("..", "catalog."), (
                f"{os.path.basename(path)} imports {name}"
            )
            assert not name.startswith("opteryx_catalog.catalog"), (
                f"{os.path.basename(path)} imports {name}"
            )
            assert name != "..catalog", f"{os.path.basename(path)} imports {name}"


def test_exceptions_imports_nothing():
    """The mixin is plain class attributes precisely so this stays true.

    A dataclass, a TypedDict or a typing import here would put something ahead
    of the exception hierarchy in the import order, which is how import cycles
    start in a package where everything else depends on this module.
    """
    path = os.path.join(REPO_ROOT, "opteryx_catalog", "exceptions.py")
    assert _imported_names(path) == []


if __name__ == "__main__":  # pragma: no cover
    import pytest

    pytest.main([__file__, "-v"])
