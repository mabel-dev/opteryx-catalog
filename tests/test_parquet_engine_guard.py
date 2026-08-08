import os
import sys

# Ensure local package imports during test runs
sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pytest

from opteryx_catalog.opteryx_catalog import _require_parquet_engine


def test_guard_passes_when_rugo_is_importable():
    # The suite writes real Parquet, so rugo must be present and healthy here.
    _require_parquet_engine()


def test_guard_names_both_install_routes(monkeypatch):
    # None in sys.modules makes `import rugo.parquet` raise, standing in for a
    # missing install without touching the real one.
    monkeypatch.setitem(sys.modules, "rugo.parquet", None)

    with pytest.raises(ImportError) as excinfo:
        _require_parquet_engine()

    message = str(excinfo.value)
    assert "pip install rugo" in message
    assert "pip install opteryx-core" in message
    # The stray-draken trap is the failure mode this guard exists to explain.
    assert "draken" in message


def test_guard_reports_a_non_import_failure(monkeypatch):
    # An ABI-mismatched rugo raises ValueError, not ImportError, on import. The
    # guard must still convert that into the actionable message.
    def _raise(*args, **kwargs):
        raise ValueError("Vector size changed (40 vs 32)")

    monkeypatch.delitem(sys.modules, "rugo.parquet", raising=False)
    monkeypatch.delitem(sys.modules, "rugo", raising=False)
    monkeypatch.setattr("builtins.__import__", _raise)

    with pytest.raises(ImportError) as excinfo:
        _require_parquet_engine()

    assert "Vector size changed" in str(excinfo.value)
