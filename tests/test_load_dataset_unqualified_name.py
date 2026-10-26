from __future__ import annotations

import pytest

from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def test_unqualified_name_is_dataset_not_found_not_valueerror():
    """`workspace.pypi` reaches the catalog as the bare `pypi` once the workspace
    is stripped; that must be a DatasetNotFound the engine can report, not an
    unpack ValueError, and it must be refused before any Firestore read."""
    catalog = object.__new__(OpteryxCatalog)

    with pytest.raises(DatasetNotFound, match="collection.dataset"):
        catalog.load_dataset("pypi")
