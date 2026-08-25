"""DROP WORKSPACE on a shell workspace (catalog binding, no data plane).

A workspace bound to an external catalog (see binding.py /
WORKSPACE_CATALOG_RESOLUTION.md) has a `$properties` doc and nothing else: no
collections, no datasets, no GCS locations. Dropping it must be a clean
no-op-plus-delete - empty sweep, `$properties` removed - not an error from
machinery expecting a data plane.
"""

from __future__ import annotations

from unittest.mock import patch

from opteryx_catalog.opteryx_catalog import OpteryxCatalog


class _FakeSnapshot:
    def __init__(self, data):
        self.exists = data is not None
        self._data = data

    def to_dict(self):
        return self._data


class _FakeDoc:
    def __init__(self, id_=""):
        self.id = id_
        self.deleted = False
        self.data = {}
        self._collections = {}

    def collection(self, name):
        return self._collections.setdefault(name, _FakeCollection())

    def get(self):
        # A shell workspace's `$properties` carries no `catalog` block, so
        # drop_workspace reads this and takes the domiciled-data path.
        return _FakeSnapshot(None if self.deleted else self.data)

    def delete(self):
        self.deleted = True


class _FakeCollection:
    def __init__(self):
        self._docs = {}

    def document(self, name):
        return self._docs.setdefault(name, _FakeDoc(name))

    def list_documents(self):
        return list(self._docs.values())

    def stream(self):
        return iter([])


def _shell_catalog():
    """An OpteryxCatalog handle shaped like a shell workspace, no external I/O."""
    catalog = object.__new__(OpteryxCatalog)
    catalog.workspace = "shell_ws"
    catalog.catalog_name = "shell_ws"
    catalog.gcs_bucket = None
    catalog.io = None
    catalog._catalog_ref = _FakeCollection()
    # the one doc a shell workspace has
    catalog._catalog_ref.document("$properties")
    return catalog


def test_drop_shell_workspace_is_clean():
    catalog = _shell_catalog()
    properties_doc = catalog._catalog_ref.document("$properties")

    with (
        patch.object(OpteryxCatalog, "_assert_not_deletion_protected", lambda self: None),
        patch("opteryx_catalog.opteryx_catalog.send_webhook") as webhook,
        patch("opteryx_catalog.opteryx_catalog.emit_audit") as audit,
    ):
        catalog.drop_workspace(author="alice")

    # the whole point: no error from sweep/deep-clean machinery expecting
    # datasets or storage, and the workspace's one document is gone.
    assert properties_doc.deleted is True
    assert webhook.called
    assert audit.called


def test_drop_shell_workspace_still_requires_author():
    catalog = _shell_catalog()
    try:
        catalog.drop_workspace(author=None)
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
