from opteryx_catalog.opteryx_catalog import OpteryxCatalog


def _make_catalog():
    # _snapshot_from_dict doesn't touch any instance state, so we can
    # bypass __init__ (which requires a live Firestore client) entirely.
    return object.__new__(OpteryxCatalog)


def test_snapshot_from_dict_preserves_commit_message():
    catalog = _make_catalog()
    sd = {
        "snapshot-id": "abc123",
        "timestamp-ms": 1234567890,
        "author": "alice",
        "sequence-number": 1,
        "user-created": True,
        "manifest": "manifest-abc123.parquet",
        "schema-id": "0",
        "summary": {},
        "operation-type": "append",
        "parent-snapshot-id": None,
        "commit-message": "add Q3 sales data",
    }

    snapshot = catalog._snapshot_from_dict(sd)

    assert snapshot.commit_message == "add Q3 sales data"


def test_snapshot_from_dict_defaults_missing_commit_message_to_none():
    catalog = _make_catalog()
    sd = {"snapshot-id": "abc123"}

    snapshot = catalog._snapshot_from_dict(sd)

    assert snapshot.commit_message is None
