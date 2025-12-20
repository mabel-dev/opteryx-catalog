from pyiceberg_firestore_gcs.view import ViewMetadata


def test_viewmetadata_workspace_roundtrip():
    # Verify from_dict reads workspace and to_dict emits it
    d = {"sql_text": "select 1", "workspace": "public", "view_version": "1"}

    vm2 = ViewMetadata.from_dict(d)
    assert vm2.workspace == "public"

    out = vm2.to_dict()
    assert out["workspace"] == "public"
