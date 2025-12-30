def test_import_opteryx_catalog():
    import importlib

    mod = importlib.import_module("opteryx_catalog")
    assert mod is not None
