def test_import_does_not_eager_load_heavy_deps():
    import sys
    import datajoint  # noqa: F401

    assert "datajoint.diagram" not in sys.modules
    assert "pandas" not in sys.modules
