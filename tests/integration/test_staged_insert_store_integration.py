"""Integration coverage for staged inserts using the default object store."""

import datajoint as dj


def test_staged_insert_keeps_resolved_default_store(schema_obj, mock_object_storage):
    @schema_obj
    class StagedDefault(dj.Manual):
        definition = """
        id: int
        ---
        data_file: <object@>
        """

    table = StagedDefault()
    config = table.connection._config
    original_default = config.stores["default"]

    # Make a second valid store pointing at the same test storage location.
    # Changing the default after writing must not change the stored reference.
    config.stores["alternate"] = dict(config.stores[original_default])
    try:
        with table.staged_insert1 as staged:
            staged.rec["id"] = 1
            with staged.open("data_file", ".dat") as destination:
                destination.write(b"staged contents")
            path = staged._staged_objects["data_file"]["relative_path"]
            assert f"/{table.table_name}/" in path
            config.stores["default"] = "alternate"
    finally:
        config.stores["default"] = original_default
        config.stores.pop("alternate", None)

    obj = (table & {"id": 1}).fetch1("data_file")
    assert obj.store == original_default
    assert f"/{table.table_name}/" in obj.path
    assert obj.read() == b"staged contents"
    table.delete()
