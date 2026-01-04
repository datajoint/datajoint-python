"""Tests for hidden job metadata in computed tables."""

import time

import pytest

import datajoint as dj


@pytest.fixture
def schema_job_metadata(connection_test, prefix):
    """Create a schema with job metadata enabled."""
    # Enable job metadata for this test
    original_setting = dj.config.jobs.add_job_metadata
    dj.config.jobs.add_job_metadata = True

    schema = dj.Schema(prefix + "_job_metadata", connection=connection_test)

    class Source(dj.Lookup):
        definition = """
        source_id : uint8
        ---
        value : float32
        """
        contents = [(1, 1.0), (2, 2.0), (3, 3.0)]

    class ComputedWithMetadata(dj.Computed):
        definition = """
        -> Source
        ---
        result : float32
        """

        def make(self, key):
            time.sleep(0.01)  # Small delay to ensure non-zero duration
            source = (Source & key).fetch1()
            self.insert1({**key, "result": source["value"] * 2})

    class ImportedWithMetadata(dj.Imported):
        definition = """
        -> Source
        ---
        imported_value : float32
        """

        def make(self, key):
            source = (Source & key).fetch1()
            self.insert1({**key, "imported_value": source["value"] + 10})

    class ManualTable(dj.Manual):
        definition = """
        manual_id : uint8
        ---
        data : float32
        """

    class ComputedWithPart(dj.Computed):
        definition = """
        -> Source
        ---
        total : float32
        """

        class Detail(dj.Part):
            definition = """
            -> master
            detail_idx : uint8
            ---
            detail_value : float32
            """

        def make(self, key):
            source = (Source & key).fetch1()
            self.insert1({**key, "total": source["value"] * 3})
            self.Detail.insert1({**key, "detail_idx": 0, "detail_value": source["value"]})

    context = {
        "Source": Source,
        "ComputedWithMetadata": ComputedWithMetadata,
        "ImportedWithMetadata": ImportedWithMetadata,
        "ManualTable": ManualTable,
        "ComputedWithPart": ComputedWithPart,
    }

    schema(Source, context=context)
    schema(ComputedWithMetadata, context=context)
    schema(ImportedWithMetadata, context=context)
    schema(ManualTable, context=context)
    schema(ComputedWithPart, context=context)

    yield {
        "schema": schema,
        "Source": Source,
        "ComputedWithMetadata": ComputedWithMetadata,
        "ImportedWithMetadata": ImportedWithMetadata,
        "ManualTable": ManualTable,
        "ComputedWithPart": ComputedWithPart,
    }

    # Cleanup
    schema.drop()
    dj.config.jobs.add_job_metadata = original_setting


class TestHiddenJobMetadataDeclaration:
    """Test that hidden job metadata columns are added during declaration."""

    def test_computed_table_has_hidden_metadata(self, schema_job_metadata):
        """Computed tables should have hidden job metadata columns."""
        table = schema_job_metadata["ComputedWithMetadata"]
        # Force heading to load from database
        _ = table.heading.attributes
        # Check _attributes (includes hidden)
        all_attrs = table.heading._attributes
        assert all_attrs is not None, "heading._attributes should not be None after loading"
        assert "_job_start_time" in all_attrs
        assert "_job_duration" in all_attrs
        assert "_job_version" in all_attrs
        # Check that they're hidden
        assert all_attrs["_job_start_time"].is_hidden
        assert all_attrs["_job_duration"].is_hidden
        assert all_attrs["_job_version"].is_hidden

    def test_imported_table_has_hidden_metadata(self, schema_job_metadata):
        """Imported tables should have hidden job metadata columns."""
        table = schema_job_metadata["ImportedWithMetadata"]
        _ = table.heading.attributes  # Force load
        all_attrs = table.heading._attributes
        assert "_job_start_time" in all_attrs
        assert "_job_duration" in all_attrs
        assert "_job_version" in all_attrs

    def test_manual_table_no_hidden_metadata(self, schema_job_metadata):
        """Manual tables should NOT have hidden job metadata columns."""
        table = schema_job_metadata["ManualTable"]
        _ = table.heading.attributes  # Force load
        all_attrs = table.heading._attributes
        assert "_job_start_time" not in all_attrs
        assert "_job_duration" not in all_attrs
        assert "_job_version" not in all_attrs

    def test_lookup_table_no_hidden_metadata(self, schema_job_metadata):
        """Lookup tables should NOT have hidden job metadata columns."""
        table = schema_job_metadata["Source"]
        _ = table.heading.attributes  # Force load
        all_attrs = table.heading._attributes
        assert "_job_start_time" not in all_attrs
        assert "_job_duration" not in all_attrs
        assert "_job_version" not in all_attrs

    def test_part_table_no_hidden_metadata(self, schema_job_metadata):
        """Part tables should NOT have hidden job metadata columns."""
        master = schema_job_metadata["ComputedWithPart"]
        part = master.Detail
        _ = part.heading.attributes  # Force load
        all_attrs = part.heading._attributes
        assert "_job_start_time" not in all_attrs
        assert "_job_duration" not in all_attrs
        assert "_job_version" not in all_attrs


class TestHiddenJobMetadataPopulation:
    """Test that job metadata is populated during make()."""

    def test_metadata_populated_after_make(self, schema_job_metadata):
        """Job metadata should be populated after make() completes."""
        table = schema_job_metadata["ComputedWithMetadata"]
        table.populate()

        # Fetch hidden attributes using raw SQL since fetch() filters them
        conn = table.connection
        result = conn.query(f"SELECT _job_start_time, _job_duration, _job_version FROM {table.full_table_name}").fetchall()
        assert len(result) == 3

        for row in result:
            start_time, duration, version = row
            assert start_time is not None
            assert duration is not None
            assert duration >= 0
            # Version may be empty string if git not available
            assert version is not None

    def test_metadata_not_in_default_fetch(self, schema_job_metadata):
        """Hidden metadata should not appear in default fetch()."""
        table = schema_job_metadata["ComputedWithMetadata"]
        table.populate()

        result = table.fetch(as_dict=True)
        for row in result:
            assert "_job_start_time" not in row
            assert "_job_duration" not in row
            assert "_job_version" not in row

    def test_hidden_attrs_not_in_heading_names(self, schema_job_metadata):
        """Hidden attributes should not appear in heading.names."""
        table = schema_job_metadata["ComputedWithMetadata"]
        _ = table.heading.attributes  # Force load
        names = table.heading.names
        assert "_job_start_time" not in names
        assert "_job_duration" not in names
        assert "_job_version" not in names


class TestHiddenAttributesExcludedFromJoins:
    """Test that hidden attributes are excluded from join operations."""

    def test_hidden_attrs_excluded_from_join(self, schema_job_metadata):
        """Hidden attributes should not participate in join matching."""
        computed = schema_job_metadata["ComputedWithMetadata"]
        imported = schema_job_metadata["ImportedWithMetadata"]

        # Populate both tables
        computed.populate()
        imported.populate()

        # Both have _job_start_time, _job_duration, _job_version
        # But these should NOT be used for joining
        joined = computed * imported
        # Should join on source_id only
        assert len(joined) == 3

        # The result heading should not have hidden attributes
        assert "_job_start_time" not in joined.heading.names
        assert "_job_duration" not in joined.heading.names


class TestConfigDisabled:
    """Test behavior when add_job_metadata is disabled."""

    def test_no_metadata_when_disabled(self, connection_test, prefix):
        """Tables should not have metadata columns when config is disabled."""
        # Ensure disabled
        original_setting = dj.config.jobs.add_job_metadata
        dj.config.jobs.add_job_metadata = False

        schema = dj.Schema(prefix + "_no_metadata", connection=connection_test)

        class Source(dj.Lookup):
            definition = """
            source_id : uint8
            """
            contents = [(1,), (2,)]

        class ComputedNoMetadata(dj.Computed):
            definition = """
            -> Source
            ---
            result : float32
            """

            def make(self, key):
                self.insert1({**key, "result": 1.0})

        context = {"Source": Source, "ComputedNoMetadata": ComputedNoMetadata}
        schema(Source, context=context)
        schema(ComputedNoMetadata, context=context)

        try:
            # Force heading to load from database
            _ = ComputedNoMetadata.heading.attributes
            # Check no hidden metadata columns
            all_attrs = ComputedNoMetadata.heading._attributes
            assert all_attrs is not None
            assert "_job_start_time" not in all_attrs
            assert "_job_duration" not in all_attrs
            assert "_job_version" not in all_attrs

            # Populate should still work
            ComputedNoMetadata.populate()
            assert len(ComputedNoMetadata()) == 2
        finally:
            schema.drop()
            dj.config.jobs.add_job_metadata = original_setting
