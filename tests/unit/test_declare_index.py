"""Index declarations become separate CREATE INDEX statements on PostgreSQL (#1672)."""

import pytest

from datajoint.adapters import get_adapter
from datajoint.declare import declare

DEFINITION = """
item_id : int32
---
data = null : json
index(data.size:integer)
unique index(item_id, data.name:varchar(20))
index(item_id)
"""


@pytest.fixture
def index_ddl():
    *_, post_ddl = declare('"s"."indexed"', DEFINITION, {}, get_adapter("postgresql"))
    return [ddl for ddl in post_ddl if "INDEX" in ddl]


def test_json_path_index_keeps_the_expression(index_ddl):
    assert index_ddl[0].startswith('CREATE INDEX "idx_indexed_jsonb_extract_path_text_')
    assert index_ddl[0].endswith(""" ON "s"."indexed" (((jsonb_extract_path_text("data", 'size'))::integer))""")


def test_index_mixing_columns_and_json_paths(index_ddl):
    assert index_ddl[1].startswith("CREATE UNIQUE INDEX ")
    assert index_ddl[1].endswith(""" ON "s"."indexed" ("item_id", ((jsonb_extract_path_text("data", 'name'))::varchar(20)))""")


def test_plain_column_index_is_unchanged(index_ddl):
    assert index_ddl[2] == 'CREATE INDEX "idx_indexed_item_id" ON "s"."indexed" ("item_id")'


@pytest.mark.parametrize("table", ["t", "_ephys_recording_probe_insertion"])
def test_expression_index_names_are_valid_and_distinct(table):
    adapter = get_adapter("postgresql")
    names = [
        adapter.create_index_ddl(f'"s"."{table}"', [expr]).split('"')[1]
        for expr in (
            "((jsonb_extract_path_text(\"data\", 'a'))::integer)",
            "((jsonb_extract_path_text(\"data\", 'b'))::integer)",
        )
    ]
    assert names[0] != names[1]
    for name in names:
        assert len(name) <= 63
        assert name.replace("_", "").isalnum()
