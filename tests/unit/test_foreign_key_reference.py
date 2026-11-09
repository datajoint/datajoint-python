"""Foreign key references are parsed and looked up, never evaluated.

The text after ``->`` names a table in the declaration context: a dotted name
(``Parent``, ``module.Parent``, ``Master.Part``), optionally projected with
``.proj(new_name="old_name")``. ``Table.alter`` re-parses the definition that
``describe()`` builds from names read back from the database catalog, so that
text must not be able to do anything but name a table.
"""

import types

import pytest

import datajoint as dj
from datajoint.adapters.mysql import MySQLAdapter
from datajoint.declare import prepare_declare, resolve_foreign_key_reference
from datajoint.errors import DataJointError
from datajoint.table import _attribute_name, _definition_token, _one_line_comment


class Parent(dj.Manual):
    definition = """
    parent_id : int32
    """

    class Part(dj.Part):
        definition = """
        -> master
        part_id : int32
        """


module = types.SimpleNamespace(Parent=Parent)
CONTEXT = {"Parent": Parent, "module": module, "master": Parent, "os": __import__("os")}


@pytest.mark.parametrize(
    "reference, expected",
    [
        ("Parent", Parent),
        ("  Parent  ", Parent),
        ("module.Parent", Parent),
        ("Parent.Part", Parent.Part),
        ("master.Part", Parent.Part),
        ("Parent  # a trailing comment", Parent),
    ],
)
def test_dotted_names_resolve(reference, expected):
    assert resolve_foreign_key_reference(reference, CONTEXT) is expected


@pytest.mark.parametrize(
    "reference",
    [
        "__import__('os').system('true')",
        "os.system('true')",
        "Parent.proj(x=__import__('os').getcwd())",
        "Parent.proj(x=y)",
        "Parent.proj(**{'a': 'b'})",
        "Parent.Part.__init__.__globals__",
        "Parent.__class__",
        "[c for c in ().__class__.__base__.__subclasses__()]",
        "Parent; import os",
        "Parent & 'parent_id=1'",
        "lambda: 0",
        "`schema`.`parent`",
        '"schema"."parent"',
        "Parent.delete()",
        "Parent.Part('x')",
        "",
    ],
)
def test_expressions_are_rejected(reference, monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("reference text was executed")

    monkeypatch.setattr("os.system", fail)
    with pytest.raises(DataJointError):
        resolve_foreign_key_reference(reference, CONTEXT)


@pytest.mark.parametrize("reference", ["os", "os.getcwd", "module", "Missing", "module.Missing"])
def test_non_tables_are_rejected(reference):
    with pytest.raises(DataJointError):
        resolve_foreign_key_reference(reference, CONTEXT)


def test_resolution_reads_no_builtins():
    """eval() would add __builtins__ to the context it is given; lookup does not."""
    context = dict(CONTEXT)
    resolve_foreign_key_reference("Parent", context)
    assert "__builtins__" not in context


def test_definition_with_crafted_reference_is_rejected():
    definition = """
    -> __import__('os').system('true')
    id : int32
    """
    with pytest.raises(DataJointError, match="not supported"):
        prepare_declare(definition, dict(CONTEXT), MySQLAdapter())


def test_describe_helpers():
    assert _attribute_name("subject_id") == "subject_id"
    assert _attribute_name("_hidden") == "_hidden"
    for name in ['a"); x', "a b", "a\nb", "", "1a"]:
        with pytest.raises(DataJointError):
            _attribute_name(name)
    assert _definition_token("`s`.`t`") == "`s`.`t`"
    with pytest.raises(DataJointError):
        _definition_token("Parent\n-> other")
    assert _one_line_comment("first\nsecond\r\nthird") == "first second third"
    assert _one_line_comment("") == ""
