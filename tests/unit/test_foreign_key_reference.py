"""Foreign key references are parsed and looked up, never evaluated.

The text after ``->`` names a table in the declaration context: a dotted name
(``Parent``, ``module.Parent``, ``Master.Part``), optionally in parentheses,
instantiated with ``()``, or projected with ``.proj(new_name="old_name")``.
``Table.alter`` re-parses the definition that ``describe()`` builds from names
read back from the database catalog, so that text is only ever read as a name.
"""

import types

import pytest

import datajoint as dj
from datajoint.adapters.mysql import MySQLAdapter
from datajoint.declare import parse_foreign_key_reference, prepare_declare, resolve_foreign_key_reference
from datajoint.errors import DataJointError
from datajoint.table import _one_line, _quoted


class Parent(dj.Manual):
    definition = """
    parent_id : int32
    """

    class Part(dj.Part):
        definition = """
        -> master
        part_id : int32
        """


class Projectable(dj.Manual):
    """Records the arguments of proj() instead of building a projection."""

    definition = """
    projectable_id : int32
    """

    def proj(self, *args, **kwargs):
        self.proj_args = (args, kwargs)
        return self


module = types.SimpleNamespace(Parent=Parent)


@pytest.fixture
def calls():
    return []


@pytest.fixture
def context(calls):
    def record(*args, **kwargs):
        calls.append((args, kwargs))
        return Parent

    return {
        "Parent": Parent,
        "Projectable": Projectable,
        "module": module,
        "master": Parent,
        "Größe": Parent,
        "record": record,
        "__builtins__": __builtins__,
    }


@pytest.mark.parametrize(
    "reference, expected",
    [
        ("Parent", Parent),
        ("  Parent  ", Parent),
        ("module.Parent", Parent),
        ("Parent.Part", Parent.Part),
        ("master.Part", Parent.Part),
        ("(master)", Parent),
        ("(module.Parent).Part", Parent.Part),
        ("Größe", Parent),
        ("Parent  # a trailing comment", Parent),
    ],
)
def test_dotted_names_resolve(reference, expected, context):
    assert resolve_foreign_key_reference(reference, context) is expected


def test_instantiation(context):
    assert isinstance(resolve_foreign_key_reference("Parent()", context), Parent)


@pytest.mark.parametrize(
    "reference, args, kwargs",
    [
        ("Projectable.proj(new_id='projectable_id')", (), {"new_id": "projectable_id"}),
        ('Projectable().proj(a="b", c="d",)', (), {"a": "b", "c": "d"}),
        ("(Projectable).proj('x', a='b')", ("x",), {"a": "b"}),
        (r'Projectable.proj(a="say \"hi\"")', (), {"a": 'say "hi"'}),
    ],
)
def test_projection_arguments_are_string_literals(reference, args, kwargs, context):
    ref = resolve_foreign_key_reference(reference, context)
    assert ref.proj_args == (args, kwargs)


@pytest.mark.parametrize(
    "reference",
    [
        "record()",
        "(record)()",
        "module.Parent.proj(x=record())",
        "Parent.proj(**record())",
        "Parent.proj(x=record)",
        "Parent.proj(x=f'{record()}')",
        "[record() for _ in 'a']",
        "Parent; record()",
        "Parent or record()",
        "lambda: record()",
        "Parent.delete()",
        "Parent.Part('x')",
        "Parent.Part.__init__.__globals__",
        "Parent.__class__",
        "__builtins__",
        "__builtins__.open",
        "Parent & 'parent_id=1'",
        "Parent.proj(a='b', 'c')",
        "Parent.proj(a='b', a='c')",
        "",
    ],
)
def test_other_forms_are_refused(reference, context, calls):
    with pytest.raises(DataJointError, match="could not be resolved"):
        resolve_foreign_key_reference(reference, context)
    assert calls == []


@pytest.mark.parametrize("reference", ["Missing", "module.Missing", "`schema`.`parent`", '"schema"."parent"'])
def test_unknown_names_could_not_be_resolved(reference, context):
    with pytest.raises(DataJointError, match="could not be resolved"):
        resolve_foreign_key_reference(reference, context)


@pytest.mark.parametrize("reference", ["module", "record", "types", "Parent.describe"])
def test_non_tables_are_refused(reference, context):
    with pytest.raises(DataJointError, match="does not name a table|is not defined"):
        resolve_foreign_key_reference(reference, context)


def test_resolution_leaves_context_unchanged(context):
    """eval() would add __builtins__ to the context it is given; lookup does not."""
    del context["__builtins__"]
    resolve_foreign_key_reference("Parent", context)
    assert "__builtins__" not in context


def test_definition_reference_is_not_called(context, calls):
    definition = """
    -> record()
    id : int32
    """
    with pytest.raises(DataJointError, match="could not be resolved"):
        prepare_declare(definition, context, MySQLAdapter())
    assert calls == []


def test_describe_text_stays_on_one_line():
    assert _one_line("first\nsecond\r\nthird") == "first second third"
    assert _one_line("") == ""
    assert _one_line(None) is None
    text = 'a "quoted" \\ value\nwith a line break'
    steps = parse_foreign_key_reference("Parent.proj(new={})".format(_quoted(text)))
    assert steps == [("name", "Parent"), ("proj", [], {"new": _one_line(text)})]


class _Lookups:
    """An object whose attributes run code when read: a property, and a custom __getattr__ for other names."""

    Static = Parent

    def __init__(self, calls):
        self._calls = calls

    @property
    def Computed(self):
        self._calls.append("property")
        return Parent

    def __getattr__(self, name):
        self._calls.append(f"__getattr__ {name}")
        if name == "Missing":
            raise AttributeError(name)
        return Parent


def _lazy_module(calls, **attributes):
    """A module that supplies the names it does not hold through a module-level __getattr__ (PEP 562)."""
    lazy_module = types.ModuleType("lazy_module")
    vars(lazy_module).update(attributes)

    def __getattr__(name):
        calls.append(f"module __getattr__ {name}")
        if name == "Missing":
            raise AttributeError(name)
        return Parent

    lazy_module.__getattr__ = __getattr__
    return lazy_module


def test_static_lookup_runs_no_code(context, calls):
    """A name the static lookup finds is resolved without running __getattr__, even when the object has one."""
    context.update(lookups=_Lookups(calls), lazy_module=_lazy_module(calls, Parent=Parent))
    assert resolve_foreign_key_reference("lookups.Static", context) is Parent
    assert resolve_foreign_key_reference("lazy_module.Parent.Part", context) is Parent.Part
    assert calls == []


@pytest.mark.parametrize(
    "reference, call",
    [
        ("lazy_module.Parent", "module __getattr__ Parent"),
        ("lookups.Anything", "__getattr__ Anything"),
        ("lookups.Computed", "property"),
    ],
)
def test_dynamic_lookup_when_static_lookup_finds_no_value(reference, call, context, calls):
    """A module or class __getattr__ and a property still resolve: getattr runs only for them."""
    context.update(lookups=_Lookups(calls), lazy_module=_lazy_module(calls))
    assert resolve_foreign_key_reference(reference, context) is Parent
    assert calls == [call]


@pytest.mark.parametrize("reference", ["lazy_module.Missing", "lookups.Missing", "module.Missing"])
def test_attribute_found_by_neither_lookup(reference, context, calls):
    context.update(lookups=_Lookups(calls), lazy_module=_lazy_module(calls))
    with pytest.raises(DataJointError, match="could not be resolved: no attribute Missing"):
        resolve_foreign_key_reference(reference, context)


def test_attribute_lookup_finds_class_and_module_attributes(context):
    real_module = types.ModuleType("real_module")
    real_module.Parent = Parent
    context["real_module"] = real_module
    assert resolve_foreign_key_reference("real_module.Parent", context) is Parent
    assert resolve_foreign_key_reference("real_module.Parent.Part", context) is Parent.Part
