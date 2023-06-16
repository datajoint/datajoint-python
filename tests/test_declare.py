# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_declare.py
# Compiled at: 2023-02-18 19:51:35
# Size of source mod 2**32: 9386 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
import datajoint.declare as declare
import pytest
from . import connection_root, connection_test
from schemas.default import schema, Subject, TTest, TTest2, User, Experiment, IndexRich, ThingC, ThingA, ThingB, Auto, Trial, Ephys

def test_schema_decorator(Subject):
    @py_assert3 = dj.Lookup
    @py_assert5 = issubclass(Subject, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.Lookup\n})\n}' % {'py0':@pytest_ar._saferepr(issubclass) if 'issubclass' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(issubclass) else 'issubclass',  'py1':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    @py_assert3 = dj.Part
    @py_assert5 = issubclass(Subject, @py_assert3)
    @py_assert7 = not @py_assert5
    if not @py_assert7:
        @py_format8 = 'assert not %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.Part\n})\n}' % {'py0':@pytest_ar._saferepr(issubclass) if 'issubclass' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(issubclass) else 'issubclass',  'py1':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert3 = @py_assert5 = @py_assert7 = None


def test_class_help(TTest, TTest2):
    @py_assert1 = TTest.definition
    @py_assert5 = TTest.__doc__
    @py_assert3 = @py_assert1 in @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.definition\n} in %(py6)s\n{%(py6)s = %(py4)s.__doc__\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(TTest) if 'TTest' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest) else 'TTest',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(TTest) if 'TTest' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest) else 'TTest',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    @py_assert1 = TTest.definition
    @py_assert5 = TTest2.__doc__
    @py_assert3 = @py_assert1 in @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.definition\n} in %(py6)s\n{%(py6)s = %(py4)s.__doc__\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(TTest) if 'TTest' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest) else 'TTest',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(TTest2) if 'TTest2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest2) else 'TTest2',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None


def test_instance_help(TTest, TTest2):
    @py_assert1 = TTest()
    @py_assert3 = @py_assert1.definition
    @py_assert7 = TTest()
    @py_assert9 = @py_assert7.__doc__
    @py_assert5 = @py_assert3 in @py_assert9
    if not @py_assert5:
        @py_format11 = @pytest_ar._call_reprcompare(('in', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s()\n}.definition\n} in %(py10)s\n{%(py10)s = %(py8)s\n{%(py8)s = %(py6)s()\n}.__doc__\n}', ), (@py_assert3, @py_assert9)) % {'py0':@pytest_ar._saferepr(TTest) if 'TTest' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest) else 'TTest',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(TTest) if 'TTest' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest) else 'TTest',  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = None
    @py_assert1 = TTest2()
    @py_assert3 = @py_assert1.definition
    @py_assert7 = TTest2()
    @py_assert9 = @py_assert7.__doc__
    @py_assert5 = @py_assert3 in @py_assert9
    if not @py_assert5:
        @py_format11 = @pytest_ar._call_reprcompare(('in', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s()\n}.definition\n} in %(py10)s\n{%(py10)s = %(py8)s\n{%(py8)s = %(py6)s()\n}.__doc__\n}', ), (@py_assert3, @py_assert9)) % {'py0':@pytest_ar._saferepr(TTest2) if 'TTest2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest2) else 'TTest2',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(TTest2) if 'TTest2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TTest2) else 'TTest2',  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = None


def test_describe(Subject, User, Experiment):
    """real_definition should match original definition"""
    rel = Experiment()
    context = locals()
    s1 = declare(rel.full_table_name, rel.definition, context)
    s2 = declare(rel.full_table_name, rel.describe(), context)
    @py_assert1 = s2 == s1
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (s2, s1)) % {'py0':@pytest_ar._saferepr(s2) if 's2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s2) else 's2',  'py2':@pytest_ar._saferepr(s1) if 's1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s1) else 's1'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None


def test_describe_indexes(Subject, User, IndexRich):
    """real_definition should match original definition"""
    rel = IndexRich()
    context = locals()
    s1 = declare(rel.full_table_name, rel.definition, context)
    s2 = declare(rel.full_table_name, rel.describe(), context)
    if not s2:
        @py_format1 = (@pytest_ar._format_assertmsg(s1) + '\n>assert %(py0)s') % {'py0': @pytest_ar._saferepr(s2) if ('s2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s2)) else 's2'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))


def test_describe_dependencies(ThingC, ThingA, ThingB):
    """real_definition should match original definition"""
    rel = ThingC()
    context = locals()
    s1 = declare(rel.full_table_name, rel.definition, context)
    s2 = declare(rel.full_table_name, rel.describe(), context)
    @py_assert1 = s2 == s1
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (s2, s1)) % {'py0':@pytest_ar._saferepr(s2) if 's2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s2) else 's2',  'py2':@pytest_ar._saferepr(s1) if 's1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s1) else 's1'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None


@pytest.fixture
def Type(schema):

    @schema
    class Type(dj.Lookup):
        definition = '\n        type :  varchar(255)\n        '
        contents = zip(('Type1', 'Type2', 'Type3'))

    yield Type
    Type.drop()


@pytest.fixture
def TypeMaster(schema, Type):

    @schema
    class TypeMaster(dj.Manual):
        definition = '\n        master_id : int\n        '

        class Type(dj.Part):
            definition = '\n            -> TypeMaster\n            -> Type\n            '

    yield TypeMaster
    TypeMaster.drop()


def test_part(TypeMaster):
    pass


def test_attributes(Auto, Subject, Experiment, Trial, Ephys):
    @py_assert1 = Auto.heading
    @py_assert3 = @py_assert1.names
    @py_assert6 = [
     'id', 'name']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.heading\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Auto) if 'Auto' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Auto) else 'Auto',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert0 = Auto.heading.attributes['id']
    @py_assert2 = @py_assert0.autoincrement
    if not @py_assert2:
        @py_format4 = 'assert %(py3)s\n{%(py3)s = %(py1)s.autoincrement\n}' % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert0 = @py_assert2 = None
    @py_assert1 = Subject.heading
    @py_assert3 = @py_assert1.names
    @py_assert6 = [
     'subject_id','real_id','species','date_of_birth','subject_notes']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.heading\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = Subject.primary_key
    @py_assert4 = [
     'subject_id']
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert0 = Subject.heading.attributes['subject_id']
    @py_assert2 = @py_assert0.numeric
    if not @py_assert2:
        @py_format4 = 'assert %(py3)s\n{%(py3)s = %(py1)s.numeric\n}' % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert0 = @py_assert2 = None
    @py_assert0 = Subject.heading.attributes['real_id']
    @py_assert2 = @py_assert0.numeric
    @py_assert4 = not @py_assert2
    if not @py_assert4:
        @py_format5 = 'assert not %(py3)s\n{%(py3)s = %(py1)s.numeric\n}' % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert0 = @py_assert2 = @py_assert4 = None
    @py_assert1 = Experiment.heading
    @py_assert3 = @py_assert1.names
    @py_assert6 = [
     'subject_id','experiment_id','experiment_date','username','data_path','notes','entry_time']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.heading\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = Experiment.primary_key
    @py_assert4 = [
     'subject_id', 'experiment_id']
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Trial.heading
    @py_assert3 = @py_assert1.names
    @py_assert6 = [
     'animal', 'experiment_id', 'trial_id', 'start_time']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.heading\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = Trial.primary_key
    @py_assert4 = [
     'animal', 'experiment_id', 'trial_id']
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Ephys.heading
    @py_assert3 = @py_assert1.names
    @py_assert6 = [
     'animal','experiment_id','trial_id','sampling_frequency','duration']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.heading\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = Ephys.primary_key
    @py_assert4 = [
     'animal', 'experiment_id', 'trial_id']
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Ephys.Channel
    @py_assert3 = @py_assert1.heading
    @py_assert5 = @py_assert3.names
    @py_assert8 = [
     'animal','experiment_id','trial_id','channel','voltage','current']
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.Channel\n}.heading\n}.names\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert8 = None
    @py_assert1 = Ephys.Channel
    @py_assert3 = @py_assert1.primary_key
    @py_assert6 = [
     'animal', 'experiment_id', 'trial_id', 'channel']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.Channel\n}.primary_key\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert0 = Ephys.Channel.heading.attributes['voltage']
    @py_assert2 = @py_assert0.is_blob
    if not @py_assert2:
        @py_format4 = 'assert %(py3)s\n{%(py3)s = %(py1)s.is_blob\n}' % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert0 = @py_assert2 = None


def test_dependencies(Experiment, User, Subject, Trial, Ephys):
    @py_assert1 = Experiment.full_table_name
    @py_assert5 = User.children
    @py_assert7 = False
    @py_assert9 = @py_assert5(primary=@py_assert7)
    @py_assert3 = @py_assert1 in @py_assert9
    if not @py_assert3:
        @py_format11 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py10)s\n{%(py10)s = %(py6)s\n{%(py6)s = %(py4)s.children\n}(primary=%(py8)s)\n}', ), (@py_assert1, @py_assert9)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(User) if 'User' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(User) else 'User',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = None
    @py_assert2 = Experiment.parents
    @py_assert4 = False
    @py_assert6 = @py_assert2(primary=@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     User.full_table_name}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.parents\n}(primary=%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert1 = Experiment.full_table_name
    @py_assert5 = User.children
    @py_assert7 = False
    @py_assert9 = @py_assert5(primary=@py_assert7)
    @py_assert3 = @py_assert1 in @py_assert9
    if not @py_assert3:
        @py_format11 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py10)s\n{%(py10)s = %(py6)s\n{%(py6)s = %(py4)s.children\n}(primary=%(py8)s)\n}', ), (@py_assert1, @py_assert9)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(User) if 'User' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(User) else 'User',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = None
    @py_assert2 = Experiment.parents
    @py_assert4 = False
    @py_assert6 = @py_assert2(primary=@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     User.full_table_name}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.parents\n}(primary=%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert1 = (s.full_table_name for s in Experiment.parents(primary=False, as_objects=True))
    @py_assert3 = set(@py_assert1)
    @py_assert6 = {
     User.full_table_name}
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = Experiment.full_table_name
    @py_assert5 = Subject.descendants
    @py_assert7 = @py_assert5()
    @py_assert3 = @py_assert1 in @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.descendants\n}()\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = Experiment.full_table_name
    @py_assert4 = {s.full_table_name for s in Subject.descendants(as_objects=True)}
    @py_assert3 = @py_assert1 in @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Subject.full_table_name
    @py_assert5 = Experiment.ancestors
    @py_assert7 = @py_assert5()
    @py_assert3 = @py_assert1 in @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.ancestors\n}()\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = Subject.full_table_name
    @py_assert4 = {s.full_table_name for s in Experiment.ancestors(as_objects=True)}
    @py_assert3 = @py_assert1 in @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Trial.full_table_name
    @py_assert5 = Experiment.descendants
    @py_assert7 = @py_assert5()
    @py_assert3 = @py_assert1 in @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.descendants\n}()\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = Trial.full_table_name
    @py_assert4 = {s.full_table_name for s in Experiment.descendants(as_objects=True)}
    @py_assert3 = @py_assert1 in @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = Experiment.full_table_name
    @py_assert5 = Trial.ancestors
    @py_assert7 = @py_assert5()
    @py_assert3 = @py_assert1 in @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.ancestors\n}()\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = Experiment.full_table_name
    @py_assert4 = {s.full_table_name for s in Trial.ancestors(as_objects=True)}
    @py_assert3 = @py_assert1 in @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('in', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.full_table_name\n} in %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert2 = Trial.children
    @py_assert4 = True
    @py_assert6 = @py_assert2(primary=@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     Ephys.full_table_name, Trial.Condition.full_table_name}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.children\n}(primary=%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert2 = Trial.parts
    @py_assert4 = @py_assert2()
    @py_assert6 = set(@py_assert4)
    @py_assert9 = {
     Trial.Condition.full_table_name}
    @py_assert8 = @py_assert6 == @py_assert9
    if not @py_assert8:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py0)s(%(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.parts\n}()\n})\n} == %(py10)s', ), (@py_assert6, @py_assert9)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert9 = None
    @py_assert1 = (s.full_table_name for s in Trial.parts(as_objects=True))
    @py_assert3 = set(@py_assert1)
    @py_assert6 = {
     Trial.Condition.full_table_name}
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert2 = Ephys.parents
    @py_assert4 = True
    @py_assert6 = @py_assert2(primary=@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     Trial.full_table_name}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.parents\n}(primary=%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert1 = (s.full_table_name for s in Ephys.parents(primary=True, as_objects=True))
    @py_assert3 = set(@py_assert1)
    @py_assert6 = {
     Trial.full_table_name}
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert2 = Ephys.children
    @py_assert4 = True
    @py_assert6 = @py_assert2(primary=@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     Ephys.Channel.full_table_name}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.children\n}(primary=%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert1 = (s.full_table_name for s in Ephys.children(primary=True, as_objects=True))
    @py_assert3 = set(@py_assert1)
    @py_assert6 = {
     Ephys.Channel.full_table_name}
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert2 = Ephys.Channel
    @py_assert4 = @py_assert2.parents
    @py_assert6 = True
    @py_assert8 = @py_assert4(primary=@py_assert6)
    @py_assert10 = set(@py_assert8)
    @py_assert13 = {
     Ephys.full_table_name}
    @py_assert12 = @py_assert10 == @py_assert13
    if not @py_assert12:
        @py_format15 = @pytest_ar._call_reprcompare(('==', ), (@py_assert12,), ('%(py11)s\n{%(py11)s = %(py0)s(%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.Channel\n}.parents\n}(primary=%(py7)s)\n})\n} == %(py14)s', ), (@py_assert10, @py_assert13)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py14':@pytest_ar._saferepr(@py_assert13)}
        @py_format17 = 'assert %(py16)s' % {'py16': @py_format15}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert13 = None
    @py_assert1 = (s.full_table_name for s in Ephys.Channel.parents(primary=True, as_objects=True))
    @py_assert3 = set(@py_assert1)
    @py_assert6 = {
     Ephys.full_table_name}
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None


@pytest.fixture
def A(schema):

    @schema
    class A(dj.Manual):
        definition = '\n        a: int\n        '

    yield A
    A.drop()


@pytest.fixture
def B(schema, A):

    @schema
    class B(dj.Manual):
        definition = '\n        -> A\n        b: int\n        '

    yield B
    B.drop()


@pytest.fixture
def Master(schema, B):

    @schema
    class Master(dj.Manual):
        definition = '\n        table_master: int\n        '

        class Part(dj.Part):
            definition = '\n            -> master\n            -> B\n            '

    yield Master
    Master.drop()


def test_descendants_only_contain_part_table(A, Master):
    """issue #927"""
    @py_assert1 = A.descendants
    @py_assert3 = @py_assert1()
    @py_assert6 = [
     '`djtest_test1`.`a`', '`djtest_test1`.`b`', '`djtest_test1`.`master__part`']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.descendants\n}()\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None


def test_bad_attribute_name(schema):
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class BadName(dj.Manual):
            definition = '\n            Bad_name : int\n            '

        BadName.drop()


def test_bad_fk_rename(schema, A):
    """issue #381"""
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class B(dj.Manual):
            definition = '\n            b -> A    # invalid, the new syntax is (b) -> A\n            '

        B.drop()


def test_primary_nullable_foreign_key(schema, Experiment):
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class Q(dj.Manual):
            definition = '\n            -> [nullable] Experiment\n            '

        Q.drop()


def test_invalid_foreign_key_option(schema, Experiment, User):
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class R(dj.Manual):
            definition = '\n            -> Experiment\n            ----\n            -> [optional] User\n            '

        R.drop()


def test_unsupported_datatype(schema):
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class Q(dj.Manual):
            definition = '\n            experiment : int\n            ---\n            description : never\n            '

        Q.drop()


def test_int_datatype(schema):

    @schema
    class Owner(dj.Manual):
        definition = '\n        ownerid : int\n        ---\n        car_count : integer\n        '

    Owner.drop()


def test_unsupported_int_datatype(schema):
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class Driver(dj.Manual):
            definition = '\n            driverid : tinyint\n            ---\n            car_count : tinyinteger\n            '

        Driver.drop()


def test_long_table_name(schema):
    """
    test issue #205 -- reject table names over 64 characters in length
    """
    with pytest.raises(dj.errors.DataJointError):

        @schema
        class WhyWouldAnyoneCreateATableNameThisLong(dj.Manual):
            definition = '\n            master : int\n            '

            class WithSuchALongPartNameThatItCrashesMySQL(dj.Part):
                definition = '\n                -> (master)\n                '

    dj.VirtualModule(schema.database, schema.database).WhyWouldAnyoneCreateATableNameThisLong.drop()