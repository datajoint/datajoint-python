# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_autopopulate.py
# Compiled at: 2023-02-17 19:13:57
# Size of source mod 2**32: 4120 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint import DataJointError
import random, pytest
from . import PREFIX, connection_root, connection_test
from schemas.default import schema, Subject, User, Experiment, Trial, Ephys

def test_populate(Subject, Experiment, Trial, Ephys):
    random.seed('populate')
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    @py_assert1 = Experiment()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('table already filled?') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    Experiment.populate()
    @py_assert2 = Experiment()
    @py_assert4 = len(@py_assert2)
    @py_assert9 = Subject()
    @py_assert11 = len(@py_assert9)
    @py_assert14 = Experiment.fake_experiments_per_subject
    @py_assert16 = @py_assert11 * @py_assert14
    @py_assert6 = @py_assert4 == @py_assert16
    if not @py_assert6:
        @py_format17 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == (%(py12)s\n{%(py12)s = %(py7)s(%(py10)s\n{%(py10)s = %(py8)s()\n})\n} * %(py15)s\n{%(py15)s = %(py13)s.fake_experiments_per_subject\n})', ), (@py_assert4, @py_assert16)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py13':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format19 = 'assert %(py18)s' % {'py18': @py_format17}
        raise AssertionError(@pytest_ar._format_explanation(@py_format19))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert9 = @py_assert11 = @py_assert14 = @py_assert16 = None
    @py_assert1 = Trial()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('table already filled?') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    restriction = Subject.proj(animal='subject_id').fetch('KEY')[0]
    d = Trial.connection.dependencies
    d.load()
    Trial.populate(restriction)
    @py_assert1 = Trial()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('table was not populated') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    key_source = Trial.key_source
    @py_assert3 = key_source & Trial
    @py_assert4 = len(@py_assert3)
    @py_assert10 = key_source & restriction
    @py_assert11 = len(@py_assert10)
    @py_assert6 = @py_assert4 == @py_assert11
    if not @py_assert6:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py12)s\n{%(py12)s = %(py7)s((%(py8)s & %(py9)s))\n}', ), (@py_assert4, @py_assert11)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(key_source) if 'key_source' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key_source) else 'key_source',  'py2':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(key_source) if 'key_source' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key_source) else 'key_source',  'py9':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert10 = @py_assert11 = None
    @py_assert3 = key_source - Trial
    @py_assert4 = len(@py_assert3)
    @py_assert10 = key_source - restriction
    @py_assert11 = len(@py_assert10)
    @py_assert6 = @py_assert4 == @py_assert11
    if not @py_assert6:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s - %(py2)s))\n} == %(py12)s\n{%(py12)s = %(py7)s((%(py8)s - %(py9)s))\n}', ), (@py_assert4, @py_assert11)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(key_source) if 'key_source' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key_source) else 'key_source',  'py2':@pytest_ar._saferepr(Trial) if 'Trial' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Trial) else 'Trial',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(key_source) if 'key_source' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key_source) else 'key_source',  'py9':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert10 = @py_assert11 = None
    @py_assert1 = Ephys()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = 'assert not %(py2)s\n{%(py2)s = %(py0)s()\n}' % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    @py_assert1 = Ephys.Channel
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = 'assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.Channel\n}()\n}' % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    Ephys.populate()
    @py_assert1 = Ephys()
    if not @py_assert1:
        @py_format3 = 'assert %(py2)s\n{%(py2)s = %(py0)s()\n}' % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    @py_assert1 = Ephys.Channel
    @py_assert3 = @py_assert1()
    if not @py_assert3:
        @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.Channel\n}()\n}' % {'py0':@pytest_ar._saferepr(Ephys) if 'Ephys' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Ephys) else 'Ephys',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = @py_assert3 = None


def test_populate_exclude_error_and_ignore_jobs(schema, Subject, Experiment):
    random.seed('populate')
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    @py_assert1 = Experiment()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('table already filled?') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    keys = Experiment.key_source.fetch('KEY', limit=2)
    for idx, key in enumerate(keys):
        if idx == 0:
            schema.jobs.ignore(Experiment.table_name, key)
        else:
            schema.jobs.error(Experiment.table_name, key, '')

    Experiment.populate(reserve_jobs=True)
    @py_assert2 = Experiment.key_source
    @py_assert5 = @py_assert2 & Experiment
    @py_assert6 = len(@py_assert5)
    @py_assert11 = Experiment.key_source
    @py_assert13 = len(@py_assert11)
    @py_assert15 = 2
    @py_assert17 = @py_assert13 - @py_assert15
    @py_assert8 = @py_assert6 == @py_assert17
    if not @py_assert8:
        @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py0)s((%(py3)s\n{%(py3)s = %(py1)s.key_source\n} & %(py4)s))\n} == (%(py14)s\n{%(py14)s = %(py9)s(%(py12)s\n{%(py12)s = %(py10)s.key_source\n})\n} - %(py16)s)', ), (@py_assert6, @py_assert17)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py10':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py16':@pytest_ar._saferepr(@py_assert15)}
        @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
        raise AssertionError(@pytest_ar._format_explanation(@py_format20))
    @py_assert2 = @py_assert5 = @py_assert6 = @py_assert8 = @py_assert11 = @py_assert13 = @py_assert15 = @py_assert17 = None


def test_allow_direct_insert(Subject, Experiment):
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    key = Subject().fetch('KEY', limit=1)[0]
    key['experiment_id'] = 1000
    key['experiment_date'] = '2018-10-30'
    Experiment.insert1(key, allow_direct_insert=True)


def test_multi_processing(Subject, Experiment):
    random.seed('populate')
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    @py_assert1 = Experiment()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('table already filled?') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    Experiment.populate(processes=2)
    @py_assert2 = Experiment()
    @py_assert4 = len(@py_assert2)
    @py_assert9 = Subject()
    @py_assert11 = len(@py_assert9)
    @py_assert14 = Experiment.fake_experiments_per_subject
    @py_assert16 = @py_assert11 * @py_assert14
    @py_assert6 = @py_assert4 == @py_assert16
    if not @py_assert6:
        @py_format17 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == (%(py12)s\n{%(py12)s = %(py7)s(%(py10)s\n{%(py10)s = %(py8)s()\n})\n} * %(py15)s\n{%(py15)s = %(py13)s.fake_experiments_per_subject\n})', ), (@py_assert4, @py_assert16)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py13':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format19 = 'assert %(py18)s' % {'py18': @py_format17}
        raise AssertionError(@pytest_ar._format_explanation(@py_format19))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert9 = @py_assert11 = @py_assert14 = @py_assert16 = None


def test_max_multi_processing(Subject, Experiment):
    random.seed('populate')
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    @py_assert1 = Experiment()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('table already filled?') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    Experiment.populate(processes=None)
    @py_assert2 = Experiment()
    @py_assert4 = len(@py_assert2)
    @py_assert9 = Subject()
    @py_assert11 = len(@py_assert9)
    @py_assert14 = Experiment.fake_experiments_per_subject
    @py_assert16 = @py_assert11 * @py_assert14
    @py_assert6 = @py_assert4 == @py_assert16
    if not @py_assert6:
        @py_format17 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == (%(py12)s\n{%(py12)s = %(py7)s(%(py10)s\n{%(py10)s = %(py8)s()\n})\n} * %(py15)s\n{%(py15)s = %(py13)s.fake_experiments_per_subject\n})', ), (@py_assert4, @py_assert16)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py13':@pytest_ar._saferepr(Experiment) if 'Experiment' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Experiment) else 'Experiment',  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format19 = 'assert %(py18)s' % {'py18': @py_format17}
        raise AssertionError(@pytest_ar._format_explanation(@py_format19))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert9 = @py_assert11 = @py_assert14 = @py_assert16 = None


def test_allow_insert(Subject, Experiment):
    @py_assert1 = Subject()
    if not @py_assert1:
        @py_format3 = (@pytest_ar._format_assertmsg('root tables are empty') + '\n>assert %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert1 = None
    key = Subject().fetch('KEY')[0]
    key['experiment_id'] = 1001
    key['experiment_date'] = '2018-10-30'
    with pytest.raises(DataJointError):
        Experiment.insert1(key)


@pytest.fixture
def schema_load_deps(connection_test):
    schema_load_deps = dj.Schema((PREFIX + '_load_dependencies_populate'),
      connection=connection_test)
    yield schema_load_deps
    schema_load_deps.drop()


@pytest.fixture
def ImageSource(schema_load_deps):

    @schema_load_deps
    class ImageSource(dj.Lookup):
        definition = '\n        image_source_id: int\n        '
        contents = [(0, )]

    yield ImageSource
    ImageSource.drop()


@pytest.fixture
def Image(schema_load_deps, ImageSource):

    @schema_load_deps
    class Image(dj.Imported):
        definition = '\n        -> ImageSource\n        ---\n        image_data: longblob\n        '

        def make(self, key):
            self.insert1(dict(key, image_data=(dict())))

    yield Image
    Image.drop()


@pytest.fixture
def Crop(schema_load_deps, Image):

    @schema_load_deps
    class Crop(dj.Computed):
        definition = '\n        -> Image\n        ---\n        crop_image: longblob\n        '

        def make(self, key):
            self.insert1(dict(key, crop_image=(dict())))

    yield Crop
    Crop.drop()


def test_load_dependencies(Image, Crop):
    Image.populate()
    Crop.populate()