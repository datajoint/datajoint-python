# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_alter.py
# Compiled at: 2023-02-17 13:52:50
# Size of source mod 2**32: 1275 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from . import connection_root, connection_test
from schemas.default import schema, Subject, User, Experiment

def test_alter(schema, Subject, User, Experiment):
    Experiment.definition1 = '  # Experiment\n    -> Subject\n    experiment_id  :smallint  # experiment number for this subject\n    ---\n    data_path     : int  # some number\n    extra=null : longblob  # just testing\n    -> [nullable] User\n    subject_notes=null         :varchar(2048) # {notes} e.g. purpose of experiment\n    entry_time=CURRENT_TIMESTAMP :timestamp   # automatic timestamp\n    '
    Experiment.original_definition = Experiment.definition
    original = schema.connection.query('SHOW CREATE TABLE ' + Experiment.full_table_name).fetchone()[1]
    Experiment.definition = Experiment.definition1
    Experiment.alter(prompt=False)
    altered = schema.connection.query('SHOW CREATE TABLE ' + Experiment.full_table_name).fetchone()[1]
    @py_assert1 = original != altered
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('!=', ), (@py_assert1,), ('%(py0)s != %(py2)s', ), (original, altered)) % {'py0':@pytest_ar._saferepr(original) if 'original' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original) else 'original',  'py2':@pytest_ar._saferepr(altered) if 'altered' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(altered) else 'altered'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    Experiment.definition = Experiment.original_definition
    Experiment().alter(prompt=False)
    restored = schema.connection.query('SHOW CREATE TABLE ' + Experiment.full_table_name).fetchone()[1]
    @py_assert1 = altered != restored
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('!=', ), (@py_assert1,), ('%(py0)s != %(py2)s', ), (altered, restored)) % {'py0':@pytest_ar._saferepr(altered) if 'altered' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(altered) else 'altered',  'py2':@pytest_ar._saferepr(restored) if 'restored' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restored) else 'restored'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    @py_assert1 = original == restored
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (original, restored)) % {'py0':@pytest_ar._saferepr(original) if 'original' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original) else 'original',  'py2':@pytest_ar._saferepr(restored) if 'restored' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restored) else 'restored'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None