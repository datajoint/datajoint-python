# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_jobs.py
# Compiled at: 2023-02-20 21:04:29
# Size of source mod 2**32: 5081 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
from datajoint.jobs import ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX
import string, random
from . import connection_root, connection_test
from schemas.default import schema, Subject, SigIntTable, SimpleSource, SigTermTable, ErrorClass, DjExceptionName

def test_reserve_job(schema, Subject):
    subjects = Subject()
    schema.jobs.delete()
    if not subjects:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(subjects) if ('subjects' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(subjects)) else 'subjects'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    table_name = 'fake_table'
    for key in subjects.fetch('KEY'):
        @py_assert1 = schema.jobs
        @py_assert3 = @py_assert1.reserve
        @py_assert7 = @py_assert3(table_name, key)
        if not @py_assert7:
            @py_format9 = (@pytest_ar._format_assertmsg('failed to reserve a job') + '\n>assert %(py8)s\n{%(py8)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.jobs\n}.reserve\n}(%(py5)s, %(py6)s)\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py5':@pytest_ar._saferepr(table_name) if 'table_name' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table_name) else 'table_name',  'py6':@pytest_ar._saferepr(key) if 'key' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key) else 'key',  'py8':@pytest_ar._saferepr(@py_assert7)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format9))
        else:
            @py_assert1 = @py_assert3 = @py_assert7 = None

    for key in subjects.fetch('KEY'):
        @py_assert1 = schema.jobs
        @py_assert3 = @py_assert1.reserve
        @py_assert7 = @py_assert3(table_name, key)
        @py_assert9 = not @py_assert7
        if not @py_assert9:
            @py_format10 = (@pytest_ar._format_assertmsg('failed to respect reservation') + '\n>assert not %(py8)s\n{%(py8)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.jobs\n}.reserve\n}(%(py5)s, %(py6)s)\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py5':@pytest_ar._saferepr(table_name) if 'table_name' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table_name) else 'table_name',  'py6':@pytest_ar._saferepr(key) if 'key' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key) else 'key',  'py8':@pytest_ar._saferepr(@py_assert7)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format10))
        else:
            @py_assert1 = @py_assert3 = @py_assert7 = @py_assert9 = None

    for key in subjects.fetch('KEY'):
        schema.jobs.complete(table_name, key)

    @py_assert1 = schema.jobs
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('failed to free jobs') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s.jobs\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None
    for key in subjects.fetch('KEY'):
        @py_assert1 = schema.jobs
        @py_assert3 = @py_assert1.reserve
        @py_assert7 = @py_assert3(table_name, key)
        if not @py_assert7:
            @py_format9 = (@pytest_ar._format_assertmsg('failed to reserve new jobs') + '\n>assert %(py8)s\n{%(py8)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.jobs\n}.reserve\n}(%(py5)s, %(py6)s)\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py5':@pytest_ar._saferepr(table_name) if 'table_name' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table_name) else 'table_name',  'py6':@pytest_ar._saferepr(key) if 'key' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key) else 'key',  'py8':@pytest_ar._saferepr(@py_assert7)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format9))
        else:
            @py_assert1 = @py_assert3 = @py_assert7 = None

    for key in subjects.fetch('KEY'):
        schema.jobs.error(table_name, key, 'error message')

    for key in subjects.fetch('KEY'):
        @py_assert1 = schema.jobs
        @py_assert3 = @py_assert1.reserve
        @py_assert7 = @py_assert3(table_name, key)
        @py_assert9 = not @py_assert7
        if not @py_assert9:
            @py_format10 = (@pytest_ar._format_assertmsg('failed to ignore error jobs') + '\n>assert not %(py8)s\n{%(py8)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.jobs\n}.reserve\n}(%(py5)s, %(py6)s)\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py5':@pytest_ar._saferepr(table_name) if 'table_name' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table_name) else 'table_name',  'py6':@pytest_ar._saferepr(key) if 'key' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key) else 'key',  'py8':@pytest_ar._saferepr(@py_assert7)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format10))
        else:
            @py_assert1 = @py_assert3 = @py_assert7 = @py_assert9 = None

    (schema.jobs & dict(status='error')).delete()
    @py_assert1 = schema.jobs
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('failed to clear error jobs') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s.jobs\n}') % {'py0':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None


def test_restrictions(schema):
    jobs = schema.jobs
    jobs.delete()
    jobs.reserve('a', {'key': 'a1'})
    jobs.reserve('a', {'key': 'a2'})
    jobs.reserve('b', {'key': 'b1'})
    jobs.error('a', {'key': 'a2'}, 'error')
    jobs.error('b', {'key': 'b1'}, 'error')
    @py_assert2 = {'table_name': 'a'}
    @py_assert4 = jobs & @py_assert2
    @py_assert5 = len(@py_assert4)
    @py_assert8 = 2
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s((%(py1)s & %(py3)s))\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(jobs) if 'jobs' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(jobs) else 'jobs',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert8 = None
    @py_assert2 = {'status': 'error'}
    @py_assert4 = jobs & @py_assert2
    @py_assert5 = len(@py_assert4)
    @py_assert8 = 2
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s((%(py1)s & %(py3)s))\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(jobs) if 'jobs' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(jobs) else 'jobs',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert8 = None
    @py_assert2 = {'table_name':'a',  'status':'error'}
    @py_assert4 = jobs & @py_assert2
    @py_assert5 = len(@py_assert4)
    @py_assert8 = 1
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s((%(py1)s & %(py3)s))\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(jobs) if 'jobs' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(jobs) else 'jobs',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert8 = None
    jobs.delete()


def test_sigint(schema, SigIntTable):
    schema.jobs.delete()
    try:
        SigIntTable().populate(reserve_jobs=True)
    except KeyboardInterrupt:
        pass

    status, error_message = schema.jobs.fetch1('status', 'error_message')
    @py_assert2 = 'error'
    @py_assert1 = status == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (status, @py_assert2)) % {'py0':@pytest_ar._saferepr(status) if 'status' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(status) else 'status',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    @py_assert2 = 'KeyboardInterrupt'
    @py_assert1 = error_message == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (error_message, @py_assert2)) % {'py0':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    schema.jobs.delete()


def test_sigterm(schema, SigTermTable):
    schema.jobs.delete()
    try:
        SigTermTable().populate(reserve_jobs=True)
    except SystemExit:
        pass

    status, error_message = schema.jobs.fetch1('status', 'error_message')
    @py_assert2 = 'error'
    @py_assert1 = status == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (status, @py_assert2)) % {'py0':@pytest_ar._saferepr(status) if 'status' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(status) else 'status',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    @py_assert2 = 'SystemExit: SIGTERM received'
    @py_assert1 = error_message == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (error_message, @py_assert2)) % {'py0':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    schema.jobs.delete()


def test_suppress_dj_errors(schema, ErrorClass, DjExceptionName):
    """test_suppress_dj_errors: dj errors suppressible w/o native py blobs"""
    schema.jobs.delete()
    ErrorClass.populate(reserve_jobs=True, suppress_errors=True)
    @py_assert2 = DjExceptionName()
    @py_assert4 = len(@py_assert2)
    @py_assert10 = schema.jobs
    @py_assert12 = len(@py_assert10)
    @py_assert6 = @py_assert4 == @py_assert12
    @py_assert14 = 0
    @py_assert7 = @py_assert12 > @py_assert14
    if not (@py_assert6 and @py_assert7):
        @py_format16 = @pytest_ar._call_reprcompare(('==', '>'), (@py_assert6, @py_assert7), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == %(py13)s\n{%(py13)s = %(py8)s(%(py11)s\n{%(py11)s = %(py9)s.jobs\n})\n}',
                                                                                              '%(py13)s\n{%(py13)s = %(py8)s(%(py11)s\n{%(py11)s = %(py9)s.jobs\n})\n} > %(py15)s'), (@py_assert4, @py_assert12, @py_assert14)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(DjExceptionName) if 'DjExceptionName' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(DjExceptionName) else 'DjExceptionName',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py9':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format18 = 'assert %(py17)s' % {'py17': @py_format16}
        raise AssertionError(@pytest_ar._format_explanation(@py_format18))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert7 = @py_assert10 = @py_assert12 = @py_assert14 = None


def test_long_error_message(schema, Subject):
    random.seed('jobs')
    subjects = Subject()
    schema.jobs.delete()
    long_error_message = ''.join((random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100)))
    short_error_message = ''.join((random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH // 2)))
    if not subjects:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(subjects) if ('subjects' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(subjects)) else 'subjects'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    table_name = 'fake_table'
    key = subjects.fetch('KEY')[0]
    schema.jobs.reserve(table_name, key)
    schema.jobs.error(table_name, key, long_error_message)
    error_message = schema.jobs.fetch1('error_message')
    @py_assert2 = len(error_message)
    @py_assert4 = @py_assert2 == ERROR_MESSAGE_LENGTH
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, ERROR_MESSAGE_LENGTH)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(ERROR_MESSAGE_LENGTH) if 'ERROR_MESSAGE_LENGTH' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ERROR_MESSAGE_LENGTH) else 'ERROR_MESSAGE_LENGTH'}
        @py_format8 = (@pytest_ar._format_assertmsg('error message is longer than max allowed') + '\n>assert %(py7)s') % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    @py_assert1 = error_message.endswith
    @py_assert4 = @py_assert1(TRUNCATION_APPENDIX)
    if not @py_assert4:
        @py_format6 = (@pytest_ar._format_assertmsg('appropriate ending missing for truncated error message') + '\n>assert %(py5)s\n{%(py5)s = %(py2)s\n{%(py2)s = %(py0)s.endswith\n}(%(py3)s)\n}') % {'py0':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(TRUNCATION_APPENDIX) if 'TRUNCATION_APPENDIX' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TRUNCATION_APPENDIX) else 'TRUNCATION_APPENDIX',  'py5':@pytest_ar._saferepr(@py_assert4)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert4 = None
    schema.jobs.delete()
    schema.jobs.reserve(table_name, key)
    schema.jobs.error(table_name, key, short_error_message)
    error_message = schema.jobs.fetch1('error_message')
    @py_assert1 = error_message == short_error_message
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (error_message, short_error_message)) % {'py0':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py2':@pytest_ar._saferepr(short_error_message) if 'short_error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(short_error_message) else 'short_error_message'}
        @py_format5 = (@pytest_ar._format_assertmsg('error messages do not agree') + '\n>assert %(py4)s') % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    @py_assert1 = error_message.endswith
    @py_assert4 = @py_assert1(TRUNCATION_APPENDIX)
    @py_assert6 = not @py_assert4
    if not @py_assert6:
        @py_format7 = (@pytest_ar._format_assertmsg('error message should not be truncated') + '\n>assert not %(py5)s\n{%(py5)s = %(py2)s\n{%(py2)s = %(py0)s.endswith\n}(%(py3)s)\n}') % {'py0':@pytest_ar._saferepr(error_message) if 'error_message' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_message) else 'error_message',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(TRUNCATION_APPENDIX) if 'TRUNCATION_APPENDIX' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(TRUNCATION_APPENDIX) else 'TRUNCATION_APPENDIX',  'py5':@pytest_ar._saferepr(@py_assert4)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert1 = @py_assert4 = @py_assert6 = None
    schema.jobs.delete()


def test_long_error_stack(schema, Subject):
    random.seed('jobs')
    subjects = Subject()
    schema.jobs.delete()
    STACK_SIZE = 89942
    long_error_stack = ''.join((random.choice(string.ascii_letters) for _ in range(STACK_SIZE)))
    if not subjects:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(subjects) if ('subjects' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(subjects)) else 'subjects'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    table_name = 'fake_table'
    key = subjects.fetch('KEY')[0]
    schema.jobs.reserve(table_name, key)
    schema.jobs.error(table_name, key, 'error message', long_error_stack)
    error_stack = schema.jobs.fetch1('error_stack')
    @py_assert1 = error_stack == long_error_stack
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (error_stack, long_error_stack)) % {'py0':@pytest_ar._saferepr(error_stack) if 'error_stack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(error_stack) else 'error_stack',  'py2':@pytest_ar._saferepr(long_error_stack) if 'long_error_stack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(long_error_stack) else 'long_error_stack'}
        @py_format5 = (@pytest_ar._format_assertmsg('error stacks do not agree') + '\n>assert %(py4)s') % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    schema.jobs.delete()