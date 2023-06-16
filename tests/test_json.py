# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_json.py
# Compiled at: 2023-02-20 16:46:34
# Size of source mod 2**32: 7594 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
import datajoint.declare as declare
from packaging import version
import numpy as np
from numpy.testing import assert_array_equal
import pytest
from . import PREFIX, connection_root, connection_test
if version.parse(dj.conn().query('select @@version;').fetchone()[0]) >= version.parse('8.0.0'):

    @pytest.fixture
    def schema(connection_test):
        schema = dj.Schema((PREFIX + '_json'), connection=connection_test)
        yield schema
        schema.drop()


    @pytest.fixture
    def Team(schema):

        @schema
        class Team(dj.Lookup):
            definition = "\n            name: varchar(40)\n            ---\n            car=null: json\n            unique index(car.name:char(20))\n            uniQue inDex ( name, car.name:char(20), (json_value(`car`, _utf8mb4'$.length' returning decimal(4, 1))) )\n            "
            contents = [
             (
              'engineering',
              {'name':'Rever', 
               'length':20.5, 
               'inspected':True, 
               'tire_pressure':[
                32, 31, 33, 34], 
               'headlights':[
                {'side':'left', 
                 'hyper_white':None},
                {'side':'right', 
                 'hyper_white':None}]}),
             (
              'business',
              {'name':'Chaching', 
               'length':100, 
               'safety_inspected':False, 
               'tire_pressure':[
                34, 30, 27, 32], 
               'headlights':[
                {'side':'left', 
                 'hyper_white':True},
                {'side':'right', 
                 'hyper_white':True}]}),
             ('marketing', None)]

        yield Team
        Team.drop()


    def test_insert_update(Team):
        car = {'name':'Discovery', 
         'length':22.9, 
         'inspected':None, 
         'tire_pressure':[
          35, 36, 34, 37], 
         'headlights':[
          {'side':'left', 
           'hyper_white':True},
          {'side':'right', 
           'hyper_white':True}]}
        Team.insert1({'name':'research',  'car':car})
        q = Team & {'name': 'research'}
        @py_assert1 = q.fetch1
        @py_assert3 = 'car'
        @py_assert5 = @py_assert1(@py_assert3)
        @py_assert7 = @py_assert5 == car
        if not @py_assert7:
            @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.fetch1\n}(%(py4)s)\n} == %(py8)s', ), (@py_assert5, car)) % {'py0':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(car) if 'car' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(car) else 'car'}
            @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
            raise AssertionError(@pytest_ar._format_explanation(@py_format11))
        @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
        car.update({'length': 23})
        Team.update1({'name':'research',  'car':car})
        @py_assert1 = q.fetch1
        @py_assert3 = 'car'
        @py_assert5 = @py_assert1(@py_assert3)
        @py_assert7 = @py_assert5 == car
        if not @py_assert7:
            @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.fetch1\n}(%(py4)s)\n} == %(py8)s', ), (@py_assert5, car)) % {'py0':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(car) if 'car' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(car) else 'car'}
            @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
            raise AssertionError(@pytest_ar._format_explanation(@py_format11))
        @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
        try:
            Team.insert1({'name':'hr',  'car':car})
            raise Exception('Inserted non-unique car name.')
        except dj.DataJointError:
            pass

        q.delete_quick()
        @py_assert1 = not q
        if not @py_assert1:
            @py_format2 = 'assert not %(py0)s' % {'py0': @pytest_ar._saferepr(q) if ('q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q)) else 'q'}
            raise AssertionError(@pytest_ar._format_explanation(@py_format2))
        @py_assert1 = None


    def test_describe(Team):
        rel = Team()
        context = locals()
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        @py_assert1 = s1 == s2
        if not @py_assert1:
            @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (s1, s2)) % {'py0':@pytest_ar._saferepr(s1) if 's1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s1) else 's1',  'py2':@pytest_ar._saferepr(s2) if 's2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(s2) else 's2'}
            @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
            raise AssertionError(@pytest_ar._format_explanation(@py_format5))
        @py_assert1 = None


    def test_restrict(Team):
        @py_assert1 = {'car.name': 'Chaching'}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.length': 20.5}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'engineering'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.inspected': 'true'}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'engineering'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.inspected:unsigned': True}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'engineering'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.safety_inspected': 'false'}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.safety_inspected:unsigned': False}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.headlights[0].hyper_white': None}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch
        @py_assert6 = 'name'
        @py_assert8 = 'name'
        @py_assert10 = True
        @py_assert12 = @py_assert4(@py_assert6, order_by=@py_assert8, as_dict=@py_assert10)
        @py_assert15 = [
         {'name': 'engineering'}, {'name': 'marketing'}]
        @py_assert14 = @py_assert12 == @py_assert15
        if not @py_assert14:
            @py_format17 = @pytest_ar._call_reprcompare(('==', ), (@py_assert14,), ('%(py13)s\n{%(py13)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch\n}(%(py7)s, order_by=%(py9)s, as_dict=%(py11)s)\n} == %(py16)s', ), (@py_assert12, @py_assert15)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py16':@pytest_ar._saferepr(@py_assert15)}
            @py_format19 = 'assert %(py18)s' % {'py18': @py_format17}
            raise AssertionError(@pytest_ar._format_explanation(@py_format19))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = @py_assert15 = None
        @py_assert1 = {'car': None}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'marketing'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.tire_pressure': [34, 30, 27, 32]}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = {'car.headlights[1]': {'side':'right',  'hyper_white':True}}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "`car`->>'$.name' LIKE '%ching%'"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('Missing substring') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "`car`->>'$.length' > 30"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('<= 30') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "JSON_VALUE(`car`, '$.safety_inspected' RETURNING UNSIGNED) = 0"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('Has `safety_inspected` set to `true`') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "`car`->>'$.headlights[0].hyper_white' = 'null'"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'engineering'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('Has 1st `headlight` with `hyper_white` not set to `null`') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "`car`->>'$.inspected' IS NOT NULL"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'engineering'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('Missing `inspected` key') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = "`car`->>'$.tire_pressure' = '[34, 30, 27, 32]'"
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('`tire_pressure` array did not match') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
        @py_assert1 = '`car`->>\'$.headlights[1]\' = \'{"side": "right", "hyper_white": true}\''
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.fetch1
        @py_assert6 = 'name'
        @py_assert8 = @py_assert4(@py_assert6)
        @py_assert11 = 'business'
        @py_assert10 = @py_assert8 == @py_assert11
        if not @py_assert10:
            @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_format15 = (@pytest_ar._format_assertmsg('2nd `headlight` object did not match') + '\n>assert %(py14)s') % {'py14': @py_format13}
            raise AssertionError(@pytest_ar._format_explanation(@py_format15))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None


    def test_proj(Team):
        @py_assert1 = Team.proj
        @py_assert3 = 'car.length'
        @py_assert5 = @py_assert1(car_length=@py_assert3)
        @py_assert7 = @py_assert5.fetch
        @py_assert9 = True
        @py_assert11 = 'car_length'
        @py_assert13 = @py_assert7(as_dict=@py_assert9, order_by=@py_assert11)
        @py_assert16 = [
         {'name':'marketing', 
          'car_length':None}, {'name':'business',  'car_length':'100'}, {'name':'engineering',  'car_length':'20.5'}]
        @py_assert15 = @py_assert13 == @py_assert16
        if not @py_assert15:
            @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert15,), ('%(py14)s\n{%(py14)s = %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.proj\n}(car_length=%(py4)s)\n}.fetch\n}(as_dict=%(py10)s, order_by=%(py12)s)\n} == %(py17)s', ), (@py_assert13, @py_assert16)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py17':@pytest_ar._saferepr(@py_assert16)}
            @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
            raise AssertionError(@pytest_ar._format_explanation(@py_format20))
        @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = @py_assert13 = @py_assert15 = @py_assert16 = None
        @py_assert1 = Team.proj
        @py_assert3 = 'car.length:decimal(4, 1)'
        @py_assert5 = @py_assert1(car_length=@py_assert3)
        @py_assert7 = @py_assert5.fetch
        @py_assert9 = True
        @py_assert11 = 'car_length'
        @py_assert13 = @py_assert7(as_dict=@py_assert9, order_by=@py_assert11)
        @py_assert16 = [
         {'name':'marketing', 
          'car_length':None}, {'name':'engineering',  'car_length':20.5}, {'name':'business',  'car_length':100.0}]
        @py_assert15 = @py_assert13 == @py_assert16
        if not @py_assert15:
            @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert15,), ('%(py14)s\n{%(py14)s = %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.proj\n}(car_length=%(py4)s)\n}.fetch\n}(as_dict=%(py10)s, order_by=%(py12)s)\n} == %(py17)s', ), (@py_assert13, @py_assert16)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py17':@pytest_ar._saferepr(@py_assert16)}
            @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
            raise AssertionError(@pytest_ar._format_explanation(@py_format20))
        @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = @py_assert13 = @py_assert15 = @py_assert16 = None
        @py_assert1 = Team.proj
        @py_assert3 = "JSON_VALUE(`car`, '$.length' RETURNING float) - 15"
        @py_assert5 = @py_assert1(car_width=@py_assert3)
        @py_assert7 = @py_assert5.fetch
        @py_assert9 = True
        @py_assert11 = 'car_width'
        @py_assert13 = @py_assert7(as_dict=@py_assert9, order_by=@py_assert11)
        @py_assert16 = [
         {'name':'marketing', 
          'car_width':None}, {'name':'engineering',  'car_width':5.5}, {'name':'business',  'car_width':85.0}]
        @py_assert15 = @py_assert13 == @py_assert16
        if not @py_assert15:
            @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert15,), ('%(py14)s\n{%(py14)s = %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.proj\n}(car_width=%(py4)s)\n}.fetch\n}(as_dict=%(py10)s, order_by=%(py12)s)\n} == %(py17)s', ), (@py_assert13, @py_assert16)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py17':@pytest_ar._saferepr(@py_assert16)}
            @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
            raise AssertionError(@pytest_ar._format_explanation(@py_format20))
        @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = @py_assert13 = @py_assert15 = @py_assert16 = None
        @py_assert1 = {'name': 'engineering'}
        @py_assert3 = Team & @py_assert1
        @py_assert4 = @py_assert3.proj
        @py_assert6 = 'car.tire_pressure'
        @py_assert8 = @py_assert4(car_tire_pressure=@py_assert6)
        @py_assert10 = @py_assert8.fetch1
        @py_assert12 = 'car_tire_pressure'
        @py_assert14 = @py_assert10(@py_assert12)
        @py_assert17 = '[32, 31, 33, 34]'
        @py_assert16 = @py_assert14 == @py_assert17
        if not @py_assert16:
            @py_format19 = @pytest_ar._call_reprcompare(('==', ), (@py_assert16,), ('%(py15)s\n{%(py15)s = %(py11)s\n{%(py11)s = %(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).proj\n}(car_tire_pressure=%(py7)s)\n}.fetch1\n}(%(py13)s)\n} == %(py18)s', ), (@py_assert14, @py_assert17)) % {'py0':@pytest_ar._saferepr(Team) if 'Team' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Team) else 'Team',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14),  'py18':@pytest_ar._saferepr(@py_assert17)}
            @py_format21 = 'assert %(py20)s' % {'py20': @py_format19}
            raise AssertionError(@pytest_ar._format_explanation(@py_format21))
        @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = @py_assert16 = @py_assert17 = None
        assert_array_equal(Team.proj(car_inspected='car.inspected').fetch('car_inspected',
          order_by='name'), np.array([None, 'true', None]))
        assert_array_equal(Team.proj(car_inspected='car.inspected:unsigned').fetch('car_inspected',
          order_by='name'), np.array([None, 1, None]))