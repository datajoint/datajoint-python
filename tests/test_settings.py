import os

__author__ = 'Fabian Sinz'

from nose.tools import assert_true, assert_raises, assert_equal, raises
import datajoint as dj


def test_load_save():
    dj.config.save('tmp.json')
    conf = dj.Config()
    conf.load('tmp.json')
    assert_true(conf == dj.config, 'Two config files do not match.')
    os.remove('tmp.json')

@raises(ValueError)
def test_nested_check():
    dummy = {'dummy.testval': {'notallowed': 2}}
    dj.config.update(dummy)

@raises(ValueError)
def test_nested_check2():
    dj.config['dummy'] = {'dummy2':2}

