import os
import pprint
import random
import string
from datajoint import settings

__author__ = 'Fabian Sinz'

from nose.tools import assert_true, assert_raises, assert_equal, raises, assert_dict_equal
import datajoint as dj


def test_load_save():
    dj.config.save('tmp.json')
    conf = dj.Config()
    conf.load('tmp.json')
    assert_true(conf == dj.config, 'Two config files do not match.')
    os.remove('tmp.json')

def test_singleton():
    dj.config.save('tmp.json')
    conf = dj.Config()
    conf.load('tmp.json')
    conf['dummy.val'] = 2

    assert_true(conf == dj.config, 'Config does not behave like a singleton.')
    os.remove('tmp.json')


@raises(ValueError)
def test_nested_check():
    dummy = {'dummy.testval': {'notallowed': 2}}
    dj.config.update(dummy)

@raises(dj.DataJointError)
def test_validator():
    dj.config['database.port'] = 'harbor'

def test_del():
    dj.config['peter'] = 2
    assert_true('peter' in dj.config)
    del dj.config['peter']
    assert_true('peter' not in dj.config)

def test_len():
    assert_equal(len(dj.config), len(dj.config._conf))

def test_str():
    assert_equal(str(dj.config), pprint.pformat(dj.config._conf, indent=4))

def test_repr():
    assert_equal(repr(dj.config), pprint.pformat(dj.config._conf, indent=4))

@raises(ValueError)
def test_nested_check2():
    dj.config['dummy'] = {'dummy2':2}

def test_save():
    tmpfile = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
    moved = False
    if os.path.isfile(settings.LOCALCONFIG):
        os.rename(settings.LOCALCONFIG, tmpfile)
        moved = True
    dj.config.save()
    assert_true(os.path.isfile(settings.LOCALCONFIG))
    if moved:
        os.rename(tmpfile, settings.LOCALCONFIG)
