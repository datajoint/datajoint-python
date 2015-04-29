import os

__author__ = 'Fabian Sinz'

from nose.tools import assert_true, assert_raises, assert_equal
import datajoint as dj

def nested_dict_compare(d1, d2):
    for k, v in d1.items():
        if k not in d2:
            return False
        else:
            if isinstance(v, dict):
                tmp = nested_dict_compare(v, d2[k])
                if not tmp: return False
            else:
                if not v == d2[k]: return False
    else:
        return True

def test_load_save():
    old = dj.config['settings']['local config file']
    dj.config['settings']['local config file'] = 'tmp.json'
    dj.config.save()
    conf = dj.Config()
    conf.load('tmp.json')
    assert_true(nested_dict_compare(conf, dj.config), 'Two config files do not match.')
    dj.config['settings']['local config file'] = old
    os.remove('tmp.json')



