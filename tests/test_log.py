from nose.tools import assert_true
from . import schema


def test_log():
    ts, events = (schema.schema.log & 'event like "Declared%%"').fetch('timestamp', 'event')
    assert_true(len(ts) >= 2)
