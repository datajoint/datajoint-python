import datajoint.hub as hub
import datajoint as dj
from nose.tools import assert_equal, raises


def test_normal_host():
    assert_equal(hub.get_host('1.2.3.4'), '1.2.3.4')
    assert_equal(hub.get_host('1.2.3.4:5678'), '1.2.3.4:5678')
    assert_equal(hub.get_host('Ever.Green_Bear-Creek'), 'Ever.Green_Bear-Creek')
    assert_equal(hub.get_host('Ever.Green_Bear-Creek:1234'), 'Ever.Green_Bear-Creek:1234')


def test_hub_host():
    assert_equal(hub.get_host('hub://fakeservices.datajoint.io/datajoint/travis'),
        'fakeservices.datajoint.io:3306')


@raises(dj.DataJointError)
def test_hub_missing_project():
    hub.get_host('hub://fakeservices.datajoint.io/datajoint/test')


@raises(dj.DataJointError)
def test_hub_no_tls():
    hub.get_host('hub://fakeservices.datajoint.io:4000/datajoint/travis')


@raises(dj.DataJointError)
def test_hub_incorrect_protocol():
    hub.get_host('djhub://datajoint/travis')


@raises(dj.DataJointError)
def test_hub_unreachable_server():
    hub.get_host('hub://fakeservices.datajoint.io:4001/datajoint/travis')


@raises(dj.DataJointError)
def test_hub_unreachable_endpoint():
    current = hub.API_TARGETS
    hub.API_TARGETS = {'PROJECT': '/wrong_one'}
    try:
        hub.get_host('hub://fakeservices.datajoint.io/datajoint/travis')
    except:
        hub.API_TARGETS = current
        raise
