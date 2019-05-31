from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
import datajoint as dj
from . import CONN_INFO
# import pymysql


class TestSSL:

    @staticmethod
    def test_secure_connection():
        pass
        # result = dj.conn(CONN_INFO['host'], user='djssl', password='djssl',reset=True).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
        # print('result is: {}'.format(result))

    @staticmethod
    def test_insecure_connection():
        pass
        # result = dj.conn(**CONN_INFO, ssl=False, reset=True).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
        # print('result is: {}'.format(result)) 

    @staticmethod
    # @raises(pymysql.err.OperationalError)
    def test_reject_insecure():
        pass
        # result = dj.conn(CONN_INFO['host'], user='djssl', password='djssl', ssl=False, reset=True).query("SHOW STATUS LIKE 'Ssl_cipher';").fetchone()[1]
        # print('result is: {}'.format(result)) 

    @staticmethod
    def test_fallback():
        # show variables like ‘%ssl%’;
        pass   