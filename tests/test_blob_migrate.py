from nose.tools import assert_true, assert_false, assert_equal, \
                        assert_list_equal, raises

import datajoint as dj
import os
from pathlib import Path
from . import S3_CONN_INFO, S3_MIGRATE_BUCKET
from . import CONN_INFO
from datajoint.migrate import _migrate_dj011_blob
dj.config['enable_python_native_blobs'] = True


class TestBlobMigrate:

    @staticmethod
    def test_convert():
        # Configure stores
        default_store = 'external'  # naming the unnamed external store
        dj.config['stores'] = {
            default_store: dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket=S3_MIGRATE_BUCKET,
                location='store',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'shared': dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket=S3_MIGRATE_BUCKET,
                location='maps',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'local': dict(
                protocol='file',
                location=str(Path(os.path.expanduser('~'),'temp',S3_MIGRATE_BUCKET)))
        }
        dj.config['cache'] = str(Path(os.path.expanduser('~'),'temp','dj-cache'))

        dj.config['database.password'] = CONN_INFO['password']
        dj.config['database.user'] = CONN_INFO['user']
        dj.config['database.host'] = CONN_INFO['host']
        schema = dj.Schema('djtest_blob_migrate')

        # Test if migration throws unexpected exceptions
        _migrate_dj011_blob(schema, default_store)

        # Test Fetch
        test_mod = dj.create_virtual_module('test_mod', 'djtest_blob_migrate')
        r1 = test_mod.A.fetch('blob_share', order_by='id')
        assert_equal(r1[1][1], 2)

        # Test Insert
        test_mod.A.insert1({
            'id': 3,
            'blob_external': [9, 8, 7, 6],
            'blob_share': {'number': 5}})
        r2 = (test_mod.A & 'id=3').fetch1()
        assert_equal(r2['blob_share']['number'], 5)

    @staticmethod
    @raises(ValueError)
    def test_type_check():
        dj.migrate_dj011_external_blob_storage_to_dj012(10, 'store')
