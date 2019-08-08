from nose.tools import assert_true, assert_false, assert_equal, \
                        assert_list_equal, raises

import datajoint as dj
import os
import re
from . import S3_CONN_INFO
from . import CONN_INFO


class TestBlobMigrate:

    @staticmethod
    def test_convert():

        schema = dj.schema('djtest_blob_migrate')
        query = schema.connection.query

        # Configure stores
        default_store = 'external'  # naming the unnamed external store
        dj.config['stores'] = {
            default_store: dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket='migrate-test',
                location='store',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'shared': dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket='migrate-test',
                location='maps',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'local': dict(
                protocol='file',
                location=os.path.expanduser('~/temp/migrate-test'))
        }
        dj.config['cache'] = os.path.expanduser('~/temp/dj-cache')

        LEGACY_HASH_SIZE = 43

        legacy_external = dj.FreeTable(
            schema.connection,
            '`{db}`.`~external`'.format(db=schema.database))

        # get referencing tables
        refs = query("""
        SELECT concat('`', table_schema, '`.`', table_name, '`')
             as referencing_table, column_name, constraint_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(
            tab=legacy_external.table_name,
            db=legacy_external.database), as_dict=True).fetchall()

        for ref in refs:
            # get comment
            column = query(
                'SHOW FULL COLUMNS FROM {referencing_table}'
                'WHERE Field="{column_name}"'.format(
                    **ref), as_dict=True).fetchone()

            store, comment = re.match(
                r':external(-(?P<store>.+))?:(?P<comment>.*)',
                column['Comment']).group('store', 'comment')

            # get all the hashes from the reference
            hashes = {x[0] for x in query(
                'SELECT `{column_name}` FROM {referencing_table}'.format(
                    **ref))}

            # sanity check make sure that store suffixes match
            if store is None:
                assert all(len(_) == LEGACY_HASH_SIZE for _ in hashes)
            else:
                assert all(_[LEGACY_HASH_SIZE:] == store for _ in hashes)

            # create new-style external table
            ext = schema.external[store or default_store]

            # add the new-style reference field
            temp_suffix = 'tempsub'

            try:
                query("""ALTER TABLE {referencing_table}
                 ADD COLUMN `{column_name}_{temp_suffix}` {type} DEFAULT NULL
                COMMENT ":blob@{store}:{comment}"
                """.format(
                    type=dj.declare.UUID_DATA_TYPE,
                    temp_suffix=temp_suffix,
                    store=(store or default_store), comment=comment, **ref))
            except:
                print('Column already added')
                pass

            # Copy references into the new external table
            # No Windows! Backslashes will cause problems

            contents_hash_function = {
                'file': lambda ext, relative_path: dj.hash.uuid_from_file(
                    os.path.join(ext.spec['location'], relative_path)),
                's3': lambda ext, relative_path: dj.hash.uuid_from_buffer(
                    ext.s3.get(relative_path))
            }

            for _hash, size in zip(*legacy_external.fetch('hash', 'size')):
                if _hash in hashes:
                    relative_path = os.path.join(schema.database, _hash)
                    uuid = dj.hash.uuid_from_buffer(init_string=relative_path)
                    ext.insert1(dict(
                        filepath=relative_path,
                        size=size,
                        contents_hash=contents_hash_function[ext.spec[
                            'protocol']](ext, relative_path),
                        hash=uuid
                    ), skip_duplicates=True)

                    query(
                        'UPDATE {referencing_table} '
                        'SET `{column_name}_{temp_suffix}`=%s '
                        'WHERE `{column_name}` = "{_hash}"'
                        .format(
                            _hash=_hash,
                            temp_suffix=temp_suffix, **ref), uuid.bytes)

            # check that all have been copied
            check = query(
                'SELECT * FROM {referencing_table} '
                'WHERE `{column_name}` IS NOT NULL'
                '  AND `{column_name}_{temp_suffix}` IS NULL'
                .format(temp_suffix=temp_suffix, **ref)).fetchall()

            assert len(check) == 0, 'Some hashes havent been migrated'

            # drop old foreign key, rename, and create new foreign key
            query("""
                ALTER TABLE {referencing_table}
                DROP FOREIGN KEY `{constraint_name}`,
                DROP COLUMN `{column_name}`,
                CHANGE COLUMN `{column_name}_{temp_suffix}` `{column_name}`
                 {type} DEFAULT NULL
                    COMMENT ":blob@{store}:{comment}",
                ADD FOREIGN KEY (`{column_name}`) REFERENCES {ext_table_name}
                 (`hash`)
                """.format(
                    temp_suffix=temp_suffix,
                    ext_table_name=ext.full_table_name,
                    type=dj.declare.UUID_DATA_TYPE,
                    store=(store or default_store), comment=comment, **ref))

        # Drop the old external table but make sure it's no longer referenced
        # get referencing tables
        refs = query("""
        SELECT concat('`', table_schema, '`.`', table_name, '`') as
         referencing_table, column_name, constraint_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(
            tab=legacy_external.table_name,
            db=legacy_external.database), as_dict=True).fetchall()

        assert not refs, 'Some references still exist'

        # drop old external table
        legacy_external.drop_quick()

    @staticmethod
    def test_query():

        dj.config['database.password'] = CONN_INFO['password']
        dj.config['database.user'] = CONN_INFO['user']
        dj.config['database.host'] = CONN_INFO['host']

        schema = dj.schema('djtest_blob_migrate')

        # Configure stores
        default_store = 'external'  # naming the unnamed external store
        dj.config['stores'] = {
            default_store: dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket='migrate-test',
                location='store',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'shared': dict(
                protocol='s3',
                endpoint=S3_CONN_INFO['endpoint'],
                bucket='migrate-test',
                location='maps',
                access_key=S3_CONN_INFO['access_key'],
                secret_key=S3_CONN_INFO['secret_key']),
            'local': dict(
                protocol='file',
                location=os.path.expanduser('~/temp/migrate-test'))
        }
        dj.config['cache'] = os.path.expanduser('~/temp/dj-cache')

        test_mod = dj.create_virtual_module('test_mod', 'djtest_blob_migrate')
        assert_equal(test_mod.A.fetch('blob_share')[1][1], 2)
