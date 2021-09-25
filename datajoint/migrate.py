import datajoint as dj
from pathlib import Path
import re
from .utils import user_choice


def migrate_dj011_external_blob_storage_to_dj012(migration_schema, store):
    """
    Utility function to migrate external blob data from 0.11 to 0.12.
    :param migration_schema: string of target schema to be migrated
    :param store: string of target dj.config['store'] to be migrated
    """
    if not isinstance(migration_schema, str):
        raise ValueError(
            'Expected type {} for migration_schema, not {}.'.format(
                str, type(migration_schema)))

    do_migration = False
    do_migration = user_choice(
            """
Warning: Ensure the following are completed before proceeding.
- Appropriate backups have been taken,
- Any existing DJ 0.11.X connections are suspended, and
- External config has been updated to new dj.config['stores'] structure.
Proceed?
            """, default='no') == 'yes'
    if do_migration:
        _migrate_dj011_blob(dj.Schema(migration_schema), store)
        print('Migration completed for schema: {}, store: {}.'.format(
                migration_schema, store))
        return
    print('No migration performed.')


def _migrate_dj011_blob(schema, default_store):
    query = schema.connection.query

    LEGACY_HASH_SIZE = 43

    legacy_external = dj.FreeTable(
        schema.connection,
        '`{db}`.`~external`'.format(db=schema.database))

    # get referencing tables
    refs = [{k.lower(): v for k, v in elem.items()} for elem in query("""
    SELECT concat('`', table_schema, '`.`', table_name, '`')
            as referencing_table, column_name, constraint_name
    FROM information_schema.key_column_usage
    WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
    """.format(
        tab=legacy_external.table_name,
        db=legacy_external.database), as_dict=True).fetchall()]

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

        for _hash, size in zip(*legacy_external.fetch('hash', 'size')):
            if _hash in hashes:
                relative_path = str(Path(schema.database, _hash).as_posix())
                uuid = dj.hash.uuid_from_buffer(init_string=relative_path)
                external_path = ext._make_external_filepath(relative_path)
                if ext.spec['protocol'] == 's3':
                    contents_hash = dj.hash.uuid_from_buffer(ext._download_buffer(external_path))
                else:
                    contents_hash = dj.hash.uuid_from_file(external_path)
                ext.insert1(dict(
                    filepath=relative_path,
                    size=size,
                    contents_hash=contents_hash,
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
    refs = [{k.lower(): v for k, v in elem.items()} for elem in query("""
    SELECT concat('`', table_schema, '`.`', table_name, '`') as
        referencing_table, column_name, constraint_name
    FROM information_schema.key_column_usage
    WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
    """.format(
        tab=legacy_external.table_name,
        db=legacy_external.database), as_dict=True).fetchall()]

    assert not refs, 'Some references still exist'

    # drop old external table
    legacy_external.drop_quick()
