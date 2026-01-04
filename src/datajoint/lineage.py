"""
Lineage management for semantic matching in DataJoint.

Lineage identifies the origin of an attribute - where it was first defined.
It is represented as a string in the format: schema_name.table_name.attribute_name

Semantic matching is applied to all binary operations that match attributes by name:
- Join (A * B): matches on homologous namesakes
- Restriction (A & B, A - B): matches on homologous namesakes
- Aggregation (A.aggr(B, ...)): requires homologous namesakes for grouping
- Union (A + B): requires all namesakes to have matching lineage

If namesake attributes have different lineages (including either being None),
a DataJointError is raised.

If the ~lineage table doesn't exist for a schema, a warning is issued and
semantic checking is disabled for operations involving that schema.

The ~lineage table stores lineage information for each schema, populated at table
declaration time. Use schema.rebuild_lineage() to restore lineage for legacy schemas.
"""

import logging

from .errors import DataJointError

logger = logging.getLogger(__name__.split(".")[0])


def ensure_lineage_table(connection, database):
    """
    Create the ~lineage table in the schema if it doesn't exist.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.
    """
    connection.query(
        """
        CREATE TABLE IF NOT EXISTS `{database}`.`~lineage` (
            table_name VARCHAR(64) NOT NULL COMMENT 'table name within the schema',
            attribute_name VARCHAR(64) NOT NULL COMMENT 'attribute name',
            lineage VARCHAR(255) NOT NULL COMMENT 'origin: schema.table.attribute',
            PRIMARY KEY (table_name, attribute_name)
        ) ENGINE=InnoDB
        """.format(database=database)
    )


def lineage_table_exists(connection, database):
    """
    Check if the ~lineage table exists in the schema.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.

    Returns
    -------
    bool
        True if the table exists, False otherwise.
    """
    result = connection.query(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = '~lineage'
        """,
        args=(database,),
    ).fetchone()
    return result[0] > 0


def get_lineage(connection, database, table_name, attribute_name):
    """
    Get the lineage for an attribute from the ~lineage table.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.
    table_name : str
        The table name.
    attribute_name : str
        The attribute name.

    Returns
    -------
    str or None
        The lineage string, or None if not found.
    """
    if not lineage_table_exists(connection, database):
        return None

    result = connection.query(
        """
        SELECT lineage FROM `{database}`.`~lineage`
        WHERE table_name = %s AND attribute_name = %s
        """.format(database=database),
        args=(table_name, attribute_name),
    ).fetchone()
    return result[0] if result else None


def get_table_lineages(connection, database, table_name):
    """
    Get all lineages for a table from the ~lineage table.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.
    table_name : str
        The table name.

    Returns
    -------
    dict[str, str]
        Dict mapping attribute names to lineage strings.
    """
    if not lineage_table_exists(connection, database):
        return {}

    results = connection.query(
        """
        SELECT attribute_name, lineage FROM `{database}`.`~lineage`
        WHERE table_name = %s
        """.format(database=database),
        args=(table_name,),
    ).fetchall()
    return {row[0]: row[1] for row in results}


def get_schema_lineages(connection, database):
    """
    Get all lineages for a schema from the ~lineage table.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.

    Returns
    -------
    dict[str, str]
        Dict mapping 'schema.table.attribute' to its lineage.
    """
    if not lineage_table_exists(connection, database):
        return {}

    results = connection.query(
        """
        SELECT table_name, attribute_name, lineage FROM `{database}`.`~lineage`
        """.format(database=database),
    ).fetchall()

    return {f"{database}.{table}.{attr}": lineage for table, attr, lineage in results}


def insert_lineages(connection, database, entries):
    """
    Insert multiple lineage entries in the ~lineage table as a single transaction.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.
    entries : list[tuple[str, str, str]]
        List of (table_name, attribute_name, lineage) tuples.
    """
    if not entries:
        return
    ensure_lineage_table(connection, database)
    # Build a single INSERT statement with multiple values for atomicity
    placeholders = ", ".join(["(%s, %s, %s)"] * len(entries))
    # Flatten the entries into a single args tuple
    args = tuple(val for entry in entries for val in entry)
    connection.query(
        """
        INSERT INTO `{database}`.`~lineage` (table_name, attribute_name, lineage)
        VALUES {placeholders}
        ON DUPLICATE KEY UPDATE lineage = VALUES(lineage)
        """.format(database=database, placeholders=placeholders),
        args=args,
    )


def delete_table_lineages(connection, database, table_name):
    """
    Delete all lineage entries for a table.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.
    table_name : str
        The table name.
    """
    if not lineage_table_exists(connection, database):
        return
    connection.query(
        """
        DELETE FROM `{database}`.`~lineage`
        WHERE table_name = %s
        """.format(database=database),
        args=(table_name,),
    )


def rebuild_schema_lineage(connection, database):
    """
    Rebuild the ~lineage table for all tables in a schema.

    This utility recomputes lineage for all attributes in all tables
    by querying FK relationships from the information_schema. Use this
    to restore lineage after corruption or for schemas that predate
    the lineage system.

    This function assumes that any upstream schemas (referenced via
    cross-schema foreign keys) have already had their lineage rebuilt.
    If a referenced attribute in another schema has no lineage entry,
    a DataJointError is raised.

    Parameters
    ----------
    connection : Connection
        A DataJoint connection object.
    database : str
        The schema/database name.

    Raises
    ------
    DataJointError
        If a referenced attribute in another schema has no lineage entry.
    """
    # Ensure the lineage table exists
    ensure_lineage_table(connection, database)

    # Clear all existing lineage entries for this schema
    connection.query(f"DELETE FROM `{database}`.`~lineage`")

    # Get all tables in the schema (excluding hidden tables)
    tables_result = connection.query(
        """
        SELECT TABLE_NAME FROM information_schema.tables
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME NOT LIKE '~%%'
        """,
        args=(database,),
    ).fetchall()
    all_tables = {row[0] for row in tables_result}

    if not all_tables:
        return

    # Get all primary key columns for all tables
    pk_result = connection.query(
        """
        SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND CONSTRAINT_NAME = 'PRIMARY'
        """,
        args=(database,),
    ).fetchall()
    # table -> set of PK columns
    pk_columns = {}
    for table, col in pk_result:
        pk_columns.setdefault(table, set()).add(col)

    # Get all FK relationships within and across schemas
    fk_result = connection.query(
        """
        SELECT TABLE_NAME, COLUMN_NAME,
               REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
        """,
        args=(database,),
    ).fetchall()

    # Build FK map: (table, column) -> (parent_schema, parent_table, parent_column)
    fk_map = {(table, col): (ref_schema, ref_table, ref_col) for table, col, ref_schema, ref_table, ref_col in fk_result}

    # Lineage cache: (table, column) -> lineage string (for this schema)
    lineage_cache = {}

    def resolve_lineage(table, col):
        """Recursively resolve lineage for an attribute."""
        if (table, col) in lineage_cache:
            return lineage_cache[(table, col)]

        if (table, col) in fk_map:
            # FK attribute - get parent's lineage
            parent_schema, parent_table, parent_col = fk_map[(table, col)]
            if parent_schema == database:
                # Same schema - recurse
                lineage = resolve_lineage(parent_table, parent_col)
            else:
                # Cross-schema - query parent's lineage table
                lineage = get_lineage(connection, parent_schema, parent_table, parent_col)
                if not lineage:
                    raise DataJointError(
                        f"Cannot rebuild lineage for `{database}`.`{table}`: "
                        f"referenced attribute `{parent_schema}`.`{parent_table}`.`{parent_col}` "
                        f"has no lineage. Rebuild lineage for schema `{parent_schema}` first."
                    )
        else:
            # Native PK attribute - lineage is self
            lineage = f"{database}.{table}.{col}"

        lineage_cache[(table, col)] = lineage
        return lineage

    # Resolve lineage for all PK and FK attributes
    for table in all_tables:
        table_pk = pk_columns.get(table, set())
        table_fk_cols = {col for (t, col) in fk_map if t == table}

        # Process all attributes that need lineage (PK and FK)
        for col in table_pk | table_fk_cols:
            if not col.startswith("_"):
                resolve_lineage(table, col)

    # Insert all lineages in one batch
    if lineage_cache:
        entries = [(table, col, lineage) for (table, col), lineage in lineage_cache.items()]
        insert_lineages(connection, database, entries)
