"""
Lineage tracking for semantic matching in joins.

Lineage identifies the origin of an attribute - where it was first defined.
It is represented as a string in the format: "schema.table.attribute"

Only attributes WITH lineage are stored in the ~lineage table:
- Native primary key attributes: lineage is this table
- FK-inherited attributes: lineage is traced to the origin
- Native secondary attributes: no lineage (no entry in table)
"""

import logging

logger = logging.getLogger(__name__.split(".")[0])

LINEAGE_TABLE_NAME = "~lineage"


def _lineage_table_sql(database):
    """Generate SQL to create the ~lineage table."""
    return f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{LINEAGE_TABLE_NAME}` (
            table_name VARCHAR(64) NOT NULL,
            attribute_name VARCHAR(64) NOT NULL,
            lineage VARCHAR(255) NOT NULL,
            PRIMARY KEY (table_name, attribute_name)
        ) ENGINE=InnoDB
    """


def ensure_lineage_table(connection, database):
    """Create the ~lineage table if it doesn't exist."""
    connection.query(_lineage_table_sql(database))


def lineage_table_exists(connection, database):
    """Check if the ~lineage table exists in the schema."""
    result = connection.query(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        args=(database, LINEAGE_TABLE_NAME),
    )
    return result.fetchone()[0] > 0


def get_lineage(connection, database, table_name, attribute_name):
    """
    Get lineage for an attribute from the ~lineage table.

    Returns the lineage string if found, None otherwise (indicating no lineage
    or attribute is a native secondary).
    """
    if not lineage_table_exists(connection, database):
        return None

    result = connection.query(
        f"""
        SELECT lineage FROM `{database}`.`{LINEAGE_TABLE_NAME}`
        WHERE table_name = %s AND attribute_name = %s
        """,
        args=(table_name, attribute_name),
    )
    row = result.fetchone()
    return row[0] if row else None


def get_all_lineages(connection, database, table_name):
    """
    Get all lineage entries for a table.

    Returns a dict mapping attribute_name -> lineage.
    Attributes not in the dict have no lineage (native secondary).
    """
    if not lineage_table_exists(connection, database):
        return {}

    result = connection.query(
        f"""
        SELECT attribute_name, lineage FROM `{database}`.`{LINEAGE_TABLE_NAME}`
        WHERE table_name = %s
        """,
        args=(table_name,),
    )
    return {row[0]: row[1] for row in result}


def delete_lineage_entries(connection, database, table_name):
    """Delete all lineage entries for a table."""
    if not lineage_table_exists(connection, database):
        return

    connection.query(
        f"""
        DELETE FROM `{database}`.`{LINEAGE_TABLE_NAME}`
        WHERE table_name = %s
        """,
        args=(table_name,),
    )


def insert_lineage_entries(connection, database, entries):
    """
    Insert lineage entries for a table.

    :param entries: list of (table_name, attribute_name, lineage) tuples
    """
    if not entries:
        return

    ensure_lineage_table(connection, database)

    # Use INSERT ... ON DUPLICATE KEY UPDATE to handle re-declarations
    for table_name, attribute_name, lineage in entries:
        connection.query(
            f"""
            INSERT INTO `{database}`.`{LINEAGE_TABLE_NAME}`
            (table_name, attribute_name, lineage)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE lineage = VALUES(lineage)
            """,
            args=(table_name, attribute_name, lineage),
        )


def compute_lineage_from_dependencies(connection, full_table_name, attribute_name, primary_key):
    """
    Compute lineage by traversing FK relationships.

    Fallback method when ~lineage table doesn't exist.

    :param connection: database connection
    :param full_table_name: fully qualified table name like `schema`.`table`
    :param attribute_name: the attribute to compute lineage for
    :param primary_key: list of primary key attribute names for this table
    :return: lineage string or None
    """
    connection.dependencies.load(force=False)

    # Parse database and table name
    parts = full_table_name.replace("`", "").split(".")
    database = parts[0]
    table_name = parts[1]

    # Check if attribute is inherited via FK
    parents = connection.dependencies.parents(full_table_name)
    for parent_table, props in parents.items():
        # Skip alias nodes (numeric strings)
        if parent_table.isdigit():
            # Get the actual parent through the alias
            grandparents = connection.dependencies.parents(parent_table)
            if grandparents:
                parent_table, props = next(iter(grandparents.items()))

        attr_map = props.get("attr_map", {})
        if attribute_name in attr_map:
            parent_attr = attr_map[attribute_name]

            # Get parent's primary key
            parent_pk = connection.dependencies.nodes.get(parent_table, {}).get("primary_key", set())

            # Recursively trace to origin
            return compute_lineage_from_dependencies(connection, parent_table, parent_attr, list(parent_pk))

    # Not inherited - check if primary key
    if attribute_name in primary_key:
        return f"{database}.{table_name}.{attribute_name}"

    # Native secondary - no lineage
    return None
