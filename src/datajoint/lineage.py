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

from .errors import MissingTableError

logger = logging.getLogger(__name__.split(".")[0])

LINEAGE_TABLE_NAME = "~lineage"


def create_lineage_table(connection, database):
    """
    Create the ~lineage table if it doesn't exist.

    :param connection: database connection
    :param database: schema/database name
    """
    connection.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`{LINEAGE_TABLE_NAME}` (
            table_name VARCHAR(64) NOT NULL,
            attribute_name VARCHAR(64) NOT NULL,
            lineage VARCHAR(255) NOT NULL,
            PRIMARY KEY (table_name, attribute_name)
        ) ENGINE=InnoDB
        """
    )


def get_lineage(connection, database, table_name, attribute_name):
    """
    Get lineage for an attribute from the ~lineage table.

    :param connection: database connection
    :param database: schema/database name
    :param table_name: name of the table
    :param attribute_name: name of the attribute
    :return: lineage string if found, None otherwise (no lineage or native secondary)
    """
    try:
        result = connection.query(
            f"""
            SELECT lineage FROM `{database}`.`{LINEAGE_TABLE_NAME}`
            WHERE table_name = %s AND attribute_name = %s
            """,
            args=(table_name, attribute_name),
        )
        row = result.fetchone()
        return row[0] if row else None
    except MissingTableError:
        # ~lineage table doesn't exist yet
        return None


def get_all_lineages(connection, database, table_name):
    """
    Get all lineage entries for a table.

    :param connection: database connection
    :param database: schema/database name
    :param table_name: name of the table
    :return: dict mapping attribute_name -> lineage (attributes not in dict have no lineage)
    """
    try:
        result = connection.query(
            f"""
            SELECT attribute_name, lineage FROM `{database}`.`{LINEAGE_TABLE_NAME}`
            WHERE table_name = %s
            """,
            args=(table_name,),
        )
        return {row[0]: row[1] for row in result}
    except MissingTableError:
        # ~lineage table doesn't exist yet
        return {}


def delete_lineage_entries(connection, database, table_name):
    """
    Delete all lineage entries for a table.

    :param connection: database connection
    :param database: schema/database name
    :param table_name: name of the table
    """
    try:
        connection.query(
            f"""
            DELETE FROM `{database}`.`{LINEAGE_TABLE_NAME}`
            WHERE table_name = %s
            """,
            args=(table_name,),
        )
    except MissingTableError:
        # ~lineage table doesn't exist yet - nothing to delete
        pass


def insert_lineage_entries(connection, database, entries):
    """
    Insert lineage entries for a table.

    :param connection: database connection
    :param database: schema/database name
    :param entries: list of (table_name, attribute_name, lineage) tuples
    """
    if not entries:
        return

    create_lineage_table(connection, database)

    for table_name, attribute_name, lineage in entries:
        connection.query(
            f"""
            INSERT INTO `{database}`.`{LINEAGE_TABLE_NAME}`
            (table_name, attribute_name, lineage)
            VALUES (%s, %s, %s)
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
