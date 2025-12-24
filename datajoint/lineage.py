"""
Lineage tracking for semantic matching in joins.

This module provides:
- LineageTable: hidden table (~lineage) for storing attribute lineage
- Functions to compute lineage from the FK graph (fallback)
- Migration utilities for existing schemas
"""

import logging
import re

from .errors import DataJointError
from .heading import Heading
from .table import Table

logger = logging.getLogger(__name__.split(".")[0])


class LineageTable(Table):
    """
    Hidden table for storing attribute lineage information.

    Each row maps (table_name, attribute_name) -> lineage string.
    Only attributes with lineage are stored; absence means no lineage.
    """

    definition = """
    # Attribute lineage tracking for semantic matching
    table_name      : varchar(64)   # name of the table
    attribute_name  : varchar(64)   # name of the attribute
    ---
    lineage         : varchar(200)  # "schema.table.attribute"
    """

    def __init__(self, connection, database):
        self.database = database
        self._connection = connection
        self._heading = Heading(
            table_info=dict(
                conn=connection,
                database=database,
                table_name=self.table_name,
                context=None,
            )
        )
        self._support = [self.full_table_name]

        if not self.is_declared:
            self.declare()

    @property
    def table_name(self):
        return "~lineage"

    def delete(self):
        """Bypass interactive prompts and dependencies."""
        self.delete_quick()

    def drop(self):
        """Bypass interactive prompts and dependencies."""
        self.drop_quick()

    def store_lineage(self, table_name, attribute_name, lineage):
        """
        Store lineage for an attribute. Only stores if lineage is not None.

        :param table_name: name of the table (without schema)
        :param attribute_name: name of the attribute
        :param lineage: lineage string "schema.table.attribute" or None
        """
        if lineage is None:
            # No lineage - delete any existing entry
            (
                self & dict(table_name=table_name, attribute_name=attribute_name)
            ).delete_quick()
        else:
            self.insert1(
                dict(
                    table_name=table_name,
                    attribute_name=attribute_name,
                    lineage=lineage,
                ),
                replace=True,
            )

    def get_lineage(self, table_name, attribute_name):
        """
        Get lineage for an attribute.

        :param table_name: name of the table (without schema)
        :param attribute_name: name of the attribute
        :return: lineage string or None if no lineage
        """
        result = (
            self & dict(table_name=table_name, attribute_name=attribute_name)
        ).fetch("lineage")
        return result[0] if len(result) else None

    def get_table_lineage(self, table_name):
        """
        Get lineage for all attributes in a table.

        :param table_name: name of the table (without schema)
        :return: dict mapping attribute_name -> lineage (only attributes with lineage)
        """
        result = (self & dict(table_name=table_name)).fetch("attribute_name", "lineage")
        if len(result[0]) == 0:
            return {}
        return dict(zip(result[0], result[1]))

    def delete_table_lineage(self, table_name):
        """
        Delete all lineage records for a table.

        :param table_name: name of the table (without schema)
        """
        (self & dict(table_name=table_name)).delete_quick()


def parse_full_table_name(full_name):
    """
    Parse a full table name like `schema`.`table` into (schema, table).

    :param full_name: full table name in format `schema`.`table`
    :return: tuple (schema, table)
    """
    match = re.match(r"`(\w+)`\.`(\w+)`", full_name)
    if not match:
        raise DataJointError(f"Invalid table name format: {full_name}")
    return match.group(1), match.group(2)


def compute_lineage_from_dependencies(connection, schema, table_name, attribute_name):
    """
    Compute lineage by traversing the FK graph.

    Uses connection.dependencies which loads FK info from INFORMATION_SCHEMA.
    This is the fallback when the ~lineage table doesn't exist.

    :param connection: database connection
    :param schema: schema name
    :param table_name: table name
    :param attribute_name: attribute name
    :return: lineage string "schema.table.attribute" or None for native secondary attrs
    """
    connection.dependencies.load(force=False)

    full_table_name = f"`{schema}`.`{table_name}`"

    # Check if the table exists in the dependency graph
    if full_table_name not in connection.dependencies:
        # Table not in graph - compute lineage based on primary key status
        # We need to query the database to check if this is a PK attribute
        pk_attrs = _get_primary_key_attrs(connection, schema, table_name)
        if attribute_name in pk_attrs:
            return f"{schema}.{table_name}.{attribute_name}"
        else:
            return None

    # Check incoming edges (foreign keys TO this table's parents)
    for parent, props in connection.dependencies.parents(full_table_name).items():
        attr_map = props.get("attr_map", {})
        if attribute_name in attr_map:
            # This attribute is inherited from parent - recurse to find origin
            parent_attr = attr_map[attribute_name]
            # Handle alias nodes (numeric string nodes in the graph)
            if parent.isdigit():
                # Find the actual parent by traversing through alias
                for grandparent, gprops in connection.dependencies.parents(
                    parent
                ).items():
                    if not grandparent.isdigit():
                        parent = grandparent
                        parent_attr = gprops.get("attr_map", {}).get(
                            attribute_name, parent_attr
                        )
                        break
            parent_schema, parent_table = parse_full_table_name(parent)
            return compute_lineage_from_dependencies(
                connection, parent_schema, parent_table, parent_attr
            )

    # Not inherited - check if it's a primary key attribute
    node_data = connection.dependencies.nodes.get(full_table_name, {})
    pk_attrs = node_data.get("primary_key", set())

    if attribute_name in pk_attrs:
        # Native primary key attribute - has lineage to itself
        return f"{schema}.{table_name}.{attribute_name}"
    else:
        # Native secondary attribute - no lineage
        return None


def _get_primary_key_attrs(connection, schema, table_name):
    """
    Get the primary key attributes for a table by querying the database.

    :param connection: database connection
    :param schema: schema name
    :param table_name: table name
    :return: set of primary key attribute names
    """
    result = connection.query(
        """
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = %s
          AND table_name = %s
          AND constraint_name = 'PRIMARY'
        """,
        args=(schema, table_name),
    )
    return {row[0] for row in result}


def compute_all_lineage_for_table(connection, schema, table_name):
    """
    Compute lineage for all attributes in a table.

    :param connection: database connection
    :param schema: schema name
    :param table_name: table name
    :return: dict mapping attribute_name -> lineage (or None)
    """
    # Get all attributes using Heading
    heading = Heading(
        table_info=dict(
            conn=connection,
            database=schema,
            table_name=table_name,
            context=None,
        )
    )

    # Compute lineage for each attribute
    return {
        attr: compute_lineage_from_dependencies(connection, schema, table_name, attr)
        for attr in heading.names
    }


def migrate_schema_lineage(connection, schema):
    """
    Compute and populate the ~lineage table for an existing schema.

    Analyzes foreign key relationships to determine attribute origins.

    :param connection: database connection
    :param schema: schema object or schema name
    """
    from .schemas import Schema

    if isinstance(schema, Schema):
        schema_name = schema.database
    else:
        schema_name = schema

    # Create or get the lineage table
    lineage_table = LineageTable(connection, schema_name)

    # Get all user tables in the schema (excluding hidden tables)
    result = connection.query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name NOT LIKE '~%%'
          AND table_type = 'BASE TABLE'
        """,
        args=(schema_name,),
    )
    tables = [row[0] for row in result]

    # Ensure dependencies are loaded
    connection.dependencies.load(force=True)

    # Compute and store lineage for each table
    for table_name in tables:
        lineage_map = compute_all_lineage_for_table(connection, schema_name, table_name)
        for attr_name, lineage in lineage_map.items():
            if lineage is not None:
                lineage_table.store_lineage(table_name, attr_name, lineage)

    logger.info(f"Migrated lineage for schema `{schema_name}`: {len(tables)} tables")


def get_lineage_for_heading(connection, schema, table_name, heading):
    """
    Get lineage information for all attributes in a heading.

    First tries to load from ~lineage table, falls back to FK graph computation.

    :param connection: database connection
    :param schema: schema name
    :param table_name: table name
    :param heading: Heading object to populate
    :return: dict mapping attribute_name -> lineage (or None)
    """
    # Check if ~lineage table exists
    lineage_table_exists = (
        connection.query(
            """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = %s AND table_name = '~lineage'
        """,
            args=(schema,),
        ).fetchone()[0]
        > 0
    )

    if lineage_table_exists:
        # Load from ~lineage table
        lineage_table = LineageTable(connection, schema)
        return lineage_table.get_table_lineage(table_name)
    else:
        # Compute from FK graph
        return compute_all_lineage_for_table(connection, schema, table_name)
