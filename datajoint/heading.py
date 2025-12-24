import logging
import re
from collections import defaultdict, namedtuple
from itertools import chain

import numpy as np

from .attribute_adapter import AttributeAdapter, get_adapter
from .declare import (
    EXTERNAL_TYPES,
    NATIVE_TYPES,
    SPECIAL_TYPES,
    TYPE_PATTERN,
    UUID_DATA_TYPE,
)
from .errors import FILEPATH_FEATURE_SWITCH, DataJointError, _support_filepath_types

logger = logging.getLogger(__name__.split(".")[0])


# =============================================================================
# Lineage tracking for semantic matching in joins
# =============================================================================


def _parse_full_table_name(full_name):
    """
    Parse a full table name like `schema`.`table` into (schema, table).

    :param full_name: full table name in format `schema`.`table`
    :return: tuple (schema, table)
    """
    match = re.match(r"`(\w+)`\.`(\w+)`", full_name)
    if not match:
        raise DataJointError(f"Invalid table name format: {full_name}")
    return match.group(1), match.group(2)


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


def _compute_lineage_from_dependencies(connection, schema, table_name, attribute_name):
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
            parent_schema, parent_table = _parse_full_table_name(parent)
            return _compute_lineage_from_dependencies(
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


def _compute_all_lineage_for_table(connection, schema, table_name):
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
        attr: _compute_lineage_from_dependencies(connection, schema, table_name, attr)
        for attr in heading.names
    }


def _get_lineage_for_heading(connection, schema, table_name):
    """
    Get lineage information for all attributes in a table.

    First tries to load from ~lineage table, falls back to FK graph computation.

    :param connection: database connection
    :param schema: schema name
    :param table_name: table name
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
        return _compute_all_lineage_for_table(connection, schema, table_name)


class LineageTable:
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
        # Lazy import to avoid circular dependency
        from .table import Table

        self._table_class = Table
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
            self._declare()

    @property
    def table_name(self):
        return "~lineage"

    @property
    def full_table_name(self):
        return f"`{self.database}`.`{self.table_name}`"

    @property
    def is_declared(self):
        return (
            self._connection.query(
                """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
                args=(self.database, self.table_name),
            ).fetchone()[0]
            > 0
        )

    def _declare(self):
        """Create the ~lineage table."""
        self._connection.query(
            f"""
            CREATE TABLE IF NOT EXISTS {self.full_table_name} (
                table_name VARCHAR(64) NOT NULL,
                attribute_name VARCHAR(64) NOT NULL,
                lineage VARCHAR(200) NOT NULL,
                PRIMARY KEY (table_name, attribute_name)
            ) ENGINE=InnoDB
            """
        )

    def insert1(self, row, replace=False):
        """Insert a single row."""
        if replace:
            self._connection.query(
                f"""
                REPLACE INTO {self.full_table_name}
                (table_name, attribute_name, lineage)
                VALUES (%s, %s, %s)
                """,
                args=(row["table_name"], row["attribute_name"], row["lineage"]),
            )
        else:
            self._connection.query(
                f"""
                INSERT INTO {self.full_table_name}
                (table_name, attribute_name, lineage)
                VALUES (%s, %s, %s)
                """,
                args=(row["table_name"], row["attribute_name"], row["lineage"]),
            )

    def delete_quick(self, table_name=None, attribute_name=None):
        """Delete rows without prompts."""
        if table_name and attribute_name:
            self._connection.query(
                f"DELETE FROM {self.full_table_name} WHERE table_name=%s AND attribute_name=%s",
                args=(table_name, attribute_name),
            )
        elif table_name:
            self._connection.query(
                f"DELETE FROM {self.full_table_name} WHERE table_name=%s",
                args=(table_name,),
            )
        else:
            self._connection.query(f"DELETE FROM {self.full_table_name}")

    def store_lineage(self, table_name, attribute_name, lineage):
        """
        Store lineage for an attribute. Only stores if lineage is not None.

        :param table_name: name of the table (without schema)
        :param attribute_name: name of the attribute
        :param lineage: lineage string "schema.table.attribute" or None
        """
        if lineage is None:
            # No lineage - delete any existing entry
            self.delete_quick(table_name, attribute_name)
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
        result = self._connection.query(
            f"SELECT lineage FROM {self.full_table_name} WHERE table_name=%s AND attribute_name=%s",
            args=(table_name, attribute_name),
        ).fetchone()
        return result[0] if result else None

    def get_table_lineage(self, table_name):
        """
        Get lineage for all attributes in a table.

        :param table_name: name of the table (without schema)
        :return: dict mapping attribute_name -> lineage (only attributes with lineage)
        """
        result = self._connection.query(
            f"SELECT attribute_name, lineage FROM {self.full_table_name} WHERE table_name=%s",
            args=(table_name,),
        ).fetchall()
        return {row[0]: row[1] for row in result}

    def delete_table_lineage(self, table_name):
        """
        Delete all lineage records for a table.

        :param table_name: name of the table (without schema)
        """
        self.delete_quick(table_name)


def compute_schema_lineage(connection, schema):
    """
    Compute and populate the ~lineage table for a schema.

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
        lineage_map = _compute_all_lineage_for_table(
            connection, schema_name, table_name
        )
        for attr_name, lineage in lineage_map.items():
            if lineage is not None:
                lineage_table.store_lineage(table_name, attr_name, lineage)

    logger.info(f"Computed lineage for schema `{schema_name}`: {len(tables)} tables")


# =============================================================================
# End of lineage tracking
# =============================================================================

default_attribute_properties = dict(  # these default values are set in computed attributes
    name=None,
    type="expression",
    in_key=False,
    nullable=False,
    default=None,
    comment="calculated attribute",
    autoincrement=False,
    numeric=None,
    string=None,
    uuid=False,
    json=None,
    is_blob=False,
    is_attachment=False,
    is_filepath=False,
    is_external=False,
    is_hidden=False,
    adapter=None,
    store=None,
    unsupported=False,
    attribute_expression=None,
    database=None,
    dtype=object,
    lineage=None,  # "schema.table.attribute" string tracing attribute origin, or None
)


class Attribute(namedtuple("_Attribute", default_attribute_properties)):
    """
    Properties of a table column (attribute)
    """

    def todict(self):
        """Convert namedtuple to dict."""
        return dict((name, self[i]) for i, name in enumerate(self._fields))

    @property
    def sql_type(self):
        """:return: datatype (as string) in database. In most cases, it is the same as self.type"""
        return UUID_DATA_TYPE if self.uuid else self.type

    @property
    def sql_comment(self):
        """:return: full comment for the SQL declaration. Includes custom type specification"""
        return (":uuid:" if self.uuid else "") + self.comment

    @property
    def sql(self):
        """
        Convert primary key attribute tuple into its SQL CREATE TABLE clause.
        Default values are not reflected.
        This is used for declaring foreign keys in referencing tables

        :return: SQL code for attribute declaration
        """
        return '`{name}` {type} NOT NULL COMMENT "{comment}"'.format(
            name=self.name, type=self.sql_type, comment=self.sql_comment
        )

    @property
    def original_name(self):
        if self.attribute_expression is None:
            return self.name
        assert self.attribute_expression.startswith("`")
        return self.attribute_expression.strip("`")


class Heading:
    """
    Local class for table headings.
    Heading contains the property attributes, which is an dict in which the keys are
    the attribute names and the values are Attributes.
    """

    def __init__(self, attribute_specs=None, table_info=None):
        """

        :param attribute_specs: a list of dicts with the same keys as Attribute
        :param table_info: a dict with information to load the heading from the database
        """
        self.indexes = None
        self.table_info = table_info
        self._table_status = None
        self._attributes = (
            None
            if attribute_specs is None
            else dict((q["name"], Attribute(**q)) for q in attribute_specs)
        )

    def __len__(self):
        return 0 if self.attributes is None else len(self.attributes)

    @property
    def table_status(self):
        if self.table_info is None:
            return None
        if self._table_status is None:
            self._init_from_database()
        return self._table_status

    @property
    def attributes(self):
        if self._attributes is None:
            self._init_from_database()  # lazy loading from database
        return {k: v for k, v in self._attributes.items() if not v.is_hidden}

    @property
    def names(self):
        return [k for k in self.attributes]

    @property
    def primary_key(self):
        return [k for k, v in self.attributes.items() if v.in_key]

    @property
    def secondary_attributes(self):
        return [k for k, v in self.attributes.items() if not v.in_key]

    @property
    def blobs(self):
        return [k for k, v in self.attributes.items() if v.is_blob]

    @property
    def non_blobs(self):
        return [
            k
            for k, v in self.attributes.items()
            if not (v.is_blob or v.is_attachment or v.is_filepath or v.json)
        ]

    @property
    def new_attributes(self):
        return [
            k for k, v in self.attributes.items() if v.attribute_expression is not None
        ]

    def get_lineage(self, name):
        """
        Get the lineage of an attribute.

        :param name: attribute name
        :return: lineage string "schema.table.attribute" or None
        """
        return self.attributes[name].lineage

    def __getitem__(self, name):
        """shortcut to the attribute"""
        return self.attributes[name]

    def __repr__(self):
        """
        :return:  heading representation in DataJoint declaration format but without foreign key expansion
        """
        in_key = True
        ret = ""
        if self._table_status is not None:
            ret += "# " + self.table_status["comment"] + "\n"
        for v in self.attributes.values():
            if in_key and not v.in_key:
                ret += "---\n"
                in_key = False
            ret += "%-20s : %-28s # %s\n" % (
                v.name if v.default is None else "%s=%s" % (v.name, v.default),
                "%s%s" % (v.type, "auto_increment" if v.autoincrement else ""),
                v.comment,
            )
        return ret

    @property
    def has_autoincrement(self):
        return any(e.autoincrement for e in self.attributes.values())

    @property
    def as_dtype(self):
        """
        represent the heading as a numpy dtype
        """
        return np.dtype(
            dict(names=self.names, formats=[v.dtype for v in self.attributes.values()])
        )

    def as_sql(self, fields, include_aliases=True):
        """
        represent heading as the SQL SELECT clause.
        """
        return ",".join(
            (
                "`%s`" % name
                if self.attributes[name].attribute_expression is None
                else self.attributes[name].attribute_expression
                + (" as `%s`" % name if include_aliases else "")
            )
            for name in fields
        )

    def __iter__(self):
        return iter(self.attributes)

    def _init_from_database(self):
        """initialize heading from an existing database table."""
        conn, database, table_name, context = (
            self.table_info[k] for k in ("conn", "database", "table_name", "context")
        )
        info = conn.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE name="{table_name}"'.format(
                table_name=table_name, database=database
            ),
            as_dict=True,
        ).fetchone()
        if info is None:
            if table_name == "~log":
                logger.warning("Could not create the ~log table")
                return
            raise DataJointError(
                "The table `{database}`.`{table_name}` is not defined.".format(
                    table_name=table_name, database=database
                )
            )
        self._table_status = {k.lower(): v for k, v in info.items()}
        cur = conn.query(
            "SHOW FULL COLUMNS FROM `{table_name}` IN `{database}`".format(
                table_name=table_name, database=database
            ),
            as_dict=True,
        )

        attributes = cur.fetchall()

        rename_map = {
            "Field": "name",
            "Type": "type",
            "Null": "nullable",
            "Default": "default",
            "Key": "in_key",
            "Comment": "comment",
        }

        fields_to_drop = ("Privileges", "Collation")

        # rename and drop attributes
        attributes = [
            {
                rename_map[k] if k in rename_map else k: v
                for k, v in x.items()
                if k not in fields_to_drop
            }
            for x in attributes
        ]
        numeric_types = {
            ("float", False): np.float64,
            ("float", True): np.float64,
            ("double", False): np.float64,
            ("double", True): np.float64,
            ("tinyint", False): np.int64,
            ("tinyint", True): np.int64,
            ("smallint", False): np.int64,
            ("smallint", True): np.int64,
            ("mediumint", False): np.int64,
            ("mediumint", True): np.int64,
            ("int", False): np.int64,
            ("int", True): np.int64,
            ("bigint", False): np.int64,
            ("bigint", True): np.uint64,
        }

        sql_literals = ["CURRENT_TIMESTAMP"]

        # additional attribute properties
        for attr in attributes:
            attr.update(
                in_key=(attr["in_key"] == "PRI"),
                database=database,
                nullable=attr["nullable"] == "YES",
                autoincrement=bool(
                    re.search(r"auto_increment", attr["Extra"], flags=re.I)
                ),
                numeric=any(
                    TYPE_PATTERN[t].match(attr["type"])
                    for t in ("DECIMAL", "INTEGER", "FLOAT")
                ),
                string=any(
                    TYPE_PATTERN[t].match(attr["type"])
                    for t in ("ENUM", "TEMPORAL", "STRING")
                ),
                is_blob=bool(TYPE_PATTERN["INTERNAL_BLOB"].match(attr["type"])),
                uuid=False,
                json=bool(TYPE_PATTERN["JSON"].match(attr["type"])),
                is_attachment=False,
                is_filepath=False,
                adapter=None,
                store=None,
                is_external=False,
                attribute_expression=None,
                is_hidden=attr["name"].startswith("_"),
            )

            if any(TYPE_PATTERN[t].match(attr["type"]) for t in ("INTEGER", "FLOAT")):
                attr["type"] = re.sub(
                    r"\(\d+\)", "", attr["type"], count=1
                )  # strip size off integers and floats
            attr["unsupported"] = not any(
                (attr["is_blob"], attr["numeric"], attr["numeric"])
            )
            attr.pop("Extra")

            # process custom DataJoint types
            special = re.match(r":(?P<type>[^:]+):(?P<comment>.*)", attr["comment"])
            if special:
                special = special.groupdict()
                attr.update(special)
            # process adapted attribute types
            if special and TYPE_PATTERN["ADAPTED"].match(attr["type"]):
                assert context is not None, "Declaration context is not set"
                adapter_name = special["type"]
                try:
                    attr.update(adapter=get_adapter(context, adapter_name))
                except DataJointError:
                    # if no adapter, then delay the error until the first invocation
                    attr.update(adapter=AttributeAdapter())
                else:
                    attr.update(type=attr["adapter"].attribute_type)
                    if not any(r.match(attr["type"]) for r in TYPE_PATTERN.values()):
                        raise DataJointError(
                            "Invalid attribute type '{type}' in adapter object <{adapter_name}>.".format(
                                adapter_name=adapter_name, **attr
                            )
                        )
                    special = not any(
                        TYPE_PATTERN[c].match(attr["type"]) for c in NATIVE_TYPES
                    )

            if special:
                try:
                    category = next(
                        c for c in SPECIAL_TYPES if TYPE_PATTERN[c].match(attr["type"])
                    )
                except StopIteration:
                    if attr["type"].startswith("external"):
                        url = (
                            "https://docs.datajoint.io/python/admin/5-blob-config.html"
                            "#migration-between-datajoint-v0-11-and-v0-12"
                        )
                        raise DataJointError(
                            "Legacy datatype `{type}`. Migrate your external stores to "
                            "datajoint 0.12: {url}".format(url=url, **attr)
                        )
                    raise DataJointError(
                        "Unknown attribute type `{type}`".format(**attr)
                    )
                if category == "FILEPATH" and not _support_filepath_types():
                    raise DataJointError(
                        """
                        The filepath data type is disabled until complete validation.
                        To turn it on as experimental feature, set the environment variable
                        {env} = TRUE or upgrade datajoint.
                        """.format(
                            env=FILEPATH_FEATURE_SWITCH
                        )
                    )
                attr.update(
                    unsupported=False,
                    is_attachment=category in ("INTERNAL_ATTACH", "EXTERNAL_ATTACH"),
                    is_filepath=category == "FILEPATH",
                    # INTERNAL_BLOB is not a custom type but is included for completeness
                    is_blob=category in ("INTERNAL_BLOB", "EXTERNAL_BLOB"),
                    uuid=category == "UUID",
                    is_external=category in EXTERNAL_TYPES,
                    store=(
                        attr["type"].split("@")[1]
                        if category in EXTERNAL_TYPES
                        else None
                    ),
                )

            if attr["in_key"] and any(
                (
                    attr["is_blob"],
                    attr["is_attachment"],
                    attr["is_filepath"],
                    attr["json"],
                )
            ):
                raise DataJointError(
                    "Json, Blob, attachment, or filepath attributes are not allowed in the primary key"
                )

            if (
                attr["string"]
                and attr["default"] is not None
                and attr["default"] not in sql_literals
            ):
                attr["default"] = '"%s"' % attr["default"]

            if attr["nullable"]:  # nullable fields always default to null
                attr["default"] = "null"

            # fill out dtype. All floats and non-nullable integers are turned into specific dtypes
            attr["dtype"] = object
            if attr["numeric"] and not attr["adapter"]:
                is_integer = TYPE_PATTERN["INTEGER"].match(attr["type"])
                is_float = TYPE_PATTERN["FLOAT"].match(attr["type"])
                if is_integer and not attr["nullable"] or is_float:
                    is_unsigned = bool(re.match("sunsigned", attr["type"], flags=re.I))
                    t = re.sub(r"\(.*\)", "", attr["type"])  # remove parentheses
                    t = re.sub(r" unsigned$", "", t)  # remove unsigned
                    assert (t, is_unsigned) in numeric_types, (
                        "dtype not found for type %s" % t
                    )
                    attr["dtype"] = numeric_types[(t, is_unsigned)]

            if attr["adapter"]:
                # restore adapted type name
                attr["type"] = adapter_name

        # Load lineage information for semantic matching
        try:
            lineage_map = _get_lineage_for_heading(conn, database, table_name)
            for attr in attributes:
                attr["lineage"] = lineage_map.get(attr["name"])
        except Exception as e:
            # If lineage loading fails, continue without it (backward compatibility)
            logger.debug(f"Could not load lineage for {database}.{table_name}: {e}")

        self._attributes = dict(((q["name"], Attribute(**q)) for q in attributes))

        # Read and tabulate secondary indexes
        keys = defaultdict(dict)
        for item in conn.query(
            "SHOW KEYS FROM `{db}`.`{tab}`".format(db=database, tab=table_name),
            as_dict=True,
        ):
            if item["Key_name"] != "PRIMARY":
                keys[item["Key_name"]][item["Seq_in_index"]] = dict(
                    column=item["Column_name"]
                    or f"({item['Expression']})".replace(r"\'", "'"),
                    unique=(item["Non_unique"] == 0),
                    nullable=item["Null"].lower() == "yes",
                )
        self.indexes = {
            tuple(item[k]["column"] for k in sorted(item.keys())): dict(
                unique=item[1]["unique"],
                nullable=any(v["nullable"] for v in item.values()),
            )
            for item in keys.values()
        }

    def select(self, select_list, rename_map=None, compute_map=None):
        """
        derive a new heading by selecting, renaming, or computing attributes.
        In relational algebra these operators are known as project, rename, and extend.

        :param select_list:  the full list of existing attributes to include
        :param rename_map:  dictionary of renamed attributes: keys=new names, values=old names
        :param compute_map: a direction of computed attributes
        This low-level method performs no error checking.
        """
        rename_map = rename_map or {}
        compute_map = compute_map or {}
        copy_attrs = list()
        for name in self.attributes:
            if name in select_list:
                copy_attrs.append(self.attributes[name].todict())
            copy_attrs.extend(
                (
                    dict(
                        self.attributes[old_name].todict(),
                        name=new_name,
                        attribute_expression="`%s`" % old_name,
                    )
                    for new_name, old_name in rename_map.items()
                    if old_name == name
                )
            )
        compute_attrs = (
            dict(default_attribute_properties, name=new_name, attribute_expression=expr)
            for new_name, expr in compute_map.items()
        )
        return Heading(chain(copy_attrs, compute_attrs))

    def join(self, other):
        """
        Join two headings into a new one.
        It assumes that self and other are headings that share no common dependent attributes.
        """
        return Heading(
            [self.attributes[name].todict() for name in self.primary_key]
            + [
                other.attributes[name].todict()
                for name in other.primary_key
                if name not in self.primary_key
            ]
            + [
                self.attributes[name].todict()
                for name in self.secondary_attributes
                if name not in other.primary_key
            ]
            + [
                other.attributes[name].todict()
                for name in other.secondary_attributes
                if name not in self.primary_key
            ]
        )

    def set_primary_key(self, primary_key):
        """
        Create a new heading with the specified primary key.
        This low-level method performs no error checking.
        """
        return Heading(
            chain(
                (
                    dict(self.attributes[name].todict(), in_key=True)
                    for name in primary_key
                ),
                (
                    dict(self.attributes[name].todict(), in_key=False)
                    for name in self.names
                    if name not in primary_key
                ),
            )
        )

    def make_subquery_heading(self):
        """
        Create a new heading with removed attribute sql_expressions.
        Used by subqueries, which resolve the sql_expressions.
        """
        return Heading(
            dict(v.todict(), attribute_expression=None)
            for v in self.attributes.values()
        )
