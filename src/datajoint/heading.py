"""
Heading management for DataJoint tables.

This module provides the Heading class for managing table column metadata,
including attribute types, constraints, and lineage information.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict, namedtuple
from itertools import chain
from typing import TYPE_CHECKING, Any

import numpy as np

from .codecs import lookup_codec
from .codecs import Codec
from .declare import (
    CORE_TYPE_NAMES,
    SPECIAL_TYPES,
    TYPE_PATTERN,
)
from .errors import DataJointError
from .lineage import get_table_lineages, lineage_table_exists

if TYPE_CHECKING:
    pass


class _MissingType(Codec, register=False):
    """Placeholder for missing/unregistered codecs. Raises error on use."""

    def __init__(self, codec_name: str):
        self._codec_name = codec_name

    @property
    def name(self) -> str:
        return self._codec_name

    def get_dtype(self, is_store: bool) -> str:
        raise DataJointError(
            f"Codec <{self._codec_name}> is not registered. Define a Codec subclass with name='{self._codec_name}'."
        )

    def encode(self, value, *, key=None, store_name=None):
        raise DataJointError(
            f"Codec <{self._codec_name}> is not registered. Define a Codec subclass with name='{self._codec_name}'."
        )

    def decode(self, stored, *, key=None):
        raise DataJointError(
            f"Codec <{self._codec_name}> is not registered. Define a Codec subclass with name='{self._codec_name}'."
        )


logger = logging.getLogger(__name__.split(".")[0])

default_attribute_properties = dict(  # these default values are set in computed attributes
    name=None,
    type="expression",
    original_type=None,  # For core types, stores the alias (e.g., "uuid") while type has db type ("binary(16)")
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
    is_hidden=False,
    codec=None,
    store=None,
    unsupported=False,
    attribute_expression=None,
    dtype=object,
    lineage=None,  # Origin of attribute, e.g. "schema.table.attr" for semantic matching
)


class Attribute(namedtuple("_Attribute", default_attribute_properties)):
    """
    Properties of a table column (attribute).

    Attributes
    ----------
    name : str
        Attribute name.
    type : str
        Database type string.
    in_key : bool
        True if part of primary key.
    nullable : bool
        True if NULL values allowed.
    default : any
        Default value.
    comment : str
        Attribute comment/description.
    codec : Codec
        Codec for encoding/decoding values.
    lineage : str
        Origin of attribute for semantic matching.
    """

    def todict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return dict((name, self[i]) for i, name in enumerate(self._fields))

    @property
    def sql_type(self) -> str:
        """
        Return the SQL datatype string.

        Returns
        -------
        str
            Database type (usually same as self.type).
        """
        # UUID is now a core type alias - already resolved to binary(16)
        return self.type

    @property
    def sql_comment(self) -> str:
        """
        Return the full SQL comment including type markers.

        Returns
        -------
        str
            Comment with optional ``:uuid:`` prefix.
        """
        # UUID info is stored in the comment for reconstruction
        return (":uuid:" if self.uuid else "") + (self.comment or "")

    @property
    def sql(self) -> str:
        """
        Generate SQL clause for this attribute in CREATE TABLE.

        Used for declaring foreign keys in referencing tables.
        Default values are not included.

        Returns
        -------
        str
            SQL attribute declaration.
        """
        return '`{name}` {type} NOT NULL COMMENT "{comment}"'.format(
            name=self.name, type=self.sql_type, comment=self.sql_comment
        )

    @property
    def original_name(self) -> str:
        """
        Return the original attribute name before any renaming.

        Returns
        -------
        str
            Original name from attribute_expression or current name.
        """
        if self.attribute_expression is None:
            return self.name
        assert self.attribute_expression.startswith("`")
        return self.attribute_expression.strip("`")


class Heading:
    """
    Table heading containing column metadata.

    Manages attribute information including names, types, constraints,
    and lineage for semantic matching.

    Parameters
    ----------
    attribute_specs : list, optional
        List of attribute specification dictionaries.
    table_info : dict, optional
        Database table information for lazy loading.
    lineage_available : bool, optional
        Whether lineage information is available. Default True.

    Attributes
    ----------
    attributes : dict
        Mapping of attribute names to Attribute objects.
    """

    def __init__(
        self,
        attribute_specs: list[dict] | None = None,
        table_info: dict | None = None,
        lineage_available: bool = True,
    ) -> None:
        self.indexes = None
        self.table_info = table_info
        self._table_status = None
        self._lineage_available = lineage_available
        self._attributes = None if attribute_specs is None else dict((q["name"], Attribute(**q)) for q in attribute_specs)

    @property
    def lineage_available(self) -> bool:
        """Whether lineage tracking is available for this heading's schema."""
        return self._lineage_available

    def __len__(self) -> int:
        return 0 if self.attributes is None else len(self.attributes)

    @property
    def table_status(self) -> dict | None:
        """Table status information from database."""
        if self.table_info is None:
            return None
        if self._table_status is None:
            self._init_from_database()
        return self._table_status

    @property
    def attributes(self) -> dict[str, Attribute]:
        """
        Mapping of attribute names to Attribute objects.

        Excludes hidden attributes (names starting with ``_``).
        """
        if self._attributes is None:
            self._init_from_database()  # lazy loading from database
        return {k: v for k, v in self._attributes.items() if not v.is_hidden}

    @property
    def names(self) -> list[str]:
        """List of visible attribute names."""
        return [k for k in self.attributes]

    @property
    def primary_key(self) -> list[str]:
        """List of primary key attribute names."""
        return [k for k, v in self.attributes.items() if v.in_key]

    @property
    def secondary_attributes(self) -> list[str]:
        """List of non-primary-key attribute names."""
        return [k for k, v in self.attributes.items() if not v.in_key]

    def determines(self, other: Heading) -> bool:
        """
        Check if self determines other (self → other).

        A determines B iff every attribute in PK(B) is in A. This means
        knowing A's primary key is sufficient to determine B's primary key
        through functional dependencies.

        Parameters
        ----------
        other : Heading
            Another Heading object.

        Returns
        -------
        bool
            True if self determines other.
        """
        self_attrs = set(self.names)
        return all(attr in self_attrs for attr in other.primary_key)

    @property
    def blobs(self) -> list[str]:
        """List of blob attribute names."""
        return [k for k, v in self.attributes.items() if v.is_blob]

    @property
    def non_blobs(self) -> list[str]:
        """Attributes that are not blobs or JSON."""
        return [k for k, v in self.attributes.items() if not (v.is_blob or v.json)]

    @property
    def new_attributes(self) -> list[str]:
        """Attributes with computed expressions (projections)."""
        return [k for k, v in self.attributes.items() if v.attribute_expression is not None]

    def __getitem__(self, name: str) -> Attribute:
        """Get attribute by name."""
        return self.attributes[name]

    def __repr__(self) -> str:
        """Return heading in DataJoint declaration format."""
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
    def has_autoincrement(self) -> bool:
        """Check if any attribute has auto_increment."""
        return any(e.autoincrement for e in self.attributes.values())

    @property
    def as_dtype(self) -> np.dtype:
        """
        Return heading as a numpy dtype.

        Returns
        -------
        numpy.dtype
            Structured dtype for creating numpy arrays.
        """
        return np.dtype(dict(names=self.names, formats=[v.dtype for v in self.attributes.values()]))

    def as_sql(self, fields: list[str], include_aliases: bool = True) -> str:
        """
        Generate SQL SELECT clause for specified fields.

        Parameters
        ----------
        fields : list[str]
            Attribute names to include.
        include_aliases : bool, optional
            Include AS clauses for computed attributes. Default True.

        Returns
        -------
        str
            Comma-separated SQL field list.
        """
        # Get adapter for proper identifier quoting
        adapter = self.table_info["conn"].adapter if self.table_info else None

        def quote(name):
            return adapter.quote_identifier(name) if adapter else f"`{name}`"

        return ",".join(
            (
                quote(name)
                if self.attributes[name].attribute_expression is None
                else self.attributes[name].attribute_expression + (f" as {quote(name)}" if include_aliases else "")
            )
            for name in fields
        )

    def __iter__(self):
        return iter(self.attributes)

    def _init_from_database(self) -> None:
        """Initialize heading from an existing database table."""
        conn, database, table_name, context = (self.table_info[k] for k in ("conn", "database", "table_name", "context"))
        adapter = conn.adapter

        # Get table metadata
        info = conn.query(
            adapter.get_table_info_sql(database, table_name),
            as_dict=True,
        ).fetchone()
        if info is None:
            raise DataJointError(
                "The table `{database}`.`{table_name}` is not defined.".format(table_name=table_name, database=database)
            )
        # Normalize table_comment to comment for backward compatibility
        self._table_status = {k.lower(): v for k, v in info.items()}
        if "table_comment" in self._table_status:
            self._table_status["comment"] = self._table_status["table_comment"]

        # Get column information
        cur = conn.query(
            adapter.get_columns_sql(database, table_name),
            as_dict=True,
        )

        # Parse columns using adapter-specific parser
        raw_attributes = cur.fetchall()
        attributes = [adapter.parse_column_info(row) for row in raw_attributes]

        # Get primary key information and mark primary key columns
        pk_query = conn.query(
            adapter.get_primary_key_sql(database, table_name),
            as_dict=True,
        )
        pk_columns = {row["column_name"] for row in pk_query.fetchall()}
        for attr in attributes:
            if attr["name"] in pk_columns:
                attr["key"] = "PRI"

        numeric_types = {
            # MySQL types
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
            # PostgreSQL types
            ("integer", False): np.int64,
            ("integer", True): np.int64,
            ("real", False): np.float64,
            ("real", True): np.float64,
            ("double precision", False): np.float64,
            ("double precision", True): np.float64,
        }

        sql_literals = ["CURRENT_TIMESTAMP"]

        # additional attribute properties
        for attr in attributes:
            attr.update(
                in_key=(attr["key"] == "PRI"),
                nullable=attr["nullable"],  # Already boolean from parse_column_info
                autoincrement=bool(re.search(r"auto_increment", attr["extra"], flags=re.I)),
                numeric=any(TYPE_PATTERN[t].match(attr["type"]) for t in ("DECIMAL", "INTEGER", "FLOAT")),
                string=any(TYPE_PATTERN[t].match(attr["type"]) for t in ("ENUM", "TEMPORAL", "STRING")),
                is_blob=any(TYPE_PATTERN[t].match(attr["type"]) for t in ("BYTES", "NATIVE_BLOB")),
                uuid=False,
                json=bool(TYPE_PATTERN["JSON"].match(attr["type"])),
                codec=None,
                store=None,
                attribute_expression=None,
                is_hidden=attr["name"].startswith("_"),
                original_type=None,  # May be set later for core type aliases
            )

            if any(TYPE_PATTERN[t].match(attr["type"]) for t in ("INTEGER", "FLOAT")):
                attr["type"] = re.sub(r"\(\d+\)", "", attr["type"], count=1)  # strip size off integers and floats
            attr["unsupported"] = not any((attr["is_blob"], attr["numeric"], attr["numeric"]))
            attr.pop("extra")
            attr.pop("key")

            # process custom DataJoint types stored in comment
            comment = attr["comment"] or ""  # Handle None for PostgreSQL
            special = re.match(r":(?P<type>[^:]+):(?P<comment>.*)", comment)
            if special:
                special = special.groupdict()
                attr["comment"] = special["comment"]  # Always update the comment
                # Only update the type for adapted types (angle brackets)
                # Core types (uuid, float32, etc.) keep the database type for SQL
                if special["type"].startswith("<"):
                    attr["type"] = special["type"]
                else:
                    # Store the original type name for display but keep db_type for SQL
                    attr["original_type"] = special["type"]

            # process Codecs (types in angle brackets)
            if special and TYPE_PATTERN["CODEC"].match(attr["type"]):
                # Context can be None for built-in types that are globally registered
                codec_spec = special["type"]
                try:
                    codec_instance, codec_store = lookup_codec(codec_spec)
                    attr["codec"] = codec_instance
                    if codec_store is not None:
                        attr["store"] = codec_store
                except DataJointError:
                    # if no codec, then delay the error until the first invocation
                    attr["codec"] = _MissingType(codec_spec)
                else:
                    # Determine if in-store storage based on store presence
                    is_store = attr.get("store") is not None
                    attr["type"] = attr["codec"].get_dtype(is_store=is_store)
                    if not any(r.match(attr["type"]) for r in TYPE_PATTERN.values()):
                        raise DataJointError(f"Invalid dtype '{attr['type']}' in codec <{codec_spec}>.")
                    # Update is_blob based on resolved dtype (check both BYTES and NATIVE_BLOB patterns)
                    attr["is_blob"] = any(TYPE_PATTERN[t].match(attr["type"]) for t in ("BYTES", "NATIVE_BLOB"))

            # Handle core type aliases (uuid, float32, etc.)
            if special:
                # Check original_type for core type detection (not attr["type"] which is now db type)
                original_type = attr["original_type"] or attr["type"]
                try:
                    category = next(c for c in SPECIAL_TYPES if TYPE_PATTERN[c].match(original_type))
                except StopIteration:
                    if original_type.startswith("external"):
                        raise DataJointError(
                            f"Legacy datatype `{original_type}`. See migration guide: "
                            "https://docs.datajoint.com/how-to/migrate-from-0x/"
                        )
                    # Not a special type - that's fine, could be native passthrough
                    category = None

                if category == "UUID":
                    attr["uuid"] = True
                elif category in CORE_TYPE_NAMES:
                    # Core type alias - already resolved in DB
                    pass

            # Check primary key constraints
            if attr["in_key"] and (attr["is_blob"] or attr["json"]):
                raise DataJointError("Blob or JSON attributes are not allowed in the primary key")

            if attr["string"] and attr["default"] is not None and attr["default"] not in sql_literals:
                attr["default"] = '"%s"' % attr["default"]

            if attr["nullable"]:  # nullable fields always default to null
                attr["default"] = "null"

            # fill out dtype. All floats and non-nullable integers are turned into specific dtypes
            attr["dtype"] = object
            if attr["numeric"] and not attr["codec"]:
                is_integer = TYPE_PATTERN["INTEGER"].match(attr["type"])
                is_float = TYPE_PATTERN["FLOAT"].match(attr["type"])
                if is_integer and not attr["nullable"] or is_float:
                    is_unsigned = bool(re.match("sunsigned", attr["type"], flags=re.I))
                    t = re.sub(r"\(.*\)", "", attr["type"])  # remove parentheses
                    t = re.sub(r" unsigned$", "", t)  # remove unsigned
                    assert (t, is_unsigned) in numeric_types, "dtype not found for type %s" % t
                    attr["dtype"] = numeric_types[(t, is_unsigned)]

            if attr["codec"]:
                # restore codec type name for display
                attr["type"] = codec_spec

        # Load lineage information for semantic matching from ~lineage table
        self._lineage_available = lineage_table_exists(conn, database)
        if self._lineage_available:
            lineages = get_table_lineages(conn, database, table_name)
            for attr in attributes:
                attr["lineage"] = lineages.get(attr["name"])
        else:
            for attr in attributes:
                attr["lineage"] = None

        self._attributes = dict(((q["name"], Attribute(**q)) for q in attributes))

        # Read and tabulate secondary indexes
        keys = defaultdict(dict)
        for item in conn.query(
            adapter.get_indexes_sql(database, table_name),
            as_dict=True,
        ):
            # Note: adapter.get_indexes_sql() already filters out PRIMARY key
            # MySQL/PostgreSQL adapters return: index_name, column_name, non_unique
            index_name = item.get("index_name") or item.get("Key_name")
            seq = item.get("seq_in_index") or item.get("Seq_in_index") or len(keys[index_name]) + 1
            column = item.get("column_name") or item.get("Column_name")
            non_unique = item.get("non_unique") or item.get("Non_unique")
            nullable = item.get("nullable") or (item.get("Null", "NO").lower() == "yes")

            keys[index_name][seq] = dict(
                column=column,
                unique=(non_unique == 0 or non_unique == False),
                nullable=nullable,
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
        # Get adapter for proper identifier quoting
        adapter = self.table_info["conn"].adapter if self.table_info else None
        copy_attrs = list()
        for name in self.attributes:
            if name in select_list:
                copy_attrs.append(self.attributes[name].todict())
            copy_attrs.extend(
                (
                    dict(
                        self.attributes[old_name].todict(),
                        name=new_name,
                        attribute_expression=(
                            adapter.quote_identifier(old_name) if adapter else f"`{old_name}`"
                        ),
                    )
                    for new_name, old_name in rename_map.items()
                    if old_name == name
                )
            )
        compute_attrs = (
            dict(default_attribute_properties, name=new_name, attribute_expression=expr)
            for new_name, expr in compute_map.items()
        )
        return Heading(chain(copy_attrs, compute_attrs), lineage_available=self._lineage_available)

    def _join_dependent(self, dependent):
        """Build attribute list when self → dependent: PK = PK(self), self's attrs first."""
        return (
            [self.attributes[name].todict() for name in self.primary_key]
            + [self.attributes[name].todict() for name in self.secondary_attributes]
            + [dependent.attributes[name].todict() for name in dependent.names if name not in self.attributes]
        )

    def join(self, other, nullable_pk=False):
        """
        Join two headings into a new one.

        The primary key of the result depends on functional dependencies:
        - A → B: PK = PK(A), A's attributes first
        - B → A (not A → B): PK = PK(B), B's attributes first
        - Both: PK = PK(A), left operand takes precedence
        - Neither: PK = PK(A) ∪ PK(B), A's PK first then B's new PK attrs

        :param nullable_pk: If True, skip PK optimization and use combined PK from both
            operands. Used for left joins that bypass the A → B constraint, where the
            right operand's PK attributes could be NULL.

        It assumes that self and other are headings that share no common dependent attributes.
        """
        if nullable_pk:
            a_determines_b = b_determines_a = False
        else:
            a_determines_b = self.determines(other)
            b_determines_a = other.determines(self)

        if a_determines_b:
            attrs = self._join_dependent(other)
        elif b_determines_a:
            attrs = other._join_dependent(self)
        else:
            # Neither direction: PK = PK(A) ∪ PK(B)
            self_pk_set = set(self.primary_key)
            other_pk_set = set(other.primary_key)
            attrs = (
                [self.attributes[name].todict() for name in self.primary_key]
                + [dict(other.attributes[name].todict(), in_key=True) for name in other.primary_key if name not in self_pk_set]
                + [self.attributes[name].todict() for name in self.secondary_attributes if name not in other_pk_set]
                + [other.attributes[name].todict() for name in other.secondary_attributes if name not in self_pk_set]
            )

        return Heading(attrs, lineage_available=self._lineage_available and other._lineage_available)

    def set_primary_key(self, primary_key):
        """
        Create a new heading with the specified primary key.
        This low-level method performs no error checking.
        """
        return Heading(
            chain(
                (dict(self.attributes[name].todict(), in_key=True) for name in primary_key),
                (dict(self.attributes[name].todict(), in_key=False) for name in self.names if name not in primary_key),
            ),
            lineage_available=self._lineage_available,
        )

    def make_subquery_heading(self):
        """
        Create a new heading with removed attribute sql_expressions.
        Used by subqueries, which resolve the sql_expressions.
        """
        return Heading(
            (dict(v.todict(), attribute_expression=None) for v in self.attributes.values()),
            lineage_available=self._lineage_available,
        )
