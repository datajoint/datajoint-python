import numpy as np
from collections import namedtuple, defaultdict
from itertools import chain
import re
import logging
from .errors import DataJointError, _support_filepath_types, FILEPATH_FEATURE_SWITCH
from .declare import (
    UUID_DATA_TYPE,
    SPECIAL_TYPES,
    TYPE_PATTERN,
    EXTERNAL_TYPES,
    NATIVE_TYPES,
)
from .attribute_adapter import get_adapter, AttributeAdapter


logger = logging.getLogger(__name__.split(".")[0])

default_attribute_properties = (
    dict(  # these default values are set in computed attributes
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
        adapter=None,
        store=None,
        unsupported=False,
        attribute_expression=None,
        database=None,
        dtype=object,
    )
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
        return self._attributes

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
