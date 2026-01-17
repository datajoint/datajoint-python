"""
Table definition parsing and SQL generation.

This module converts DataJoint table definitions into MySQL CREATE TABLE
statements, handling type mapping, foreign key resolution, and index creation.
"""

from __future__ import annotations

import logging
import re

import pyparsing as pp

from .codecs import lookup_codec
from .condition import translate_attribute
from .errors import DataJointError
from .settings import config

# Core DataJoint types - scientist-friendly names that are fully supported
# These are recorded in field comments using :type: syntax for reconstruction
# Format: pattern_name -> (regex_pattern, mysql_type or None if same as matched)
CORE_TYPES = {
    # Numeric types (aliased to native SQL)
    "float32": (r"float32$", "float"),
    "float64": (r"float64$", "double"),
    "int64": (r"int64$", "bigint"),
    "int32": (r"int32$", "int"),
    "int16": (r"int16$", "smallint"),
    "int8": (r"int8$", "tinyint"),
    "bool": (r"bool$", "tinyint"),
    # UUID (stored as binary)
    "uuid": (r"uuid$", "binary(16)"),
    # JSON
    "json": (r"json$", None),  # json passes through as-is
    # Binary (bytes maps to longblob in MySQL, bytea in PostgreSQL)
    "bytes": (r"bytes$", "longblob"),
    # Temporal
    "date": (r"date$", None),
    "datetime": (r"datetime(\s*\(\d+\))?$", None),  # datetime with optional fractional seconds precision
    # String types (with parameters)
    "char": (r"char\s*\(\d+\)$", None),
    "varchar": (r"varchar\s*\(\d+\)$", None),
    # Enumeration
    "enum": (r"enum\s*\(.+\)$", None),
    # Fixed-point decimal
    "decimal": (r"decimal\s*\(\d+\s*,\s*\d+\)$", None),
}

# Compile core type patterns
CORE_TYPE_PATTERNS = {name: re.compile(pattern, re.I) for name, (pattern, _) in CORE_TYPES.items()}

# Get SQL mapping for core types
CORE_TYPE_SQL = {name: sql_type for name, (_, sql_type) in CORE_TYPES.items()}

MAX_TABLE_NAME_LENGTH = 64
CONSTANT_LITERALS = {
    "CURRENT_TIMESTAMP",
    "NULL",
}  # SQL literals to be used without quotes (case insensitive)

# Type patterns for declaration parsing
TYPE_PATTERN = {
    k: re.compile(v, re.I)
    for k, v in dict(
        # Core DataJoint types
        **{name.upper(): pattern for name, (pattern, _) in CORE_TYPES.items()},
        # Native SQL types (passthrough with warning for non-standard use)
        INTEGER=r"((tiny|small|medium|big|)int|integer)(\s*\(.+\))?(\s+unsigned)?(\s+auto_increment)?|serial$",
        NUMERIC=r"numeric(\s*\(.+\))?(\s+unsigned)?$",  # numeric is SQL alias, use decimal instead
        FLOAT=r"(double|float|real)(\s*\(.+\))?(\s+unsigned)?$",
        STRING=r"(var)?char\s*\(.+\)$",  # Catches char/varchar not matched by core types
        TEMPORAL=r"(time|timestamp|year)(\s*\(.+\))?$",  # time, timestamp, year (not date/datetime)
        NATIVE_BLOB=r"(tiny|small|medium|long)blob$",  # Specific blob variants
        NATIVE_TEXT=r"(tiny|small|medium|long)?text$",  # Native text types (not portable)
        # Codecs use angle brackets
        CODEC=r"<.+>$",
    ).items()
}

# Core types are stored in attribute comment for reconstruction
CORE_TYPE_NAMES = {name.upper() for name in CORE_TYPES}

# Special types that need comment storage (core types + adapted)
SPECIAL_TYPES = CORE_TYPE_NAMES | {"CODEC"}

# Native SQL types that pass through (with optional warning)
NATIVE_TYPES = set(TYPE_PATTERN) - SPECIAL_TYPES

assert SPECIAL_TYPES <= set(TYPE_PATTERN)


def match_type(attribute_type: str) -> str:
    """
    Match an attribute type string to its category.

    Parameters
    ----------
    attribute_type : str
        The type string from the table definition (e.g., ``"float32"``, ``"varchar(255)"``).

    Returns
    -------
    str
        Category name from TYPE_PATTERN (e.g., ``"FLOAT32"``, ``"STRING"``, ``"CODEC"``).

    Raises
    ------
    DataJointError
        If the type string doesn't match any known pattern.
    """
    try:
        return next(category for category, pattern in TYPE_PATTERN.items() if pattern.match(attribute_type))
    except StopIteration:
        raise DataJointError("Unsupported attribute type {type}".format(type=attribute_type))


logger = logging.getLogger(__name__.split(".")[0])


def build_foreign_key_parser() -> pp.ParserElement:
    """
    Build a pyparsing parser for foreign key definitions.

    Returns
    -------
    pp.ParserElement
        Parser that extracts ``options`` and ``ref_table`` from lines like
        ``-> [nullable] ParentTable``.
    """
    arrow = pp.Literal("->").suppress()
    lbracket = pp.Literal("[").suppress()
    rbracket = pp.Literal("]").suppress()
    option = pp.Word(pp.srange("[a-zA-Z]"))
    options = pp.Optional(lbracket + pp.DelimitedList(option) + rbracket).set_results_name("options")
    ref_table = pp.restOfLine.set_results_name("ref_table")
    return arrow + options + ref_table


def build_attribute_parser() -> pp.ParserElement:
    """
    Build a pyparsing parser for attribute definitions.

    Returns
    -------
    pp.ParserElement
        Parser that extracts ``name``, ``type``, ``default``, and ``comment``
        from attribute definition lines.
    """
    quoted = pp.QuotedString('"') ^ pp.QuotedString("'")
    colon = pp.Literal(":").suppress()
    attribute_name = pp.Word(pp.srange("[a-z]"), pp.srange("[a-z0-9_]")).set_results_name("name")
    data_type = (
        pp.Combine(pp.Word(pp.alphas) + pp.SkipTo("#", ignore=quoted))
        ^ pp.QuotedString("<", end_quote_char=">", unquote_results=False)
    ).set_results_name("type")
    default = pp.Literal("=").suppress() + pp.SkipTo(colon, ignore=quoted).set_results_name("default")
    comment = pp.Literal("#").suppress() + pp.restOfLine.set_results_name("comment")
    return attribute_name + pp.Optional(default) + colon + data_type + comment


foreign_key_parser = build_foreign_key_parser()
attribute_parser = build_attribute_parser()


def is_foreign_key(line: str) -> bool:
    """
    Check if a definition line is a foreign key reference.

    Parameters
    ----------
    line : str
        A line from the table definition.

    Returns
    -------
    bool
        True if the line appears to be a foreign key definition (contains ``->``
        not inside quotes or comments).
    """
    arrow_position = line.find("->")
    return arrow_position >= 0 and not any(c in line[:arrow_position] for c in "\"#'")


def compile_foreign_key(
    line: str,
    context: dict,
    attributes: list[str],
    primary_key: list[str] | None,
    attr_sql: list[str],
    foreign_key_sql: list[str],
    index_sql: list[str],
    fk_attribute_map: dict[str, tuple[str, str]] | None = None,
) -> None:
    """
    Parse a foreign key line and update declaration components.

    Parameters
    ----------
    line : str
        A foreign key line from the table definition (e.g., ``"-> Parent"``).
    context : dict
        Namespace containing referenced table objects.
    attributes : list[str]
        Attribute names already declared. Updated in place with new FK attributes.
    primary_key : list[str] or None
        Primary key attributes so far. None if in dependent section.
        Updated in place with FK attributes when not None.
    attr_sql : list[str]
        SQL attribute definitions. Updated in place.
    foreign_key_sql : list[str]
        SQL FOREIGN KEY constraints. Updated in place.
    index_sql : list[str]
        SQL INDEX declarations. Updated in place.
    fk_attribute_map : dict, optional
        Mapping of ``child_attr -> (parent_table, parent_attr)``. Updated in place.

    Raises
    ------
    DataJointError
        If the foreign key reference cannot be resolved or options are invalid.
    """
    # Parse and validate
    from .expression import QueryExpression
    from .table import Table

    try:
        result = foreign_key_parser.parse_string(line)
    except pp.ParseException as err:
        raise DataJointError('Parsing error in line "%s". %s.' % (line, err))

    try:
        ref = eval(result.ref_table, context)
    except Exception:
        raise DataJointError("Foreign key reference %s could not be resolved" % result.ref_table)

    options = [opt.upper() for opt in result.options]
    for opt in options:  # check for invalid options
        if opt not in {"NULLABLE", "UNIQUE"}:
            raise DataJointError('Invalid foreign key option "{opt}"'.format(opt=opt))
    is_nullable = "NULLABLE" in options
    is_unique = "UNIQUE" in options
    if is_nullable and primary_key is not None:
        raise DataJointError('Primary dependencies cannot be nullable in line "{line}"'.format(line=line))

    if isinstance(ref, type) and issubclass(ref, Table):
        ref = ref()

    # check that dependency is of a supported type
    if (
        not isinstance(ref, QueryExpression)
        or len(ref.restriction)
        or len(ref.support) != 1
        or not isinstance(ref.support[0], str)
    ):
        raise DataJointError('Dependency "%s" is not supported (yet). Use a base table or its projection.' % result.ref_table)

    # declare new foreign key attributes
    for attr in ref.primary_key:
        if attr not in attributes:
            attributes.append(attr)
            if primary_key is not None:
                primary_key.append(attr)
            attr_sql.append(ref.heading[attr].sql.replace("NOT NULL ", "", int(is_nullable)))
        # Track FK attribute mapping for lineage: child_attr -> (parent_table, parent_attr)
        if fk_attribute_map is not None:
            parent_table = ref.support[0]  # e.g., `schema`.`table`
            parent_attr = ref.heading[attr].original_name
            fk_attribute_map[attr] = (parent_table, parent_attr)

    # declare the foreign key
    foreign_key_sql.append(
        "FOREIGN KEY (`{fk}`) REFERENCES {ref} (`{pk}`) ON UPDATE CASCADE ON DELETE RESTRICT".format(
            fk="`,`".join(ref.primary_key),
            pk="`,`".join(ref.heading[name].original_name for name in ref.primary_key),
            ref=ref.support[0],
        )
    )

    # declare unique index
    if is_unique:
        index_sql.append("UNIQUE INDEX ({attrs})".format(attrs=",".join("`%s`" % attr for attr in ref.primary_key)))


def prepare_declare(
    definition: str, context: dict
) -> tuple[str, list[str], list[str], list[str], list[str], list[str], dict[str, tuple[str, str]]]:
    """
    Parse a table definition into its components.

    Parameters
    ----------
    definition : str
        DataJoint table definition string.
    context : dict
        Namespace for resolving foreign key references.

    Returns
    -------
    tuple
        Seven-element tuple containing:

        - table_comment : str
        - primary_key : list[str]
        - attribute_sql : list[str]
        - foreign_key_sql : list[str]
        - index_sql : list[str]
        - external_stores : list[str]
        - fk_attribute_map : dict[str, tuple[str, str]]
    """
    # split definition into lines
    definition = re.split(r"\s*\n\s*", definition.strip())
    # check for optional table comment
    table_comment = definition.pop(0)[1:].strip() if definition[0].startswith("#") else ""
    if table_comment.startswith(":"):
        raise DataJointError('Table comment must not start with a colon ":"')
    in_key = True  # parse primary keys
    primary_key = []
    attributes = []
    attribute_sql = []
    foreign_key_sql = []
    index_sql = []
    external_stores = []
    fk_attribute_map = {}  # child_attr -> (parent_table, parent_attr)

    for line in definition:
        if not line or line.startswith("#"):  # ignore additional comments
            pass
        elif line.startswith("---") or line.startswith("___"):
            in_key = False  # start parsing dependent attributes
        elif is_foreign_key(line):
            compile_foreign_key(
                line,
                context,
                attributes,
                primary_key if in_key else None,
                attribute_sql,
                foreign_key_sql,
                index_sql,
                fk_attribute_map,
            )
        elif re.match(r"^(unique\s+)?index\s*.*$", line, re.I):  # index
            compile_index(line, index_sql)
        else:
            name, sql, store = compile_attribute(line, in_key, foreign_key_sql, context)
            if store:
                external_stores.append(store)
            if in_key and name not in primary_key:
                primary_key.append(name)
            if name not in attributes:
                attributes.append(name)
                attribute_sql.append(sql)

    return (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        fk_attribute_map,
    )


def declare(
    full_table_name: str, definition: str, context: dict
) -> tuple[str, list[str], list[str], dict[str, tuple[str, str]]]:
    r"""
    Parse a definition and generate SQL CREATE TABLE statement.

    Parameters
    ----------
    full_table_name : str
        Fully qualified table name (e.g., ```\`schema\`.\`table\```).
    definition : str
        DataJoint table definition string.
    context : dict
        Namespace for resolving foreign key references.

    Returns
    -------
    tuple
        Four-element tuple:

        - sql : str - SQL CREATE TABLE statement
        - external_stores : list[str] - External store names used
        - primary_key : list[str] - Primary key attribute names
        - fk_attribute_map : dict - FK attribute lineage mapping

    Raises
    ------
    DataJointError
        If table name exceeds max length or has no primary key.
    """
    table_name = full_table_name.strip("`").split(".")[1]
    if len(table_name) > MAX_TABLE_NAME_LENGTH:
        raise DataJointError(
            "Table name `{name}` exceeds the max length of {max_length}".format(
                name=table_name, max_length=MAX_TABLE_NAME_LENGTH
            )
        )

    (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        fk_attribute_map,
    ) = prepare_declare(definition, context)

    # Add hidden job metadata for Computed/Imported tables (not parts)
    # Note: table_name may still have backticks, strip them for prefix checking
    clean_table_name = table_name.strip("`")
    if config.jobs.add_job_metadata:
        # Check if this is a Computed (__) or Imported (_) table, but not a Part (contains __ in middle)
        is_computed = clean_table_name.startswith("__") and "__" not in clean_table_name[2:]
        is_imported = clean_table_name.startswith("_") and not clean_table_name.startswith("__")
        if is_computed or is_imported:
            job_metadata_sql = [
                "`_job_start_time` datetime(3) DEFAULT NULL",
                "`_job_duration` float DEFAULT NULL",
                "`_job_version` varchar(64) DEFAULT ''",
            ]
            attribute_sql.extend(job_metadata_sql)

    if not primary_key:
        raise DataJointError("Table must have a primary key")

    sql = (
        "CREATE TABLE IF NOT EXISTS %s (\n" % full_table_name
        + ",\n".join(attribute_sql + ["PRIMARY KEY (`" + "`,`".join(primary_key) + "`)"] + foreign_key_sql + index_sql)
        + '\n) ENGINE=InnoDB, COMMENT "%s"' % table_comment
    )
    return sql, external_stores, primary_key, fk_attribute_map


def _make_attribute_alter(new: list[str], old: list[str], primary_key: list[str]) -> list[str]:
    """
    Generate SQL ALTER commands for attribute changes.

    Parameters
    ----------
    new : list[str]
        New attribute SQL declarations.
    old : list[str]
        Old attribute SQL declarations.
    primary_key : list[str]
        Primary key attribute names (cannot be altered).

    Returns
    -------
    list[str]
        SQL ALTER commands (ADD, MODIFY, CHANGE, DROP).

    Raises
    ------
    DataJointError
        If an attribute is renamed twice or renamed from non-existent attribute.
    """
    # parse attribute names
    name_regexp = re.compile(r"^`(?P<name>\w+)`")
    original_regexp = re.compile(r'COMMENT "{\s*(?P<name>\w+)\s*}')
    matched = ((name_regexp.match(d), original_regexp.search(d)) for d in new)
    new_names = dict((d.group("name"), n and n.group("name")) for d, n in matched)
    old_names = [name_regexp.search(d).group("name") for d in old]

    # verify that original names are only used once
    renamed = set()
    for v in new_names.values():
        if v:
            if v in renamed:
                raise DataJointError("Alter attempted to rename attribute {%s} twice." % v)
            renamed.add(v)

    # verify that all renamed attributes existed in the old definition
    try:
        raise DataJointError(
            "Attribute {} does not exist in the original definition".format(
                next(attr for attr in renamed if attr not in old_names)
            )
        )
    except StopIteration:
        pass

    # dropping attributes
    to_drop = [n for n in old_names if n not in renamed and n not in new_names]
    sql = ["DROP `%s`" % n for n in to_drop]
    old_names = [name for name in old_names if name not in to_drop]

    # add or change attributes in order
    prev = None
    for new_def, (new_name, old_name) in zip(new, new_names.items()):
        if new_name not in primary_key:
            after = None  # if None, then must include the AFTER clause
            if prev:
                try:
                    idx = old_names.index(old_name or new_name)
                except ValueError:
                    after = prev[0]
                else:
                    if idx >= 1 and old_names[idx - 1] != (prev[1] or prev[0]):
                        after = prev[0]
            if new_def not in old or after:
                sql.append(
                    "{command} {new_def} {after}".format(
                        command=(
                            "ADD"
                            if (old_name or new_name) not in old_names
                            else "MODIFY"
                            if not old_name
                            else "CHANGE `%s`" % old_name
                        ),
                        new_def=new_def,
                        after="" if after is None else "AFTER `%s`" % after,
                    )
                )
        prev = new_name, old_name

    return sql


def alter(definition: str, old_definition: str, context: dict) -> tuple[list[str], list[str]]:
    """
    Generate SQL ALTER commands for table definition changes.

    Parameters
    ----------
    definition : str
        New table definition.
    old_definition : str
        Current table definition.
    context : dict
        Namespace for resolving foreign key references.

    Returns
    -------
    tuple
        Two-element tuple:

        - sql : list[str] - SQL ALTER commands
        - new_stores : list[str] - New external stores used

    Raises
    ------
    NotImplementedError
        If attempting to alter primary key, foreign keys, or indexes.
    """
    (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        _fk_attribute_map,
    ) = prepare_declare(definition, context)
    (
        table_comment_,
        primary_key_,
        attribute_sql_,
        foreign_key_sql_,
        index_sql_,
        external_stores_,
        _fk_attribute_map_,
    ) = prepare_declare(old_definition, context)

    # analyze differences between declarations
    sql = list()
    if primary_key != primary_key_:
        raise NotImplementedError("table.alter cannot alter the primary key (yet).")
    if foreign_key_sql != foreign_key_sql_:
        raise NotImplementedError("table.alter cannot alter foreign keys (yet).")
    if index_sql != index_sql_:
        raise NotImplementedError("table.alter cannot alter indexes (yet)")
    if attribute_sql != attribute_sql_:
        sql.extend(_make_attribute_alter(attribute_sql, attribute_sql_, primary_key))
    if table_comment != table_comment_:
        sql.append('COMMENT="%s"' % table_comment)
    return sql, [e for e in external_stores if e not in external_stores_]


def compile_index(line: str, index_sql: list[str]) -> None:
    """
    Parse an index declaration and append SQL to index_sql.

    Parameters
    ----------
    line : str
        Index declaration line (e.g., ``"index(attr1, attr2)"`` or
        ``"unique index(attr)"``).
    index_sql : list[str]
        List of index SQL declarations. Updated in place.

    Raises
    ------
    DataJointError
        If the index syntax is invalid.
    """

    def format_attribute(attr):
        match, attr = translate_attribute(attr)
        if match is None:
            return attr
        if match["path"] is None:
            return f"`{attr}`"
        return f"({attr})"

    match = re.match(r"(?P<unique>unique\s+)?index\s*\(\s*(?P<args>.*)\)", line, re.I)
    if match is None:
        raise DataJointError(f'Table definition syntax error in line "{line}"')
    match = match.groupdict()

    attr_list = re.findall(r"(?:[^,(]|\([^)]*\))+", match["args"])
    index_sql.append(
        "{unique}index ({attrs})".format(
            unique="unique " if match["unique"] else "",
            attrs=",".join(format_attribute(a.strip()) for a in attr_list),
        )
    )


def substitute_special_type(match: dict, category: str, foreign_key_sql: list[str], context: dict) -> None:
    """
    Substitute special types with their native SQL equivalents.

    Special types include core DataJoint types (``float32`` → ``float``,
    ``uuid`` → ``binary(16)``, ``bytes`` → ``longblob``) and codec types
    (angle bracket syntax like ``<array>``).

    Parameters
    ----------
    match : dict
        Parsed attribute with keys ``"type"``, ``"comment"``, etc.
        Modified in place with substituted type.
    category : str
        Type category from TYPE_PATTERN (e.g., ``"FLOAT32"``, ``"CODEC"``).
    foreign_key_sql : list[str]
        Foreign key declarations (unused, kept for API compatibility).
    context : dict
        Namespace for codec lookup (unused, kept for API compatibility).
    """
    if category == "CODEC":
        # Codec - resolve to underlying dtype
        codec, store_name = lookup_codec(match["type"])
        if store_name is not None:
            match["store"] = store_name
        # Determine if in-store storage is used (store_name is present, even if empty string for default)
        is_store = store_name is not None
        inner_dtype = codec.get_dtype(is_store=is_store)

        # If inner dtype is a codec without store, propagate the store from outer type
        # e.g., <attach@mystore> returns <hash>, we need to resolve as <hash@mystore>
        if inner_dtype.startswith("<") and "@" not in inner_dtype and match.get("store") is not None:
            # Append store to the inner dtype
            inner_dtype = inner_dtype[:-1] + "@" + match["store"] + ">"

        match["type"] = inner_dtype
        # Recursively resolve if dtype is also a special type
        category = match_type(match["type"])
        if category in SPECIAL_TYPES:
            substitute_special_type(match, category, foreign_key_sql, context)
    elif category in CORE_TYPE_NAMES:
        # Core DataJoint type - substitute with native SQL type if mapping exists
        core_name = category.lower()
        sql_type = CORE_TYPE_SQL.get(core_name)
        if sql_type is not None:
            match["type"] = sql_type
        # else: type passes through as-is (json, date, datetime, char, varchar, enum)
    else:
        raise DataJointError(f"Unknown special type: {category}")


def compile_attribute(line: str, in_key: bool, foreign_key_sql: list[str], context: dict) -> tuple[str, str, str | None]:
    """
    Convert an attribute definition from DataJoint format to SQL.

    Parameters
    ----------
    line : str
        Attribute definition line (e.g., ``"session_id : int32  # unique session"``).
    in_key : bool
        True if the attribute is part of the primary key.
    foreign_key_sql : list[str]
        Foreign key declarations (passed to type substitution).
    context : dict
        Namespace for codec lookup.

    Returns
    -------
    tuple
        Three-element tuple:

        - name : str - Attribute name
        - sql : str - SQL column declaration
        - store : str or None - External store name if applicable

    Raises
    ------
    DataJointError
        If syntax is invalid, primary key is nullable, or blob has invalid default.
    """
    try:
        match = attribute_parser.parse_string(line + "#", parse_all=True)
    except pp.ParseException as err:
        raise DataJointError(
            "Declaration error in position {pos} in line:\n  {line}\n{msg}".format(
                line=err.args[0], pos=err.args[1], msg=err.args[2]
            )
        )
    match["comment"] = match["comment"].rstrip("#")
    if "default" not in match:
        match["default"] = ""
    match = {k: v.strip() for k, v in match.items()}
    match["nullable"] = match["default"].lower() == "null"

    if match["nullable"]:
        if in_key:
            raise DataJointError('Primary key attributes cannot be nullable in line "%s"' % line)
        match["default"] = "DEFAULT NULL"  # nullable attributes default to null
    else:
        if match["default"]:
            quote = match["default"].split("(")[0].upper() not in CONSTANT_LITERALS and match["default"][0] not in "\"'"
            match["default"] = "NOT NULL DEFAULT " + ('"%s"' if quote else "%s") % match["default"]
        else:
            match["default"] = "NOT NULL"

    match["comment"] = match["comment"].replace('"', '\\"')  # escape double quotes in comment

    if match["comment"].startswith(":"):
        raise DataJointError('An attribute comment must not start with a colon in comment "{comment}"'.format(**match))

    category = match_type(match["type"])

    if category in SPECIAL_TYPES:
        # Core types and Codecs are recorded in comment for reconstruction
        match["comment"] = ":{type}:{comment}".format(**match)
        substitute_special_type(match, category, foreign_key_sql, context)
    elif category in NATIVE_TYPES:
        # Native type - warn user
        logger.warning(
            f"Native type '{match['type']}' is used in attribute '{match['name']}'. "
            "Consider using a core DataJoint type for better portability."
        )

    # Check for invalid default values on blob types (after type substitution)
    # Note: blob → longblob, so check for NATIVE_BLOB or longblob result
    final_type = match["type"].lower()
    if ("blob" in final_type) and match["default"] not in {"DEFAULT NULL", "NOT NULL"}:
        raise DataJointError("The default value for blob attributes can only be NULL in:\n{line}".format(line=line))

    sql = ("`{name}` {type} {default}" + (' COMMENT "{comment}"' if match["comment"] else "")).format(**match)
    return match["name"], sql, match.get("store")
