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
    # JSON (matches both json and jsonb for PostgreSQL compatibility)
    "json": (r"jsonb?$", None),  # json/jsonb passes through as-is
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
        INTEGER=r"(((tiny|small|medium|big|)int|integer)(\s*\(.+\))?(\s+unsigned)?(\s+auto_increment)?|serial)$",
        # decimal/numeric/dec/fixed are the same SQL type. The canonical core spelling
        # decimal(M,D) is matched above; anything carrying a modifier or an alias falls
        # through to here and passes as a native type, as int/float unsigned do.
        NUMERIC=r"(decimal|numeric|dec|fixed)(\s*\(.+\))?(\s+unsigned)?(\s+zerofill)?$",
        FLOAT=r"(double|float|real)(\s*\(.+\))?(\s+unsigned)?$",
        STRING=r"(var)?char\s*\(.+\)$",  # Catches char/varchar not matched by core types
        TEMPORAL=r"(time|timestamp|year)(\s*\(.+\))?$",  # time, timestamp, year (not date/datetime)
        # Size prefix is optional: bare `blob` is a MySQL type in its own right, and is
        # already recognized by migrate.BLOB_TYPES.
        NATIVE_BLOB=r"(tiny|small|medium|long)?blob$",
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
    # fullmatch, not match: a declared type must be consumed in its entirety. With
    # a prefix match, a misspelling such as `int24` or `tinyinteger` classifies as a
    # native integer and is emitted into the DDL verbatim, so the failure surfaces as
    # a SQL syntax error from the server rather than as an unsupported-type error here.
    try:
        return next(category for category, pattern in TYPE_PATTERN.items() if pattern.fullmatch(attribute_type))
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
    # A leading underscore is permitted by the grammar so the framework can
    # declare its own columns in this notation. Whether a *user* may is policy,
    # enforced in prepare_declare where a user's definition is parsed.
    attribute_name = pp.Word(pp.srange("[a-z_]"), pp.srange("[a-z0-9_]")).set_results_name("name")
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


def _column(
    name: str,
    sql_type: str,
    nullable: bool,
    default: str | None,
    comment: str | None,
    sql: str,
    renamed_from: str | None = None,
) -> dict:
    """
    The parts of a declared column, as ``alter`` compares them.

    Parameters
    ----------
    name : str
        Column name.
    sql_type : str
        Backend type, e.g. ``"real"`` or a schema-qualified enum type.
    nullable : bool
        Whether the column accepts NULL.
    default : str or None
        Default as an SQL expression (``"NULL"`` for a nullable attribute,
        ``"'1.5'"``, ``"CURRENT_TIMESTAMP"``), or None when there is none.
    comment : str or None
        Comment as stored, including the ``:type:`` marker of a core type.
    sql : str
        The compiled column declaration.
    renamed_from : str, optional
        The attribute this one renames, from its rename marker (see
        ``_renamed_from``).

    Returns
    -------
    dict
        Keys ``name``, ``type``, ``nullable``, ``default``, ``comment``, ``sql``
        and ``renamed_from``.
    """
    return dict(
        name=name, type=sql_type, nullable=nullable, default=default, comment=comment, sql=sql, renamed_from=renamed_from
    )


def compile_foreign_key(
    line: str,
    context: dict,
    attributes: list[str],
    primary_key: list[str] | None,
    attr_sql: list[str],
    foreign_key_sql: list[str],
    index_sql: list[str],
    adapter,
    fk_attribute_map: dict[str, tuple[str, str]] | None = None,
    fk_index_candidates: list[list[str]] | None = None,
    column_comments: dict[str, str] | None = None,
    columns: dict[str, dict] | None = None,
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
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.
    fk_attribute_map : dict, optional
        Mapping of ``child_attr -> (parent_table, parent_attr)``. Updated in place.
    fk_index_candidates : list, optional
        PostgreSQL only. Collects each non-unique foreign key's columns as a
        candidate supporting index; the redundancy/coverage decision is made
        post-parse (once the full primary key and index list are known).
        Updated in place.
    column_comments : dict, optional
        Mapping of ``column -> comment`` for backends that set column comments
        out of line (PostgreSQL's ``COMMENT ON COLUMN``). Each copied column's
        comment, with its core-type marker (e.g. ``:int32:``), is added so the
        child keeps the parent's declared type. Updated in place.
    columns : dict, optional
        Mapping of ``column -> parts`` (see ``_column``) for ``alter``.
        Updated in place.

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

            # Build foreign key column definition using adapter
            parent_attr = ref.heading[attr]
            sql_type = parent_attr.sql_type
            # For PostgreSQL enum types, qualify with schema name
            # Enum type names start with "enum_" (generated hash-based names)
            if sql_type.startswith("enum_") and adapter.backend == "postgresql":
                sql_type = f"{adapter.quote_identifier(ref.database)}.{adapter.quote_identifier(sql_type)}"
            comment = parent_attr.sql_comment
            col_def = adapter.format_column_definition(
                name=attr,
                sql_type=sql_type,
                nullable=is_nullable,
                default=None,
                comment=comment,
            )
            attr_sql.append(col_def)
            if comment and column_comments is not None:
                column_comments[attr] = comment
            if columns is not None:
                columns[attr] = _column(attr, sql_type, is_nullable, None, comment or None, col_def)

        # Track FK attribute mapping for lineage: child_attr -> (parent_table, parent_attr)
        if fk_attribute_map is not None:
            parent_table = ref.support[0]  # e.g., `schema`.`table`
            parent_attr = ref.heading[attr].original_name
            fk_attribute_map[attr] = (parent_table, parent_attr)

    # declare the foreign key using adapter for identifier quoting
    fk_cols = ", ".join(adapter.quote_identifier(col) for col in ref.primary_key)
    pk_cols = ", ".join(adapter.quote_identifier(ref.heading[name].original_name) for name in ref.primary_key)

    # Build referenced table name with proper quoting
    # ref.support[0] may have cached quoting from a different backend
    # Extract database and table name and rebuild with current adapter
    parent_full_name = ref.support[0]
    # Parse as database.table using the adapter's quoting convention
    parts = adapter.split_full_table_name(parent_full_name)
    ref_table_name = adapter.make_full_table_name(parts[0], parts[1])

    foreign_key_sql.append(
        f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table_name} ({pk_cols}) ON UPDATE CASCADE ON DELETE RESTRICT"
    )

    # Supporting index on the foreign-key columns.
    #
    # The `unique` option is a uniqueness constraint, always emitted, on every
    # backend. For the non-unique case: MySQL/InnoDB indexes every foreign key
    # implicitly, but PostgreSQL never indexes the referencing (child) columns
    # and offers no server setting to make it (see #1512), so DataJoint must.
    # Whether that index is *redundant* (the columns are already a left-prefix of
    # the primary key or of another declared index) can only be decided once the
    # whole definition is parsed, so here we merely record the candidate; the
    # coverage decision happens post-parse in `prepare_declare`.
    fk_attrs = list(ref.primary_key)
    if is_unique:
        index_cols = ", ".join(adapter.quote_identifier(attr) for attr in fk_attrs)
        index_sql.append(f"UNIQUE INDEX ({index_cols})")
    elif not adapter.auto_indexes_foreign_keys and fk_index_candidates is not None:
        fk_index_candidates.append(fk_attrs)


# =============================================================================
# Platform-managed attributes
#
# Written in DataJoint notation and compiled by the same `compile_attribute` as
# any user attribute, so the backend type mapping, the `:type:` comment and the
# column-comment bookkeeping all come from the one path that owns them.
# =============================================================================

#: Primary key for a table that declares none of its own.
SINGLETON_DEFINITION = "_singleton = 1 : bool # singleton primary key"

#: Per-row execution record on the auto-populated tiers.
JOB_METADATA_DEFINITION = (
    "_job_start_time = null : datetime(3) # when computation began",
    "_job_duration = null : float32 # computation duration in seconds",
    '_job_version = "" : varchar(64) # code version',
)

#: Extrinsic provenance on Entry tables, where rows arrive from outside.
PROV_DEFINITION = "_prov = null : json # extrinsic provenance for a row that entered from outside"


def _tier_attributes(table_name: str | None, config) -> tuple[str, ...]:
    """The platform-managed attributes a tier receives, in DataJoint notation."""
    from .user_tables import Computed, Imported, Manual, is_tier

    if table_name is None:  # `alter` compares two definitions; neither declares these
        return ()
    if config.jobs.add_job_metadata and (is_tier(table_name, Computed) or is_tier(table_name, Imported)):
        return JOB_METADATA_DEFINITION
    if config.provenance.capture and is_tier(table_name, Manual):
        return (PROV_DEFINITION,)
    return ()


def prepare_declare(
    definition: str, context: dict, adapter, *, table_name: str | None = None, config=None, columns: dict | None = None
) -> tuple[str, list[str], list[str], list[str], list[str], list[str], dict[str, tuple[str, str]], dict[str, str]]:
    """
    Parse a table definition into its components.

    The platform-managed attributes are added here too, once the user's lines are
    parsed and by the same ``add_attribute``.  That is what lets ``_singleton``
    be decided from the primary key the parse produced, and leaves the loop
    iterating user lines only, where a user-declared hidden one is refused.

    Parameters
    ----------
    definition : str
        DataJoint table definition string.
    context : dict
        Namespace for resolving foreign key references.
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.
    table_name : str, optional
        Stripped table name, which carries the tier.  Given one, the tier's
        platform attributes are added and ``config`` is required; ``alter``
        passes neither, since it compares two definitions and neither side
        declares them.
    config : Config, optional
        Read for ``jobs.add_job_metadata`` and ``provenance.capture``.
    columns : dict, optional
        Filled in place with ``column -> parts`` (see ``_column``) for every
        attribute, in declaration order.

    Returns
    -------
    tuple
        Eight-element tuple containing:

        - table_comment : str
        - primary_key : list[str]
        - attribute_sql : list[str]
        - foreign_key_sql : list[str]
        - index_sql : list[str]
        - external_stores : list[str]
        - fk_attribute_map : dict[str, tuple[str, str]]
        - column_comments : dict[str, str] - Column name to comment mapping
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
    column_comments = {}  # column_name -> comment (for PostgreSQL COMMENT ON)
    fk_index_candidates = []  # PostgreSQL: FK column-lists that may need a support index (#1512)

    def add_attribute(line: str, in_key: bool) -> None:
        column = {}
        name, sql, store, comment = compile_attribute(line, in_key, foreign_key_sql, context, adapter, column=column)
        if store:
            external_stores.append(store)
        if in_key and name not in primary_key:
            primary_key.append(name)
        if name not in attributes:
            attributes.append(name)
            attribute_sql.append(sql)
            if comment:
                column_comments[name] = comment
            if columns is not None:
                columns[name] = column

    for line in definition:
        if not line or line.startswith("#"):  # ignore additional comments
            pass
        elif line.startswith("---"):
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
                adapter,
                fk_attribute_map,
                fk_index_candidates,
                column_comments,
                columns,
            )
        elif re.match(r"^(unique\s+)?index\s*\(.*\)\s*(#.*)?$", line, re.I):  # index
            compile_index(re.sub(r"\s*#.*$", "", line), index_sql, adapter)
        elif line.startswith("_"):
            # Policy, not grammar: the parser accepts the name so that the
            # framework can declare its own columns in the same notation. Only
            # user lines reach here -- the platform's are added after the loop.
            raise DataJointError(
                f'Attribute name in line "{line}" starts with an underscore. '
                "Names with leading underscore are reserved for platform-managed "
                "columns (e.g. _job_start_time, _singleton). Use a regular "
                "attribute name; if you need to control visibility at the call "
                "site, use proj()."
            )
        else:
            add_attribute(line, in_key)

    # A table that declares no primary key of its own gets the sentinel, which is
    # what makes it hold at most one row. Only the parse knows whether it does:
    # `index (...)` contributes no attribute and `-> Parent` may contribute many.
    if not primary_key:
        add_attribute(SINGLETON_DEFINITION, True)
    for line in _tier_attributes(table_name, config):
        add_attribute(line, False)

    # Foreign-key support indexes (PostgreSQL; #1512). Now that the whole
    # definition is parsed, emit an index on each candidate foreign key's columns
    # UNLESS they are already a left-prefix of the primary key, of a declared
    # index, or of a longer FK-support index already emitted — in which case that
    # existing index already serves foreign-key lookups and cascades. Candidates
    # are only collected on PostgreSQL (MySQL/InnoDB indexes FKs implicitly), so
    # this loop is a no-op elsewhere.
    if fk_index_candidates:

        def _index_columns(index_def: str) -> list[str]:
            match = re.match(r"(?:unique\s+)?index\s*\(([^)]+)\)", index_def, re.I)
            return [col.strip().strip('`"') for col in match.group(1).split(",")] if match else []

        # Existing prefixes an FK index could be redundant against: the primary
        # key and every already-declared index. Grows as we accept FK indexes.
        existing_prefixes = [list(primary_key)] + [_index_columns(s) for s in index_sql]

        def _covered(candidate: list[str]) -> bool:
            return any(prefix[: len(candidate)] == candidate for prefix in existing_prefixes)

        # Longest first, so a shorter FK index left-covered by a longer one is skipped.
        for candidate in sorted(fk_index_candidates, key=len, reverse=True):
            if not _covered(candidate):
                index_cols = ", ".join(adapter.quote_identifier(attr) for attr in candidate)
                index_sql.append(f"INDEX ({index_cols})")
                existing_prefixes.append(candidate)

    return (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        fk_attribute_map,
        column_comments,
    )


def declare(
    full_table_name: str, definition: str, context: dict, adapter, *, config=None
) -> tuple[str, list[str], list[str], dict[str, tuple[str, str]], list[str], list[str]]:
    r"""
    Parse a definition and generate SQL CREATE TABLE statement.

    Parameters
    ----------
    full_table_name : str
        Fully qualified table name (e.g., ```\`schema\`.\`table\``` or ```"schema"."table"```).
    definition : str
        DataJoint table definition string.
    context : dict
        Namespace for resolving foreign key references.
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.
    config : Config, optional
        Configuration object. If None, falls back to global config.

    Returns
    -------
    tuple
        Six-element tuple:

        - sql : str - SQL CREATE TABLE statement
        - external_stores : list[str] - External store names used
        - primary_key : list[str] - Primary key attribute names
        - fk_attribute_map : dict - FK attribute lineage mapping
        - pre_ddl : list[str] - DDL statements to run BEFORE CREATE TABLE (e.g., CREATE TYPE)
        - post_ddl : list[str] - DDL statements to run AFTER CREATE TABLE (e.g., COMMENT ON)

    Raises
    ------
    DataJointError
        If the table name exceeds the backend's max length.
    """
    # Parse table name using adapter (handles backend-specific quoting)
    schema_name, table_name = adapter.split_full_table_name(full_table_name)

    if len(table_name) > adapter.max_table_name_length:
        raise DataJointError(
            "Table name `{name}` exceeds the max length of {max_length}".format(
                name=table_name, max_length=adapter.max_table_name_length
            )
        )

    if config is None:
        from .settings import config as _config

        config = _config

    (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        fk_attribute_map,
        column_comments,
    ) = prepare_declare(definition, context, adapter, table_name=table_name, config=config)

    pre_ddl = []  # DDL to run BEFORE CREATE TABLE (e.g., CREATE TYPE for enums)
    post_ddl = []  # DDL to run AFTER CREATE TABLE (e.g., COMMENT ON)

    # Get pending enum type DDL for PostgreSQL (must run before CREATE TABLE)
    if schema_name and hasattr(adapter, "get_pending_enum_ddl"):
        pre_ddl.extend(adapter.get_pending_enum_ddl(schema_name))

    # Build PRIMARY KEY clause using adapter
    pk_cols = ", ".join(adapter.quote_identifier(pk) for pk in primary_key)
    pk_clause = f"PRIMARY KEY ({pk_cols})"

    # Handle indexes - inline for MySQL, separate CREATE INDEX for PostgreSQL
    if adapter.supports_inline_indexes:
        # MySQL: include indexes in CREATE TABLE
        create_table_indexes = index_sql
    else:
        # PostgreSQL: convert to CREATE INDEX statements for post_ddl
        create_table_indexes = []
        for idx_def in index_sql:
            # Parse index definition: "unique index (cols)" or "index (cols)"
            idx_match = re.match(r"(unique\s+)?index\s*\(([^)]+)\)", idx_def, re.I)
            if idx_match:
                is_unique = idx_match.group(1) is not None
                # Extract column names (may be quoted or have expressions)
                cols_str = idx_match.group(2)
                # Simple split on comma - columns are already quoted
                columns = [c.strip().strip('`"') for c in cols_str.split(",")]
                # Generate CREATE INDEX DDL
                create_idx_ddl = adapter.create_index_ddl(full_table_name, columns, unique=is_unique)
                post_ddl.append(create_idx_ddl)

    # Assemble CREATE TABLE
    sql = (
        f"CREATE TABLE IF NOT EXISTS {full_table_name} (\n"
        + ",\n".join(attribute_sql + [pk_clause] + foreign_key_sql + create_table_indexes)
        + f"\n) {adapter.table_options_clause(table_comment)}"
    )

    # Add table-level comment DDL if needed (PostgreSQL)
    table_comment_ddl = adapter.table_comment_ddl(full_table_name, table_comment)
    if table_comment_ddl:
        post_ddl.append(table_comment_ddl)

    # Add column-level comments DDL if needed (PostgreSQL)
    # Column comments contain type specifications like :<blob>:user_comment
    for col_name, comment in column_comments.items():
        col_comment_ddl = adapter.column_comment_ddl(full_table_name, col_name, comment)
        if col_comment_ddl:
            post_ddl.append(col_comment_ddl)

    return sql, external_stores, primary_key, fk_attribute_map, pre_ddl, post_ddl


def _renamed_from(comment: str) -> str | None:
    """
    Name of the attribute an attribute renames, if its comment says so.

    A comment that opens with an attribute name in braces renames that
    attribute: ``reading : float32  # {value} the value`` renames ``value`` to
    ``reading``.

    Parameters
    ----------
    comment : str
        The comment written in the definition, without the ``:type:`` marker
        a core type adds to the stored comment.

    Returns
    -------
    str or None
        The name in braces, or None.
    """
    match = re.match(r"{\s*(?P<name>\w+)\s*}", comment)
    return match["name"] if match else None


def _make_attribute_alter(
    new: dict[str, dict], old: dict[str, dict], primary_key: list[str], adapter
) -> tuple[list[str], list[dict]]:
    """
    Find the attributes to drop, add and change between two definitions.

    Parameters
    ----------
    new : dict[str, dict]
        Columns of the new definition, ``name -> parts`` (see ``_column``),
        in declaration order.
    old : dict[str, dict]
        Columns of the current definition, likewise.
    primary_key : list[str]
        Primary key attribute names (cannot be altered).
    adapter : DatabaseAdapter
        Database adapter, consulted on whether a column can be positioned.

    Returns
    -------
    tuple
        Two-element tuple:

        - drop : list[str] - Names of the attributes to drop
        - changes : list[dict] - One per attribute to add or change, in
          declaration order, with keys ``new`` (its parts), ``old`` (the parts
          of the column it changes, or None when it is added),
          ``renamed_from`` (the name in its rename marker, or None) and
          ``after`` (the attribute to place it after, or None to leave it).

    Raises
    ------
    DataJointError
        If an attribute is renamed twice or renamed from non-existent attribute.
    """
    new_names = {name: column["renamed_from"] for name, column in new.items()}
    old_names = list(old)
    old_sql = [column["sql"] for column in old.values()]

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
    old_names = [name for name in old_names if name not in to_drop]

    # add or change attributes in order
    changes = []
    prev = None
    for new_name, old_name in new_names.items():
        column = new[new_name]
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
            if not adapter.supports_column_position:
                # Without an AFTER clause a reorder-only change has nothing to
                # emit, so drop the position before it can force a statement.
                after = None
            source = old[old_name or new_name] if (old_name or new_name) in old_names else None
            # The comment is compared on its own because PostgreSQL leaves it
            # out of the column declaration.
            if column["sql"] not in old_sql or after or (source is not None and source["comment"] != column["comment"]):
                changes.append(dict(new=column, old=source, renamed_from=old_name, after=after))
        prev = new_name, old_name

    return to_drop, changes


def alter(
    definition: str,
    old_definition: str,
    context: dict,
    adapter,
    *,
    full_table_name: str,
) -> tuple[list[str], list[str], list[str]]:
    """
    Generate SQL statements for table definition changes.

    Parameters
    ----------
    definition : str
        New table definition.
    old_definition : str
        Current table definition.
    context : dict
        Namespace for resolving foreign key references.
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.
    full_table_name : str
        Quoted name of the table to alter. Its schema also qualifies the
        pre-DDL of backends that declare column types separately (PostgreSQL
        enums).

    Returns
    -------
    tuple
        Three-element tuple:

        - sql : list[str] - Statements to run in order (see
          ``DatabaseAdapter.alter_table_ddl``); empty when nothing changes
        - new_stores : list[str] - New external stores used
        - pre_ddl : list[str] - DDL to run before the ALTER (e.g. CREATE TYPE)

    Raises
    ------
    NotImplementedError
        If attempting to alter primary key, foreign keys, or indexes.
    DataJointError
        If the backend cannot convert an attribute to its new type.
    """
    schema_name, _ = adapter.split_full_table_name(full_table_name)
    columns = {}
    (
        table_comment,
        primary_key,
        _attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
        _fk_attribute_map,
        _column_comments,
    ) = prepare_declare(definition, context, adapter, columns=columns)

    # prepare_declare registers backend types (PostgreSQL enums) on the adapter
    # as a side effect, so each parse must be drained separately to tell the two
    # apart. Type names are content hashes, making the statements comparable.
    new_type_ddl = adapter.get_pending_enum_ddl(schema_name)

    old_columns = {}
    (
        table_comment_,
        primary_key_,
        _attribute_sql_,
        foreign_key_sql_,
        index_sql_,
        external_stores_,
        _fk_attribute_map_,
        _column_comments_,
    ) = prepare_declare(old_definition, context, adapter, columns=old_columns)

    # Whatever the old definition registered already exists in the database, so
    # only the difference needs creating. Draining both also leaves nothing
    # behind to leak into the next declare() on this adapter, including on the
    # NotImplementedError paths below.
    old_type_ddl = set(adapter.get_pending_enum_ddl(schema_name))
    pre_ddl = [ddl for ddl in new_type_ddl if ddl not in old_type_ddl]

    # analyze differences between declarations
    if primary_key != primary_key_:
        raise NotImplementedError("table.alter cannot alter the primary key (yet).")
    if foreign_key_sql != foreign_key_sql_:
        raise NotImplementedError("table.alter cannot alter foreign keys (yet).")
    if index_sql != index_sql_:
        raise NotImplementedError("table.alter cannot alter indexes (yet)")
    drop, changes = [], []
    if list(columns.values()) != list(old_columns.values()):  # a list, so that order counts
        drop, changes = _make_attribute_alter(columns, old_columns, primary_key, adapter)
    sql = adapter.alter_table_ddl(
        full_table_name,
        drop,
        changes,
        table_comment=table_comment if table_comment != table_comment_ else None,
    )
    return sql, [e for e in external_stores if e not in external_stores_], pre_ddl


def _parse_index_args(args: str) -> list[str]:
    """
    Parse comma-separated index arguments, handling nested parentheses.

    Parameters
    ----------
    args : str
        The arguments string from an index declaration (e.g., ``"a, b, (func(x, y))"``)

    Returns
    -------
    list[str]
        List of individual arguments with surrounding whitespace stripped.

    Notes
    -----
    This parser correctly handles nested parentheses in expressions like
    ``(json_value(`col`, '$.path' returning char(20)))``.
    """
    result = []
    current = []
    depth = 0
    for char in args:
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            result.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        result.append("".join(current).strip())
    return [arg for arg in result if arg]  # Filter empty strings


def compile_index(line: str, index_sql: list[str], adapter) -> None:
    """
    Parse an index declaration and append SQL to index_sql.

    Parameters
    ----------
    line : str
        Index declaration line (e.g., ``"index(attr1, attr2)"`` or
        ``"unique index(attr)"``).
    index_sql : list[str]
        List of index SQL declarations. Updated in place.
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.

    Raises
    ------
    DataJointError
        If the index syntax is invalid.
    """

    def format_attribute(attr):
        match, attr = translate_attribute(attr, adapter)
        if match is None:
            return attr
        if match["path"] is None:
            return adapter.quote_identifier(attr)
        return f"({attr})"

    match = re.match(r"(?P<unique>unique\s+)?index\s*\(\s*(?P<args>.*)\)", line, re.I)
    if match is None:
        raise DataJointError(f'Table definition syntax error in line "{line}"')
    match = match.groupdict()

    attr_list = _parse_index_args(match["args"])
    index_sql.append(
        "{unique}index ({attrs})".format(
            unique="unique " if match["unique"] else "",
            attrs=",".join(format_attribute(a.strip()) for a in attr_list),
        )
    )


def substitute_special_type(match: dict, category: str, foreign_key_sql: list[str], context: dict, adapter) -> None:
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
    adapter : DatabaseAdapter
        Database adapter for backend-specific type mapping.
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
            substitute_special_type(match, category, foreign_key_sql, context, adapter)
    elif category in CORE_TYPE_NAMES:
        # Core DataJoint type - substitute with native SQL type using adapter
        # Pass the full type string (e.g., "varchar(255)") not just category name
        sql_type = adapter.core_type_to_sql(match["type"])
        if sql_type is not None:
            match["type"] = sql_type
        # else: type passes through as-is (json, date, datetime, char, varchar, enum)
    else:
        raise DataJointError(f"Unknown special type: {category}")


def compile_attribute(
    line: str, in_key: bool, foreign_key_sql: list[str], context: dict, adapter, *, column: dict | None = None
) -> tuple[str, str, str | None, str | None]:
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
    adapter : DatabaseAdapter
        Database adapter for backend-specific SQL generation.
    column : dict, optional
        Filled in place with the parts of the column: see ``_column``.
        ``alter`` compares these rather than the compiled declarations, which
        differ by backend (PostgreSQL keeps comments out of them).

    Returns
    -------
    tuple
        Four-element tuple:

        - name : str - Attribute name
        - sql : str - SQL column declaration
        - store : str or None - External store name if applicable
        - comment : str or None - Column comment (for PostgreSQL COMMENT ON)

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

    # `default` is the SQL expression; match["default"] becomes the whole clause
    default = None
    if match["nullable"]:
        if in_key:
            raise DataJointError('Primary key attributes cannot be nullable in line "%s"' % line)
        default = "NULL"
        match["default"] = "DEFAULT NULL"  # nullable attributes default to null
    else:
        if match["default"]:
            default_val = match["default"]
            base_val = default_val.split("(")[0].upper()

            if base_val in CONSTANT_LITERALS:
                # SQL constants like NULL, CURRENT_TIMESTAMP - use as-is
                default = default_val
            elif default_val.startswith('"') and default_val.endswith('"'):
                # Double-quoted string - convert to single quotes for PostgreSQL
                inner = default_val[1:-1].replace("'", "''")  # Escape single quotes
                default = f"'{inner}'"
            elif default_val.startswith("'"):
                # Already single-quoted - use as-is
                default = default_val
            else:
                # Unquoted value - wrap in single quotes
                default = f"'{default_val}'"
            match["default"] = f"NOT NULL DEFAULT {default}"
        else:
            match["default"] = "NOT NULL"

    match["comment"] = match["comment"].replace('"', '\\"')  # escape double quotes in comment

    if match["comment"].startswith(":"):
        raise DataJointError('An attribute comment must not start with a colon in comment "{comment}"'.format(**match))
    renamed_from = _renamed_from(match["comment"])

    category = match_type(match["type"])

    if category in SPECIAL_TYPES:
        # Core types and Codecs are recorded in comment for reconstruction
        match["comment"] = ":{type}:{comment}".format(**match)
        substitute_special_type(match, category, foreign_key_sql, context, adapter)
    elif category == "NATIVE_BLOB":
        # A native blob is a raw binary column, not a DataJoint blob. Say so plainly:
        # the generic portability warning below reads as cosmetic, and users migrating
        # from pre-2.0 expect `longblob` to serialize objects for them.
        logger.warning(
            f"Native type '{match['type']}' is used in attribute '{match['name']}'. "
            "It stores raw bytes only — values are not serialized on write or "
            "deserialized on read. Use '<blob>' to store arrays and other Python "
            "objects, or 'bytes' if raw binary is intended."
        )
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

    # Use adapter to format column definition
    sql = adapter.format_column_definition(
        name=match["name"],
        sql_type=match["type"],
        nullable=match["nullable"],
        default=match["default"] if match["default"] else None,
        comment=match["comment"] if match["comment"] else None,
    )
    comment = match["comment"] if match["comment"] else None
    if column is not None:
        column.update(_column(match["name"], match["type"], match["nullable"], default, comment, sql, renamed_from))
    return match["name"], sql, match.get("store"), comment
