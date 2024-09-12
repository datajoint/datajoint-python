"""
This module hosts functions to convert DataJoint table definitions into mysql table definitions, and to
declare the corresponding mysql tables.
"""

import re
import pyparsing as pp
import logging
from .errors import DataJointError, _support_filepath_types, FILEPATH_FEATURE_SWITCH
from .attribute_adapter import get_adapter
from .condition import translate_attribute

UUID_DATA_TYPE = "binary(16)"
MAX_TABLE_NAME_LENGTH = 64
CONSTANT_LITERALS = {
    "CURRENT_TIMESTAMP",
    "NULL",
}  # SQL literals to be used without quotes (case insensitive)
EXTERNAL_TABLE_ROOT = "~external"

TYPE_PATTERN = {
    k: re.compile(v, re.I)
    for k, v in dict(
        INTEGER=r"((tiny|small|medium|big|)int|integer)(\s*\(.+\))?(\s+unsigned)?(\s+auto_increment)?|serial$",
        DECIMAL=r"(decimal|numeric)(\s*\(.+\))?(\s+unsigned)?$",
        FLOAT=r"(double|float|real)(\s*\(.+\))?(\s+unsigned)?$",
        STRING=r"(var)?char\s*\(.+\)$",
        JSON=r"json$",
        ENUM=r"enum\s*\(.+\)$",
        BOOL=r"bool(ean)?$",  # aliased to tinyint(1)
        TEMPORAL=r"(date|datetime|time|timestamp|year)(\s*\(.+\))?$",
        INTERNAL_BLOB=r"(tiny|small|medium|long|)blob$",
        EXTERNAL_BLOB=r"blob@(?P<store>[a-z][\-\w]*)$",
        INTERNAL_ATTACH=r"attach$",
        EXTERNAL_ATTACH=r"attach@(?P<store>[a-z][\-\w]*)$",
        FILEPATH=r"filepath@(?P<store>[a-z][\-\w]*)$",
        UUID=r"uuid$",
        ADAPTED=r"<.+>$",
    ).items()
}

# custom types are stored in attribute comment
SPECIAL_TYPES = {
    "UUID",
    "INTERNAL_ATTACH",
    "EXTERNAL_ATTACH",
    "EXTERNAL_BLOB",
    "FILEPATH",
    "ADAPTED",
}
NATIVE_TYPES = set(TYPE_PATTERN) - SPECIAL_TYPES
EXTERNAL_TYPES = {
    "EXTERNAL_ATTACH",
    "EXTERNAL_BLOB",
    "FILEPATH",
}  # data referenced by a UUID in external tables
SERIALIZED_TYPES = {
    "EXTERNAL_ATTACH",
    "INTERNAL_ATTACH",
    "EXTERNAL_BLOB",
    "INTERNAL_BLOB",
}  # requires packing data

assert set().union(SPECIAL_TYPES, EXTERNAL_TYPES, SERIALIZED_TYPES) <= set(TYPE_PATTERN)


def match_type(attribute_type):
    try:
        return next(
            category
            for category, pattern in TYPE_PATTERN.items()
            if pattern.match(attribute_type)
        )
    except StopIteration:
        raise DataJointError(
            "Unsupported attribute type {type}".format(type=attribute_type)
        )


logger = logging.getLogger(__name__.split(".")[0])


def build_foreign_key_parser_old():
    # old-style foreign key parser. Superseded by expression-based syntax. See issue #436
    # This will be deprecated in a future release.
    left = pp.Literal("(").suppress()
    right = pp.Literal(")").suppress()
    attribute_name = pp.Word(pp.srange("[a-z]"), pp.srange("[a-z0-9_]"))
    new_attrs = pp.Optional(
        left + pp.delimitedList(attribute_name) + right
    ).setResultsName("new_attrs")
    arrow = pp.Literal("->").suppress()
    lbracket = pp.Literal("[").suppress()
    rbracket = pp.Literal("]").suppress()
    option = pp.Word(pp.srange("[a-zA-Z]"))
    options = pp.Optional(
        lbracket + pp.delimitedList(option) + rbracket
    ).setResultsName("options")
    ref_table = pp.Word(pp.alphas, pp.alphanums + "._").setResultsName("ref_table")
    ref_attrs = pp.Optional(
        left + pp.delimitedList(attribute_name) + right
    ).setResultsName("ref_attrs")
    return new_attrs + arrow + options + ref_table + ref_attrs


def build_foreign_key_parser():
    arrow = pp.Literal("->").suppress()
    lbracket = pp.Literal("[").suppress()
    rbracket = pp.Literal("]").suppress()
    option = pp.Word(pp.srange("[a-zA-Z]"))
    options = pp.Optional(
        lbracket + pp.delimitedList(option) + rbracket
    ).setResultsName("options")
    ref_table = pp.restOfLine.setResultsName("ref_table")
    return arrow + options + ref_table


def build_attribute_parser():
    quoted = pp.QuotedString('"') ^ pp.QuotedString("'")
    colon = pp.Literal(":").suppress()
    attribute_name = pp.Word(pp.srange("[a-z]"), pp.srange("[a-z0-9_]")).setResultsName(
        "name"
    )
    data_type = (
        pp.Combine(pp.Word(pp.alphas) + pp.SkipTo("#", ignore=quoted))
        ^ pp.QuotedString("<", endQuoteChar=">", unquoteResults=False)
    ).setResultsName("type")
    default = pp.Literal("=").suppress() + pp.SkipTo(
        colon, ignore=quoted
    ).setResultsName("default")
    comment = pp.Literal("#").suppress() + pp.restOfLine.setResultsName("comment")
    return attribute_name + pp.Optional(default) + colon + data_type + comment


foreign_key_parser_old = build_foreign_key_parser_old()
foreign_key_parser = build_foreign_key_parser()
attribute_parser = build_attribute_parser()


def is_foreign_key(line):
    """

    :param line: a line from the table definition
    :return: true if the line appears to be a foreign key definition
    """
    arrow_position = line.find("->")
    return arrow_position >= 0 and not any(c in line[:arrow_position] for c in "\"#'")


def compile_foreign_key(
    line, context, attributes, primary_key, attr_sql, foreign_key_sql, index_sql
):
    """
    :param line: a line from a table definition
    :param context: namespace containing referenced objects
    :param attributes: list of attribute names already in the declaration -- to be updated by this function
    :param primary_key: None if the current foreign key is made from the dependent section. Otherwise it is the list
        of primary key attributes thus far -- to be updated by the function
    :param attr_sql: list of sql statements defining attributes -- to be updated by this function.
    :param foreign_key_sql: list of sql statements specifying foreign key constraints -- to be updated by this function.
    :param index_sql: list of INDEX declaration statements, duplicate or redundant indexes are ok.
    """
    # Parse and validate
    from .table import Table
    from .expression import QueryExpression

    try:
        result = foreign_key_parser.parseString(line)
    except pp.ParseException as err:
        raise DataJointError('Parsing error in line "%s". %s.' % (line, err))

    try:
        ref = eval(result.ref_table, context)
    except Exception:
        raise DataJointError(
            "Foreign key reference %s could not be resolved" % result.ref_table
        )

    options = [opt.upper() for opt in result.options]
    for opt in options:  # check for invalid options
        if opt not in {"NULLABLE", "UNIQUE"}:
            raise DataJointError('Invalid foreign key option "{opt}"'.format(opt=opt))
    is_nullable = "NULLABLE" in options
    is_unique = "UNIQUE" in options
    if is_nullable and primary_key is not None:
        raise DataJointError(
            'Primary dependencies cannot be nullable in line "{line}"'.format(line=line)
        )

    if isinstance(ref, type) and issubclass(ref, Table):
        ref = ref()

    # check that dependency is of a supported type
    if (
        not isinstance(ref, QueryExpression)
        or len(ref.restriction)
        or len(ref.support) != 1
        or not isinstance(ref.support[0], str)
    ):
        raise DataJointError(
            'Dependency "%s" is not supported (yet). Use a base table or its projection.'
            % result.ref_table
        )

    # declare new foreign key attributes
    for attr in ref.primary_key:
        if attr not in attributes:
            attributes.append(attr)
            if primary_key is not None:
                primary_key.append(attr)
            attr_sql.append(
                ref.heading[attr].sql.replace("NOT NULL ", "", int(is_nullable))
            )

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
        index_sql.append(
            "UNIQUE INDEX ({attrs})".format(
                attrs=",".join("`%s`" % attr for attr in ref.primary_key)
            )
        )


def prepare_declare(definition, context):
    # split definition into lines
    definition = re.split(r"\s*\n\s*", definition.strip())
    # check for optional table comment
    table_comment = (
        definition.pop(0)[1:].strip() if definition[0].startswith("#") else ""
    )
    if table_comment.startswith(":"):
        raise DataJointError('Table comment must not start with a colon ":"')
    in_key = True  # parse primary keys
    primary_key = []
    attributes = []
    attribute_sql = []
    foreign_key_sql = []
    index_sql = []
    external_stores = []

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
    )


def declare(full_table_name, definition, context):
    """
    Parse declaration and generate the SQL CREATE TABLE code

    :param full_table_name: full name of the table
    :param definition: DataJoint table definition
    :param context: dictionary of objects that might be referred to in the table
    :return: SQL CREATE TABLE statement, list of external stores used
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
    ) = prepare_declare(definition, context)

    if not primary_key:
        raise DataJointError("Table must have a primary key")

    return (
        "CREATE TABLE IF NOT EXISTS %s (\n" % full_table_name
        + ",\n".join(
            attribute_sql
            + ["PRIMARY KEY (`" + "`,`".join(primary_key) + "`)"]
            + foreign_key_sql
            + index_sql
        )
        + '\n) ENGINE=InnoDB, COMMENT "%s"' % table_comment
    ), external_stores


def _make_attribute_alter(new, old, primary_key):
    """
    :param new: new attribute declarations
    :param old: old attribute declarations
    :param primary_key: primary key attributes
    :return: list of SQL ALTER commands
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
                raise DataJointError(
                    "Alter attempted to rename attribute {%s} twice." % v
                )
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
                            else "MODIFY" if not old_name else "CHANGE `%s`" % old_name
                        ),
                        new_def=new_def,
                        after="" if after is None else "AFTER `%s`" % after,
                    )
                )
        prev = new_name, old_name

    return sql


def alter(definition, old_definition, context):
    """
    :param definition: new table definition
    :param old_definition: current table definition
    :param context: the context in which to evaluate foreign key definitions
    :return: string SQL ALTER command, list of new stores used for external storage
    """
    (
        table_comment,
        primary_key,
        attribute_sql,
        foreign_key_sql,
        index_sql,
        external_stores,
    ) = prepare_declare(definition, context)
    (
        table_comment_,
        primary_key_,
        attribute_sql_,
        foreign_key_sql_,
        index_sql_,
        external_stores_,
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


def compile_index(line, index_sql):
    def format_attribute(attr):
        match, attr = translate_attribute(attr)
        if match is None:
            return attr
        if match["path"] is None:
            return f"`{attr}`"
        return f"({attr})"

    match = re.match(
        r"(?P<unique>unique\s+)?index\s*\(\s*(?P<args>.*)\)", line, re.I
    ).groupdict()
    attr_list = re.findall(r"(?:[^,(]|\([^)]*\))+", match["args"])
    index_sql.append(
        "{unique}index ({attrs})".format(
            unique="unique " if match["unique"] else "",
            attrs=",".join(format_attribute(a.strip()) for a in attr_list),
        )
    )


def substitute_special_type(match, category, foreign_key_sql, context):
    """
    :param match: dict containing with keys "type" and "comment" -- will be modified in place
    :param category: attribute type category from TYPE_PATTERN
    :param foreign_key_sql: list of foreign key declarations to add to
    :param context: context for looking up user-defined attribute_type adapters
    """
    if category == "UUID":
        match["type"] = UUID_DATA_TYPE
    elif category == "INTERNAL_ATTACH":
        match["type"] = "LONGBLOB"
    elif category in EXTERNAL_TYPES:
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
        match["store"] = match["type"].split("@", 1)[1]
        match["type"] = UUID_DATA_TYPE
        foreign_key_sql.append(
            "FOREIGN KEY (`{name}`) REFERENCES `{{database}}`.`{external_table_root}_{store}` (`hash`) "
            "ON UPDATE RESTRICT ON DELETE RESTRICT".format(
                external_table_root=EXTERNAL_TABLE_ROOT, **match
            )
        )
    elif category == "ADAPTED":
        adapter = get_adapter(context, match["type"])
        match["type"] = adapter.attribute_type
        category = match_type(match["type"])
        if category in SPECIAL_TYPES:
            # recursive redefinition from user-defined datatypes.
            substitute_special_type(match, category, foreign_key_sql, context)
    else:
        assert False, "Unknown special type"


def compile_attribute(line, in_key, foreign_key_sql, context):
    """
    Convert attribute definition from DataJoint format to SQL

    :param line: attribution line
    :param in_key: set to True if attribute is in primary key set
    :param foreign_key_sql: the list of foreign key declarations to add to
    :param context: context in which to look up user-defined attribute type adapterss
    :returns: (name, sql, is_external) -- attribute name and sql code for its declaration
    """
    try:
        match = attribute_parser.parseString(line + "#", parseAll=True)
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
            raise DataJointError(
                'Primary key attributes cannot be nullable in line "%s"' % line
            )
        match["default"] = "DEFAULT NULL"  # nullable attributes default to null
    else:
        if match["default"]:
            quote = (
                match["default"].split("(")[0].upper() not in CONSTANT_LITERALS
                and match["default"][0] not in "\"'"
            )
            match["default"] = (
                "NOT NULL DEFAULT " + ('"%s"' if quote else "%s") % match["default"]
            )
        else:
            match["default"] = "NOT NULL"

    match["comment"] = match["comment"].replace(
        '"', '\\"'
    )  # escape double quotes in comment

    if match["comment"].startswith(":"):
        raise DataJointError(
            'An attribute comment must not start with a colon in comment "{comment}"'.format(
                **match
            )
        )

    category = match_type(match["type"])
    if category in SPECIAL_TYPES:
        match["comment"] = ":{type}:{comment}".format(
            **match
        )  # insert custom type into comment
        substitute_special_type(match, category, foreign_key_sql, context)

    if category in SERIALIZED_TYPES and match["default"] not in {
        "DEFAULT NULL",
        "NOT NULL",
    }:
        raise DataJointError(
            "The default value for a blob or attachment attributes can only be NULL in:\n{line}".format(
                line=line
            )
        )

    sql = (
        "`{name}` {type} {default}"
        + (' COMMENT "{comment}"' if match["comment"] else "")
    ).format(**match)
    return match["name"], sql, match.get("store")
