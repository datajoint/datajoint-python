import importlib
import abc
from types import ModuleType
from enum import Enum
from . import DataJointError
from .table import Table
import logging
import re
from .settings import Role, role_to_prefix
from .utils import from_camel_case
from .heading import Heading

logger = logging.getLogger(__name__)


class Base(Table, metaclass=abc.ABCMeta):
    """
    Base is a Table that implements data definition functions.
    It is an abstract class with the abstract property 'definition'.

    Example for a usage of Base::

        import datajoint as dj


        class Subjects(dj.Base):
            definition = '''
            test1.Subjects (manual)                                    # Basic subject info
            subject_id            : int                                # unique subject id
            ---
            real_id               : varchar(40)                        #  real-world name
            species = "mouse"     : enum('mouse', 'monkey', 'human')   # species
            '''

    """

    @abc.abstractproperty
    def definition(self):
        """
        :return: string containing the table declaration using the DataJoint Data Definition Language.
        The DataJoint DDL is described at:  TODO
        """
        pass

    def __init__(self):
        self.class_name = self.__class__.__name__
        module = self.__module__
        mod_obj = importlib.import_module(module)
        self._use_package = False
        try:
            conn = mod_obj.conn
        except AttributeError:
            try:
                # check if database bound at the package level instead
                pkg_obj = importlib.import_module(mod_obj.__package__)
                conn = pkg_obj.conn
                self._use_package = True
            except AttributeError:
                raise DataJointError(
                    "Please define object 'conn' in '{}' or in its containing package.".format(self.__module__))
        self.conn = conn
        try:
            if self._use_package:
                pkg_name = '.'.join(module.split('.')[:-1])
                dbname = self.conn.mod_to_db[pkg_name]
            else:
                dbname = self.conn.mod_to_db[module]
        except KeyError:
            raise DataJointError(
                'Module {} is not bound to a database. See datajoint.connection.bind'.format(self.__module__))
        self.dbname = dbname
        self.declare()
        super().__init__(conn=conn, dbname=dbname, class_name=self.__class__.__name__)

    @property
    def is_declared(self):
        self.conn.load_headings(self.dbname)
        return self.class_name in self.conn.table_names[self.dbname]

    def declare(self):
        """
        Declare the table in database if it doesn't already exist.

        :raises: DataJointError if the table cannot be declared.
        """
        if not self.is_declared:
            self._declare()
            if not self.is_declared:
                raise DataJointError(
                    'Table could not be declared for %s' % self.class_name)

    def _field_to_sql(self, field):
        """
        Converts an attribute definition tuple into SQL code.
        :param field: attribute definition
        :rtype : SQL code
        """
        mysql_constants = ['CURRENT_TIMESTAMP']
        if field.nullable:
            default = 'DEFAULT NULL'
        else:
            default = 'NOT NULL'
            # if some default specified
            if field.default:
                # enclose value in quotes (even numeric), except special SQL values
                # or values already enclosed by the user
                if field.default.upper() in mysql_constants or field.default[:1] in ["'", '"']:
                    default = '%s DEFAULT %s' % (default, field.default)
                else:
                    default = '%s DEFAULT "%s"' % (default, field.default)

        # TODO: escape instead! - same goes for Matlab side implementation
        assert not any((c in r'\"' for c in field.comment)), \
            'Illegal characters in attribute comment "%s"' % field.comment

        return '`{name}` {type} {default} COMMENT "{comment}",\n'.format(
            name=field.name, type=field.type, default=default, comment=field.comment)

    def _declare(self):
        """
        Declares the table in the data base if no table in the database matches this object.
        """
        if not self.definition:
            raise DataJointError('Table declaration is missing!')
        table_info, parents, referenced, fieldDefs, indexDefs = self._parse_declaration()
        defined_name = table_info['module'] + '.' + table_info['className']
        expected_name = self.__module__.split('.')[-1] + '.' + self.class_name
        if not defined_name == expected_name:
            raise DataJointError('Table name {} does not match the declared'
                                 'name {}'.format(expected_name, defined_name))

        # compile the CREATE TABLE statement
        # TODO: support prefix
        table_name = role_to_prefix[
                         table_info['tier']] + from_camel_case(self.class_name)
        sql = 'CREATE TABLE `%s`.`%s` (\n' % (self.dbname, table_name)

        # add inherited primary key fields
        primary_key_fields = set()
        non_key_fields = set()
        for p in parents:
            for key in p.primary_key:
                field = p.heading[key]
                if field.name not in primary_key_fields:
                    primary_key_fields.add(field.name)
                    sql += self._field_to_SQL(field)
                else:
                    logger.debug('Field definition of {} in {} ignored'.format(
                        field.name, p.full_class_name))

        # add newly defined primary key fields
        for field in (f for f in fieldDefs if f.in_key):
            if field.nullable:
                raise DataJointError('Primary key {} cannot be nullable'.format(
                    field.name))
            if field.name in primary_key_fields:
                raise DataJointError('Duplicate declaration of the primary key '
                                     '{key}. Check to make sure that the key '
                                     'is not declared already in referenced '
                                     'tables'.format(key=field.name))
            primary_key_fields.add(field.name)
            sql += self._field_to_sql(field)

        # add secondary foreign key attributes
        for r in referenced:
            keys = (x for x in r.heading.attrs.values() if x.in_key)
            for field in keys:
                if field.name not in primary_key_fields | non_key_fields:
                    non_key_fields.add(field.name)
                    sql += self._field_to_sql(field)

        # add dependent attributes
        for field in (f for f in fieldDefs if not f.in_key):
            non_key_fields.add(field.name)
            sql += self._field_to_sql(field)

        # add primary key declaration
        assert len(primary_key_fields) > 0, 'table must have a primary key'
        keys = ', '.join(primary_key_fields)
        sql += 'PRIMARY KEY (%s),\n' % keys

        # add foreign key declarations
        for ref in parents + referenced:
            keys = ', '.join(ref.primary_key)
            sql += 'FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE CASCADE ON DELETE RESTRICT,\n' % \
                   (keys, ref.full_table_name, keys)

        # add secondary index declarations
        # gather implicit indexes due to foreign keys first
        implicit_indices = []
        for fk_source in parents + referenced:
            implicit_indices.append(fk_source.primary_key)

        # for index in indexDefs:
        # TODO: finish this up...

        # close the declaration
        sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (
            sql[:-2], table_info['comment'])

        # make sure that the table does not alredy exist
        self.conn.load_headings(self.dbname, force=True)
        if not self.is_declared:
            # execute declaration
            logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
            self.conn.query(sql)
            self.conn.load_headings(self.dbname, force=True)

    def _parse_declaration(self):
        """
        Parse declaration and create new SQL table accordingly.
        """
        parents = []
        referenced = []
        index_defs = []
        field_defs = []
        declaration = re.split(r'\s*\n\s*', self.definition.strip())

        # remove comment lines
        declaration = [x for x in declaration if not x.startswith('#')]
        ptrn = """
        ^(?P<module>\w+)\.(?P<className>\w+)\s*     #  module.className
        \(\s*(?P<tier>\w+)\s*\)\s*                  #  (tier)
        \#\s*(?P<comment>.*)$                       #  comment
        """
        p = re.compile(ptrn, re.X)
        table_info = p.match(declaration[0]).groupdict()
        if table_info['tier'] not in Role.__members__:
            raise DataJointError('InvalidTableTier: Invalid tier {tier} for table\
                                 {module}.{cls}'.format(tier=table_info['tier'],
                                                        module=table_info[
                                                            'module'],
                                                        cls=table_info['className']))
        table_info['tier'] = Role[table_info['tier']]  # convert into enum

        in_key = True  # parse primary keys
        field_ptrn = """
        ^[a-z][a-z\d_]*\s*          # name
        (=\s*\S+(\s+\S+)*\s*)?      # optional defaults
        :\s*\w.*$                   # type, comment
        """
        fieldP = re.compile(field_ptrn, re.I + re.X)  # ignore case and verbose

        for line in declaration[1:]:
            if line.startswith('---'):
                in_key = False  # start parsing non-PK fields
            elif line.startswith('->'):
                # foreign key
                module_name, class_name = line[2:].strip().split('.')
                rel = self.get_base(module_name, class_name)
                (parents if in_key else referenced).append(rel)
            elif re.match(r'^(unique\s+)?index[^:]*$', line):
                index_defs.append(self._parse_index_def(line))
            elif fieldP.match(line):
                field_defs.append(self._parse_attr_def(line, in_key))
            else:
                raise DataJointError(
                    'Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs

    def _parse_attr_def(self, line, in_key=False):  # todo add docu for in_key
        """
        Parse attribute definition line in the declaration and returns
        an attribute tuple.

        :param line: attribution line
        :param in_key:
        :returns: attribute tuple
        """
        line = line.strip()
        attr_ptrn = """
        ^(?P<name>[a-z][a-z\d_]*)\s*             # field name
        (=\s*(?P<default>\S+(\s+\S+)*?)\s*)?     # default value
        :\s*(?P<type>\w[^\#]*[^\#\s])\s*         # datatype
        (\#\s*(?P<comment>\S*(\s+\S+)*)\s*)?$    # comment
        """

        attrP = re.compile(attr_ptrn, re.I + re.X)
        m = attrP.match(line)
        assert m, 'Invalid field declaration "%s"' % line
        attr_info = m.groupdict()
        if not attr_info['comment']:
            attr_info['comment'] = ''
        if not attr_info['default']:
            attr_info['default'] = ''
        attr_info['nullable'] = attr_info['default'].lower() == 'null'
        assert (not re.match(r'^bigint', attr_info['type'], re.I) or not attr_info['nullable']), \
            'BIGINT attributes cannot be nullable in "%s"' % line

        attr_info['in_key'] = in_key
        attr_info['autoincrement'] = None
        attr_info['numeric'] = None
        attr_info['string'] = None
        attr_info['is_blob'] = None
        attr_info['computation'] = None
        attr_info['dtype'] = None

        return Heading.AttrTuple(**attr_info)

    def _parse_index_def(self, line):
        """
        Parses index definition.

        :param line: definition line
        :return: groupdict with index info
        """
        line = line.strip()
        index_ptrn = """
        ^(?P<unique>UNIQUE)?\s*INDEX\s*      # [UNIQUE] INDEX
        \((?P<attributes>[^\)]+)\)$          # (attr1, attr2)
        """
        indexP = re.compile(index_ptrn, re.I + re.X)
        m = indexP.match(line)
        assert m, 'Invalid index declaration "%s"' % line
        index_info = m.groupdict()
        attributes = re.split(r'\s*,\s*', index_info['attributes'].strip())
        index_info['attributes'] = attributes
        assert len(attributes) == len(set(attributes)), \
            'Duplicate attributes in index declaration "%s"' % line
        return index_info

    def get_base(self, module_name, class_name):
        """
        Loads the base relation from the module.  If the base relation is not defined in
        the module, then construct it using Base constructor.

        :param module_name: module name
        :param class_name: class name
        :returns: the base relation
        """
        mod_obj = self.get_module(module_name)
        try:
            ret = getattr(mod_obj, class_name)()
        except KeyError:
            ret = self.__class__(conn=self.conn,
                                 dbname=self.conn.schemas[module_name],
                                 class_name=class_name)
        return ret

    @classmethod
    def get_module(cls, module_name):
        """
        Resolve short name reference to a module and return the corresponding module object

        :param module_name: short name for the module, whose reference is to be resolved
        :return: resolved module object. If no module matches the short name, `None` will be returned

        The module_name resolution steps in the following order:

        1. Global reference to a module of the same name defined in the module that contains this Base derivative.
           This is the recommended use case.
        2. Module of the same name defined in the package containing this Base derivative. This will only look for the
           most immediate containing package (e.g. if this class is contained in package.subpackage.module, it will
           check within `package.subpackage` but not inside `package`).
        3. Globally accessible module with the same name.
        """
        mod_obj = importlib.import_module(cls.__module__)
        attr = getattr(mod_obj, module_name, None)
        if isinstance(attr, ModuleType):
            return attr
        if mod_obj.__package__:
            try:
                return importlib.import_module('.' + module_name, mod_obj.__package__)
            except ImportError:
                try:
                    return importlib.import_module(module_name)
                except ImportError:
                    return None
