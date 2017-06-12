import warnings
import pymysql
import logging
import inspect
import re
from . import conn, DataJointError, config
from .heading import Heading
from .utils import user_choice, to_camel_case
from .user_relations import Part, Computed, Imported, Manual, Lookup
from .base_relation import lookup_class_name, Log

logger = logging.getLogger(__name__)

def ordered_dir(klass):
    """
    List (most) attributes of the class including inherited ones, similar to `dir` build-in function,
    but respects order of attribute declaration as much as possible.
    :param klass: class to list members for
    :return: a list of attributes declared in klass and its superclasses
    """
    m = []
    mro = klass.mro()
    for c in mro:
        if hasattr(c, '_ordered_class_members'):
            elements = c._ordered_class_members
        else:
            elements = c.__dict__.keys()
        m = [e for e in elements if e not in m] + m
    return m

class Schema:
    """
    A schema object is a decorator for UserRelation classes that binds them to their database.
    It also specifies the namespace `context` in which other UserRelation classes are defined.
    """

    def __init__(self, database, context, connection=None):
        """
        Associates the specified database with this schema object. If the target database does not exist
        already, will attempt on creating the database.

        :param database: name of the database to associate the decorated class with
        :param context: dictionary for looking up foreign keys references, usually set to locals()
        :param connection: Connection object. Defaults to datajoint.conn()
        """
        if connection is None:
            connection = conn()
        self._log = None
        self.database = database
        self.connection = connection
        self.context = context
        if not self.exists:
            # create database
            logger.info("Database `{database}` could not be found. "
                        "Attempting to create the database.".format(database=database))
            try:
                connection.query("CREATE DATABASE `{database}`".format(database=database))
                logger.info('Created database `{database}`.'.format(database=database))
            except pymysql.OperationalError:
                raise DataJointError("Database named `{database}` was not defined, and"
                                     " an attempt to create has failed. Check"
                                     " permissions.".format(database=database))
            else:
                self.log('created')
        self.log('connect')
        connection.register(self)

    @property
    def log(self):
        if self._log is None:
            self._log = Log(self.connection, self.database)
        return self._log

    def __repr__(self):
        return 'Schema database: `{database}` in module: {context}\n'.format(
            database=self.database,
            context=self.context['__name__'] if '__name__' in self.context else "__")

    @property
    def size_on_disk(self):
        """
        :return: size of the database in bytes
        """
        return float(self.connection.query(
            """SELECT Sum(data_length + index_length) FROM information_schema.tables WHERE table_schema='{}';""".format(
                self.database)).fetchone()[0])

    def spawn_missing_classes(self):
        """
        Creates the appropriate python user relation classes from tables in the database and places them
        in the context.
        """
        tables = [
            row[0] for row in self.connection.query('SHOW TABLES in `%s`' % self.database)
            if lookup_class_name('`{db}`.`{tab}`'.format(db=self.database, tab=row[0]), self.context, 0) is None]
        master_classes = (Lookup, Manual, Imported, Computed)
        part_tables = []
        for table_name in tables:
            class_name = to_camel_case(table_name)
            if class_name not in self.context:
                try:
                    cls = next(cls for cls in master_classes if re.fullmatch(cls.tier_regexp, table_name))
                except StopIteration:
                    if re.fullmatch(Part.tier_regexp, table_name):
                        part_tables.append(table_name)
                else:
                    # declare and decorate master relation classes
                    self.context[class_name] = self(type(class_name, (cls,), dict()))

        # attach parts to masters
        for table_name in part_tables:
            groups = re.fullmatch(Part.tier_regexp, table_name).groupdict()
            class_name = to_camel_case(groups['part'])
            try:
                master_class = self.context[to_camel_case(groups['master'])]
            except KeyError:
                raise DataJointError('The table %s does not follow DataJoint naming conventions' % table_name)
            part_class = type(class_name, (Part,), dict(definition=...))
            part_class._master = master_class
            self.process_relation_class(part_class, context=self.context, assert_declared=True)
            setattr(master_class, class_name, part_class)

    def drop(self, force=False):
        """
        Drop the associated database if it exists
        """
        if not self.exists:
            logger.info("Database named `{database}` does not exist. Doing nothing.".format(database=self.database))
        elif (not config['safemode'] or
              force or
              user_choice("Proceed to delete entire schema `%s`?" % self.database, default='no') == 'yes'):
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Database `{database}` was dropped successfully.".format(database=self.database))
            except pymysql.OperationalError:
                raise DataJointError("An attempt to drop database named `{database}` "
                                     "has failed. Check permissions.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated database exists on the server
        """
        cur = self.connection.query("SHOW DATABASES LIKE '{database}'".format(database=self.database))
        return cur.rowcount > 0

    def process_relation_class(self, relation_class, context, assert_declared=False):
        """
        assign schema properties to the relation class and declare the table
        """
        relation_class.database = self.database
        relation_class._connection = self.connection
        relation_class._heading = Heading()
        relation_class._context = context
        # instantiate the class, declare the table if not already, and fill it with initial values.
        instance = relation_class()
        if not instance.is_declared:
            assert not assert_declared, 'incorrect table name generation'
            instance.declare()
        if instance.is_declared and hasattr(instance, 'contents'):
            # process the contents attribute
            contents = list(instance.contents)
            if len(contents) > len(instance):
                if instance.heading.has_autoincrement:
                    warnings.warn(
                        'Contents has changed but cannot be inserted because {table} has autoincrement.'.format(
                            table=instance.__class__.__name__))
                else:
                    instance.insert(contents, skip_duplicates=True)

    def __call__(self, cls):
        """
        Binds the passed in class object to a database. This is intended to be used as a decorator.
        :param cls: class to be decorated
        """

        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')
        ext = {
            cls.__name__: cls,
            'self': cls
        }
        self.process_relation_class(cls, context=dict(self.context, **ext))

        # Process part relations
        for part in ordered_dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if inspect.isclass(part) and issubclass(part, Part):
                    part._master = cls
                    # allow addressing master by name or keyword 'master'
                    ext = {
                        cls.__name__: cls,
                        part.__name__: part,
                        'master': cls,
                        'self': part
                    }
                    self.process_relation_class(part, context=dict(self.context, **ext))
        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]

