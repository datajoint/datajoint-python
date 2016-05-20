from operator import itemgetter
import pymysql
import logging
import re
from . import conn, DataJointError
from datajoint.utils import to_camel_case
from .heading import Heading
from .user_relations import Part, Computed, Imported, Manual, Lookup
import inspect

logger = logging.getLogger(__name__)


class Schema:
    """
    A schema object can be used  as a decorator that associates a Relation class to a database as
    well as a namespace for looking up foreign key references in table declaration.
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
        self.database = database
        self.connection = connection
        self.context = context
        if not self.exists:
            # create schema
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
            for row in connection.query('SHOW TABLES in {database}'.format(database=self.database)):
                class_name, class_obj = self._create_missing_relation_class(row[0])
                # Add the missing class to the context
                # decorate class with @schema if it is not None and not a dj.Part
                context[class_name] = (
                    class_obj if class_obj is None or issubclass(class_obj, Part)
                    else self(class_obj))
        connection.register(self)

    def _create_missing_relation_class(self, table_name):
        """
        Creates the appropriate python user relation classes from tables in a database. The tier of the class
        is inferred from the table name.

        Schema stores the class objects in a class dictionary and returns those
        when prompted for the same table from the same database again. This way, the id
        of both returned class objects is the same and comparison with python's "is" works
        correctly.
        """
        class_name = to_camel_case(table_name)

        def _make_tuples_stub(other, key):
            raise NotImplementedError("This is an automatically created class. _make_tuples is not implemented.")

        if re.fullmatch(Part._regexp, table_name):
            groups = re.fullmatch(Part._regexp, table_name).groupdict()
            master_table_name = groups['master']
            master_name, master_class = self._create_missing_relation_class(master_table_name)
            class_name = to_camel_case(groups['part'])
            class_obj = type(class_name, (Part,), dict(definition=...))
            setattr(master_class, class_name, class_obj)
            class_name, class_obj = master_name, master_class
        elif re.fullmatch(Computed._regexp, table_name):
            class_obj = type(class_name, (Computed,), dict(definition=..., _make_tuples=_make_tuples_stub))
        elif re.fullmatch(Imported._regexp, table_name):
            class_obj = type(class_name, (Imported,), dict(definition=..., _make_tuples=_make_tuples_stub))
        elif re.fullmatch(Lookup._regexp, table_name):
            class_obj = type(class_name, (Lookup,), dict(definition=...))
        elif re.fullmatch(Manual._regexp, table_name):
            class_obj = type(class_name, (Manual,), dict(definition=...))
        else:
            class_obj = None
        return class_name, class_obj

    def drop(self):
        """
        Drop the associated database if it exists
        """
        if self.exists:
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Database `{database}` was dropped successfully.".format(database=self.database))
            except pymysql.OperationalError:
                raise DataJointError("An attempt to drop database named `{database}` "
                                     "has failed. Check permissions.".format(database=self.database))
        else:
            logger.info("Database named `{database}` does not exist. Doing nothing.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated database exists on the server
        """
        cur = self.connection.query("SHOW DATABASES LIKE '{database}'".format(database=self.database))
        return cur.rowcount > 0

    def __call__(self, cls):
        """
        Binds the passed in class object to a database. This is intended to be used as a decorator.
        :param cls: class to be decorated
        """

        def process_relation_class(relation_class, context):
            """
            assign schema properties to the relation class and declare the table
            """
            relation_class.database = self.database
            relation_class._connection = self.connection
            relation_class._heading = Heading()
            relation_class._context = context
            # instantiate the class and declare the table in database if not already present
            relation_class().declare()

        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')

        process_relation_class(cls, context=self.context)

        # Process part relations
        def is_part(x):
            return inspect.isclass(x) and issubclass(x, Part)

        parts = list()
        for part in dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if is_part(part):
                    parts.append(part)
                    part._master = cls
                    process_relation_class(part, context=dict(self.context, **{cls.__name__: cls}))

        # invoke Relation._prepare() on class and its part relations.
        cls()._prepare()
        for part in parts:
            part()._prepare()

        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]
