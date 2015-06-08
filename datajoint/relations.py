import abc
import logging
from collections import namedtuple
import pymysql
from .connection import conn
from .abstract_relation import Relation
from . import DataJointError


logger = logging.getLogger(__name__)


SharedInfo = namedtuple(
    'SharedInfo',
    ('database', 'context', 'connection', 'heading', 'parents', 'children', 'references', 'referenced'))


def schema(database, context, connection=None):
    """
    Returns a schema decorator that can be used to associate a Relation class to a
    specific database with :param name:. Name reference to other tables in the table definition
    will be resolved by looking up the corresponding key entry in the passed in context dictionary.
    It is most common to set context equal to the return value of call to locals() in the module.
    For more details, please refer to the tutorial online.

    :param database: name of the database to associate the decorated class with
    :param context: dictionary used to resolve (any) name references within the table definition string
    :param connection: connection object to the database server. If ommited, will try to establish connection according to
    config values
    :return: a decorator function to be used on Relation derivative classes
    """
    if connection is None:
        connection = conn()

    # if the database does not exist, create it
    cur = connection.query("SHOW DATABASES LIKE '{database}'".format(database=database))
    if cur.rowcount == 0:
        logger.info("Database `{database}` could not be found. "
                    "Attempting to create the database.".format(database=database))
        try:
            connection.query("CREATE DATABASE `{database}`".format(database=database))
            logger.info('Created database `{database}`.'.format(database=database))
        except pymysql.OperationalError:
            raise DataJointError("Database named `{database}` was not defined, and"
                                 "an attempt to create has failed. Check"
                                 " permissions.".format(database=database))

    def decorator(cls):
        cls._shared_info = SharedInfo(
            database=database,
            context=context,
            connection=connection,
            heading=None,
            parents=[],
            children=[],
            references=[],
            referenced=[]
        )
        return cls

    return decorator


class ClassBoundRelation(Relation):
    """
    Abstract class for dedicated table classes.
    Subclasses of ClassBoundRelation are dedicated interfaces to a single table.
    The main purpose of ClassBoundRelation is to encapsulated sharedInfo containing the table heading
    and dependency information shared by all instances of
    """

    _shared_info = None

    def __init__(self):
        if self._shared_info is None:
            raise DataJointError('The class must define _shared_info')

    @property
    def database(self):
        return self._shared_info.database

    @property
    def connection(self):
        return self._shared_info.connection

    @property
    def context(self):
        return self._shared_info.context

    @property
    def heading(self):
        if self._shared_info.heading is None:
            self._shared_info.heading = super().heading
        return self._shared_info.heading
