__author__ = 'eywalker'
from .connection import conn

def schema(name, context, connection=None): #TODO consider moving this into relation module
    """
    Returns a schema decorator that can be used to associate a Relation class to a
    specific database with :param name:. Name reference to other tables in the table definition
    will be resolved by looking up the corresponding key entry in the passed in context dictionary.
    It is most common to set context equal to the return value of call to locals() in the module.
    For more details, please refer to the tutorial online.

    :param name: name of the database to associate the decorated class with
    :param context: dictionary used to resolve (any) name references within the table definition string
    :param connection: connection object to the database server. If ommited, will try to establish connection according to
    config values
    :return: a decorator function to be used on Relation derivative classes
    """
    if connection is None:
        connection = conn()

    def _dec(cls):
        cls._schema_name = name
        cls._context = context
        cls._connection = connection
        return cls

    return _dec


