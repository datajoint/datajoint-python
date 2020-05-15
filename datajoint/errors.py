"""
Exception classes for the DataJoint library
"""

import os


# --- Top Level ---
class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal operation.
    """
    def suggest(self, *args):
        """
        regenerate the exception with additional arguments
        :param args: addition arguments
        :return: a new exception of the same type with the additional arguments
        """
        return self.__class__(*(self.args + args))


# --- Second Level ---
class LostConnectionError(DataJointError):
    """
    Loss of server connection
    """


class QueryError(DataJointError):
    """
    Errors arising from queries to the database
    """


# --- Third Level: QueryErrors ---
class QuerySyntaxError(QueryError):
    """
    Errors arising from incorrect query syntax
    """


class AccessError(QueryError):
    """
    User access error: insufficient privileges.
    """


class MissingTableError(DataJointError):
    """
    Query on a table that has not been declared
    """


class DuplicateError(QueryError):
    """
    An integrity error caused by a duplicate entry into a unique key
    """


class IntegrityError(QueryError):
    """
    An integrity error triggered by foreign key constraints
    """


class UnknownAttributeError(QueryError):
    """
    User requests an attribute name not found in query heading
    """


class MissingAttributeError(QueryError):
    """
    An error arising when a required attribute value is not provided in INSERT
    """


class MissingExternalFile(DataJointError):
    """
    Error raised when an external file managed by DataJoint is no longer accessible
    """


class BucketInaccessible(DataJointError):
    """
    Error raised when a S3 bucket is inaccessible
    """


# environment variables to control availability of experimental features

ADAPTED_TYPE_SWITCH = "DJ_SUPPORT_ADAPTED_TYPES"
FILEPATH_FEATURE_SWITCH = "DJ_SUPPORT_FILEPATH_MANAGEMENT"


def _switch_adapted_types(on):
    """
    Enable (on=True) or disable (on=False) support for AttributeAdapter
    """
    if on:
        os.environ[ADAPTED_TYPE_SWITCH] = "TRUE"
    else:
        del os.environ[ADAPTED_TYPE_SWITCH]


def _support_adapted_types():
    """
    check if support for AttributeAdapter is enabled
    """
    return os.getenv(ADAPTED_TYPE_SWITCH, "FALSE").upper() == "TRUE"


def _switch_filepath_types(on):
    """
    Enable (on=True) or disable (on=False) support for AttributeAdapter
    """
    if on:
        os.environ[FILEPATH_FEATURE_SWITCH] = "TRUE"
    else:
        del os.environ[FILEPATH_FEATURE_SWITCH]


def _support_filepath_types():
    """
    check if support for AttributeAdapter is enabled
    """
    return os.getenv(FILEPATH_FEATURE_SWITCH, "FALSE").upper() == "TRUE"
