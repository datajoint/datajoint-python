"""
Exception classes for the DataJoint library
"""


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
