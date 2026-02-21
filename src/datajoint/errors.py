"""
Exception classes for the DataJoint library.

This module defines the exception hierarchy for DataJoint errors.
"""

from __future__ import annotations


# --- Top Level ---
class DataJointError(Exception):
    """Base class for errors specific to DataJoint internal operation."""

    def suggest(self, *args: object) -> "DataJointError":
        """
        Regenerate the exception with additional arguments.

        Parameters
        ----------
        *args : object
            Additional arguments to append to the exception.

        Returns
        -------
        DataJointError
            A new exception of the same type with the additional arguments.
        """
        return self.__class__(*(self.args + args))


# --- Second Level ---
class LostConnectionError(DataJointError):
    """Loss of server connection."""


class QueryError(DataJointError):
    """Errors arising from queries to the database."""


# --- Third Level: QueryErrors ---
class QuerySyntaxError(QueryError):
    """Errors arising from incorrect query syntax."""


class AccessError(QueryError):
    """User access error: insufficient privileges."""


class MissingTableError(DataJointError):
    """Query on a table that has not been declared."""


class DuplicateError(QueryError):
    """Integrity error caused by a duplicate entry into a unique key."""


class IntegrityError(QueryError):
    """Integrity error triggered by foreign key constraints."""


class UnknownAttributeError(QueryError):
    """User requests an attribute name not found in query heading."""


class MissingAttributeError(QueryError):
    """Required attribute value not provided in INSERT."""


class MissingExternalFile(DataJointError):
    """External file managed by DataJoint is no longer accessible."""


class BucketInaccessible(DataJointError):
    """S3 bucket is inaccessible."""


class ThreadSafetyError(DataJointError):
    """Global DataJoint state is disabled in thread-safe mode."""
