from decorator import decorator
from . import DataJointError, TransactionError


def _not_in_transaction(f, *args, **kwargs):
    if not hasattr(args[0], '_conn'):
        raise DataJointError(u"{0:s} does not have a member called _conn".format(args[0].__class__.__name__, ))
    if not hasattr(args[0]._conn, 'in_transaction'):
        raise DataJointError(
            u"{0:s}._conn does not have a property in_transaction".format(args[0].__class__.__name__, ))
    if args[0]._conn.in_transaction:
        raise TransactionError(
            u"{0:s} is currently in transaction. Operation not allowed to avoid implicit commits.".format(
                args[0].__class__.__name__))
    return f(*args, **kwargs)


def not_in_transaction(f):
    """
    This decorator raises an error if the function is called during a transaction.
    """
    return decorator(_not_in_transaction, f)
