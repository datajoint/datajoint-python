server_error_codes = {
    'unknown column': 1054,
    'duplicate entry': 1062,
    'parse error': 1064,
    'command denied': 1142,
    'table does not exist': 1146,
    'syntax error': 1149
}


class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal operation.
    """
    pass


class DuplicateError(DataJointError):
    """
    Error caused by a violation of a unique constraint when inserting data
    """
    pass
