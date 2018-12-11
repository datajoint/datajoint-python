from pymysql import err

server_error_codes = {
    'unknown column': 1054,
    'duplicate entry': 1062,
    'parse error': 1064,
    'command denied': 1142,
    'table does not exist': 1146,
    'syntax error': 1149,
}

operation_error_codes = {
    'connection timedout': 2006,
    'lost connection': 2013,
}


def is_connection_error(e):
    """
    Checks if error e pertains to a connection issue
    """
    return (isinstance(e, err.InterfaceError) and e.args[0] == "(0, '')") or\
        (isinstance(e, err.OperationalError) and e.args[0] in operation_error_codes.values())


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
