from . import conn


def set_password(new_password, connection=conn()):
    connection.query("SET PASSWORD = PASSWORD('%s')" % new_password)
    print('done.')
