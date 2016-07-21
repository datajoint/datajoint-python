from . import conn
from .utils import user_choice


def set_password(new_password, connection=conn()):
    if 'yes' == user_choice('Do you wish to change the password?'):
        connection.query("SET PASSWORD = PASSWORD('%s')" % new_password)
        print('Done!')
    else:
        print('Password left unchanged')