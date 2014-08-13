import re

# package-wide settings that control execution
class settings:
    verbose = True


def log(msg):
    if settings.verbose:
        print(msg)


class DataJointError(Exception):
    pass

def camelCase(s):
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)

def fromCamelCase(s):
    assert not re.search(r'\s', s), 'white space is not allowed'
    assert not re.match(r'\d.*', s), 'string cannot begin with a digit'
    assert re.match(r'^[a-zA-Z0-9]*$', s), 'fromCameCase string can only contain alphanumerica characters'
    def conv(matchobj):
        if matchobj.groups()[0]:
            return '_' + matchobj.group(0).lower()
        else:
            return matchobj.group(0).lower()
    return re.sub(r'(\B[A-Z])|(\b[A-Z])', conv, s)