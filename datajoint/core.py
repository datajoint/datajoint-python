import exceptions
import re

# package wide variable
VERBOSE = True

class DataJointError(exceptions.Exception):
    pass

def camelCase(s):
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)

