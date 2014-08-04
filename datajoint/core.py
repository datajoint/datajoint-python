import re

# package-wide settings that control execution
settings = dict(
verbose = True
)

class DataJointError(Exception):
    pass

def camelCase(s):
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)