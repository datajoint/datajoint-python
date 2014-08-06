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