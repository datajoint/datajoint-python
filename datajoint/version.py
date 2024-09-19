try:
    # Use datajoint/_version.py written by setuptools_scm if it exists
    # This module is not tracked in VCS and defines a __version_tuple__ like
    # (0, 14, 3, 'dev224', 'g0812fe17.d20240919')
    from ._version import __version_tuple__ as version_tuple
except ImportError:
    version_tuple = (0, 14, 3)

__version__ = ".".join(str(x) for x in version_tuple[:3])

assert len(__version__) <= 10  # The log table limits version to the 10 characters
