"""
Settings for DataJoint.
"""
__author__ = 'eywalker'
import logging

# Settings dictionary. Don't manipulate this directly

class Config(object):
    """
    Configuration object
    """




logger = logging.getLogger()
logger.setLevel(logging.DEBUG) #set package wide logger level TODO:make this respond to environmental variable
