"""utilities for MongoDB"""
__version__ = '1.0.18'
__author__ = '@nickmilon'

from os import path
from sys import version_info
_PATH_ROOT = path.abspath(path.dirname(__file__))
_PATH_TO_JS = path.abspath(path.dirname(_PATH_ROOT)) + "/MRjs/"
_PATH_TO_DATA = path.abspath(path.dirname(_PATH_ROOT)) + "/data/"

_PY_VERSION = version_info
_IS_PY2 = (_PY_VERSION[0] == 2)
_IS_PY3 = (_PY_VERSION[0] == 3)
