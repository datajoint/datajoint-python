#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path
import sys
import re

min_py_version = (3, 6)

if sys.version_info <  min_py_version:
    sys.exit('DataJoint is only supported for Python {}.{} or higher'.format(*min_py_version))

here = path.abspath(path.dirname(__file__))

long_description = "A relational data framework for scientific data pipelines with MySQL backend."

# read in version number into __version__
with open(path.join(here, 'datajoint', 'version.py')) as f:
    exec(f.read())

with open(path.join(here, 'requirements.txt')) as f:
    requirements = ['{pkg} @ {target}#egg={pkg}'.format(
        pkg=re.search(r'/([A-Za-z0-9\-]+)\.git', r).group(1),
        target=r) if '+' in r else r for r in f.read().splitlines() if '#' not in r]

setup(
    name='datajoint',
    version=__version__,
    description="A relational data pipeline framework.",
    long_description=long_description,
    author='Dimitri Yatsenko',
    author_email='info@datajoint.io',
    license="GNU LGPL",
    url='https://datajoint.io',
    keywords='database organization',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=requirements,
    python_requires='~={}.{}'.format(*min_py_version),
    setup_requires=['otumat>=0.2.0'],  # maybe remove due to conflicts?
    pubkey_path='./datajoint.pub'
)
