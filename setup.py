#!/usr/bin/env python

from distutils.core import setup

setup(
    name='datajoint',
    version='0.1',
    author='Dimitri Yatsenko',  # todo: change that once deon
    author_email='Dimitri.Yatsenko@gmail.com',
    description='An object-relational mapping and relational algebra to facilitate data definition and data manipulation in MySQL databases.',
    url='https://github.com/datajoint/datajoint-python',
    packages=['datajoint'],
    requires=['numpy', 'pymysql', 'networkx', 'matplotlib', 'sphinx_rtd_theme', 'mock', 'json', 'decorator'],
    license = "MIT",
)
