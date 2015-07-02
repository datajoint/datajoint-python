#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

#with open(path.join(here, 'VERSION')) as version_file:
#   version = version_file.read().strip()
long_description="An object-relational mapping and relational algebra to facilitate data definition and data manipulation in MySQL databases."


setup(
    name='datajoint',
    version='0.1.0.dev5',
    description="An ORM with closed relational algebra",
    long_description=long_description,
    author='Dimitri Yatsenko',
    author_email='Dimitri.Yatsenko@gmail.com',
    license = "MIT",
    url='https://github.com/datajoint/datajoint-python',
    keywords='database organization',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=['numpy', 'pymysql', 'pyparsing', 'networkx', 'matplotlib', 'sphinx_rtd_theme', 'mock'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database :: Front-Ends',
    ],
)
