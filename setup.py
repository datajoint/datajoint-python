#!/usr/bin/env python
from setuptools import setup, find_packages
from os import path
import sys

min_py_version = (3, 8)

if sys.version_info < min_py_version:
    sys.exit(
        "DataJoint is only supported for Python {}.{} or higher".format(*min_py_version)
    )

here = path.abspath(path.dirname(__file__))

long_description = (
    "A relational data framework for scientific data pipelines with MySQL backend."
)

# read in version number into __version__
with open(path.join(here, "datajoint", "version.py")) as f:
    exec(f.read())

with open(path.join(here, "requirements.txt")) as f:
    requirements = [line.split("#", 1)[0].rstrip() for line in f.readlines()]

setup(
    name="datajoint",
    version=__version__,
    description="A relational data pipeline framework.",
    long_description=long_description,
    author="DataJoint Contributors",
    author_email="support@datajoint.com",
    license="GNU LGPL",
    url="https://datajoint.com",
    keywords=[
        "database",
        "data pipelines",
        "scientific computing",
        "automated research workflows",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    install_requires=requirements,
    python_requires="~={}.{}".format(*min_py_version),
    setup_requires=["otumat"],  # maybe remove due to conflicts?
    pubkey_path="./datajoint.pub",
)
