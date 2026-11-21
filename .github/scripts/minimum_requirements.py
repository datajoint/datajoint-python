"""Print pins for the minimum version of each runtime dependency.

Reads [project].dependencies from pyproject.toml and prints one `name==minimum`
line per dependency that applies to the given Python version. CI installs the
package with these as constraints, so the test suite runs against the oldest
versions that pyproject.toml admits. Optional extras are not pinned.

Every applicable dependency must declare a minimum (>=, ==, or ~=). A
dependency without one is an error: GitHub's dependency graph omits it, and
its oldest release is untested.

Usage: python minimum_requirements.py [pyproject.toml] [--python 3.10]
"""

import argparse
import re
import sys

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


def python_version(value: str) -> str:
    if not re.fullmatch(r"3\.\d+", value):
        raise argparse.ArgumentTypeError(f"expected a version like 3.10, got {value!r}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("pyproject", nargs="?", default="pyproject.toml")
    parser.add_argument("--python", type=python_version, default="3.10", help="Python version to evaluate markers for")
    args = parser.parse_args()

    with open(args.pyproject, "rb") as f:
        dependencies = tomllib.load(f)["project"]["dependencies"]

    # A dependency may span several lines, e.g. a minimum plus an upper bound
    # under a marker. Combine the applicable lines per package.
    environment = {"python_version": args.python, "python_full_version": f"{args.python}.0"}
    specifiers: dict[str, SpecifierSet] = {}
    for line in dependencies:
        req = Requirement(line)
        if req.marker is not None and not req.marker.evaluate(environment):
            continue
        name = canonicalize_name(req.name)
        specifiers[name] = specifiers.get(name, SpecifierSet()) & req.specifier

    pins, errors = {}, []
    for name, specifier in sorted(specifiers.items()):
        bounds = [s.version for s in specifier if s.operator in (">=", "==", "~=")]
        if not bounds:
            errors.append(f"{name} declares no minimum version")
            continue
        if any("*" in bound for bound in bounds):
            errors.append(f"{name}: wildcard versions are not supported ({specifier})")
            continue
        minimum = max(bounds, key=Version)
        if minimum not in specifier:
            errors.append(f"{name}: minimum {minimum} is excluded by {specifier}")
            continue
        pins[name] = minimum

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    for name, version in pins.items():
        print(f"{name}=={version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
