"""
Collection of test cases to test core module.
"""

from datajoint import DataJointError
from datajoint.utils import from_camel_case, to_camel_case
import pytest


def test_from_camel_case():
    assert from_camel_case("AllGroups") == "all_groups"
    with pytest.raises(DataJointError):
        from_camel_case("repNames")
    with pytest.raises(DataJointError):
        from_camel_case("10_all")
    with pytest.raises(DataJointError):
        from_camel_case("hello world")
    with pytest.raises(DataJointError):
        from_camel_case("#baisc_names")


def test_to_camel_case():
    assert to_camel_case("all_groups") == "AllGroups"
    assert to_camel_case("hello") == "Hello"
    assert to_camel_case("this_is_a_sample_case") == "ThisIsASampleCase"
    assert to_camel_case("This_is_Mixed") == "ThisIsMixed"
