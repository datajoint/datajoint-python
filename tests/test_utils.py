"""
Collection of test cases to test core module.
"""

from datajoint import DataJointError
from datajoint.utils import (
    from_camel_case,
    to_camel_case,
    is_camel_case,
    contains_non_ascii_char,
)
import pytest


def test_is_camel_case():
    assert is_camel_case("AllGroups")
    assert not is_camel_case("allGroups")
    assert not is_camel_case("repNames")
    assert not is_camel_case("10_all")
    assert not is_camel_case("hello world")
    assert not is_camel_case("#baisc_names")
    assert not is_camel_case("alphaBeta")
    non_ascii_class_name = "TestΣ"
    assert contains_non_ascii_char(non_ascii_class_name)
    assert not is_camel_case(non_ascii_class_name)


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
