"""
Collection of test cases to test core module.
"""

from datajoint import DataJointError
from datajoint.utils import (
    from_camel_case,
    to_camel_case,
    is_camel_case,
)
import pytest


def test_is_camel_case():
    assert is_camel_case("AllGroups")
    assert not is_camel_case("All_Groups")
    assert not is_camel_case("All_Groups_")
    assert not is_camel_case("_AllGroups")
    assert not is_camel_case("allGroups")
    assert not is_camel_case("repNames")
    assert not is_camel_case("10_all")
    assert not is_camel_case("hello world")
    assert not is_camel_case("#baisc_names")
    assert not is_camel_case("alphaBeta")
    assert not is_camel_case("TestΣ")


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
