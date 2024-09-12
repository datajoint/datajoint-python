from nose.tools import assert_true, raises, assert_list_equal
from .schema import *
from datajoint.dependencies import unite_master_parts


def test_unite_master_parts():
    assert_list_equal(
        unite_master_parts(
            [
                "`s`.`a`",
                "`s`.`a__q`",
                "`s`.`b`",
                "`s`.`c`",
                "`s`.`c__q`",
                "`s`.`b__q`",
                "`s`.`d`",
                "`s`.`a__r`",
            ]
        ),
        [
            "`s`.`a`",
            "`s`.`a__q`",
            "`s`.`a__r`",
            "`s`.`b`",
            "`s`.`b__q`",
            "`s`.`c`",
            "`s`.`c__q`",
            "`s`.`d`",
        ],
    )
    assert_list_equal(
        unite_master_parts(
            [
                "`lab`.`#equipment`",
                "`cells`.`cell_analysis_method`",
                "`cells`.`cell_analysis_method_task_type`",
                "`cells`.`cell_analysis_method_users`",
                "`cells`.`favorite_selection`",
                "`cells`.`cell_analysis_method__cell_selection_params`",
                "`lab`.`#equipment__config`",
                "`cells`.`cell_analysis_method__field_detect_params`",
            ]
        ),
        [
            "`lab`.`#equipment`",
            "`lab`.`#equipment__config`",
            "`cells`.`cell_analysis_method`",
            "`cells`.`cell_analysis_method__cell_selection_params`",
            "`cells`.`cell_analysis_method__field_detect_params`",
            "`cells`.`cell_analysis_method_task_type`",
            "`cells`.`cell_analysis_method_users`",
            "`cells`.`favorite_selection`",
        ],
    )


def test_nullable_dependency():
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    a = ThingA()
    b = ThingB()
    c = ThingC()

    # clear previous contents if any.
    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))

    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    # missing foreign key attributes = ok
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))

    # unique foreign key attributes = ok
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))

    assert_true(len(c) == len(c.fetch()) == 5)


@raises(dj.errors.DuplicateError)
def test_unique_dependency():
    """test nullable unique foreign key"""

    # Thing C has a nullable dependency on B whose primary key is composite
    a = ThingA()
    b = ThingB()
    c = ThingC()

    # clear previous contents if any.
    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))

    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    c.insert1(dict(a=0, b1=1, b2=1))
    # duplicate foreign key attributes = not ok
    c.insert1(dict(a=1, b1=1, b2=1))
