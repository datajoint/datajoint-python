from datajoint import errors
from pytest import raises
from datajoint.dependencies import unite_master_parts


def test_unite_master_parts():
    assert unite_master_parts(
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
    ) == [
        "`s`.`a`",
        "`s`.`a__q`",
        "`s`.`a__r`",
        "`s`.`b`",
        "`s`.`b__q`",
        "`s`.`c`",
        "`s`.`c__q`",
        "`s`.`d`",
    ]
    assert unite_master_parts(
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
    ) == [
        "`lab`.`#equipment`",
        "`lab`.`#equipment__config`",
        "`cells`.`cell_analysis_method`",
        "`cells`.`cell_analysis_method__cell_selection_params`",
        "`cells`.`cell_analysis_method__field_detect_params`",
        "`cells`.`cell_analysis_method_task_type`",
        "`cells`.`cell_analysis_method_users`",
        "`cells`.`favorite_selection`",
    ]


def test_nullable_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    # missing foreign key attributes = ok
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))

    # unique foreign key attributes = ok
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))

    assert len(c) == len(c.fetch()) == 5


def test_unique_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    c.insert1(dict(a=0, b1=1, b2=1))
    # duplicate foreign key attributes = not ok
    with raises(errors.DuplicateError):
        c.insert1(dict(a=1, b1=1, b2=1))
