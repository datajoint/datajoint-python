from nose.tools import assert_true, assert_equal, assert_list_equal
from .schema_university import *


def test_restrict():
    """
    test diverse restrictions from the university database.
    This test relies on a specific instantiation of the database.
    """
    utahns1 = Student & {'home_state': 'UT'}
    utahns2 = Student & 'home_state="UT"'
    assert_true(len(utahns1) == len(utahns2.fetch('KEY')) == 7)

    # male nonutahns
    sex1, state1 = ((Student & 'sex="M"') - {'home_state': 'UT'}).fetch(
        'sex', 'home_state', order_by='student_id')
    sex2, state2 = ((Student & 'sex="M"') - {'home_state': 'UT'}).fetch(
        'sex', 'home_state', order_by='student_id')
    assert_true(len(set(state1)) == len(set(state2)) == 44)
    assert_true(set(sex1).pop() == set(sex2).pop() == "M")

    # students from OK, NM, TX
    s1 = (Student & [{'home_state': s} for s in ('OK', 'NM', 'TX')]).fetch("KEY", order_by="student_id")
    s2 = (Student & 'home_state in ("OK", "NM", "TX")').fetch('KEY', order_by="student_id")
    assert_true(len(s1) == 11)
    assert_list_equal(s1, s2)

    millenials = Student & 'date_of_birth between "1981-01-01" and "1996-12-31"'
    assert_true(len(millenials) == 172)
    millenials_no_math = millenials - (Enroll & 'dept="MATH"')
    assert_true(len(millenials_no_math) == 43)

    inactive_students = Student - (Enroll & CurrentTerm)
    assert_true(len(inactive_students) == 195)

    # Females who are active or major in non-math
    special = Student & [Enroll, StudentMajor - {'dept': "MATH"}] & {'sex': "F"}
    assert_true(len(special) == 156)


def test_advanced_join():
    """test advanced joins"""
    # Students with ungraded courses in current term
    ungraded = Student & (Enroll * CurrentTerm - Grade)
    assert_true(len(ungraded) == 29)


def test_union():
    # effective left join Enroll with Major
    q = Enroll() * StudentMajor + Enroll()
    q.make_sql()


def test_aggr():
    avg_grade_per_course = Course.aggr(Grade*LetterGrade, avg_grade='avg(points)')