from nose.tools import assert_true, assert_list_equal, assert_false, raises
import hashlib
from datajoint import DataJointError
from .schema_university import *
from . import PREFIX, CONN_INFO


def _hash4(table):
    """hash of table contents"""
    data = table.fetch(order_by="KEY", as_dict=True)
    blob = dj.blob.pack(data, compress=False)
    return hashlib.md5(blob).digest().hex()[:4]


@raises(DataJointError)
def test_activate_unauthorized():
    schema.activate('unauthorized', connection=dj.conn(**CONN_INFO))


def test_activate():
    schema.activate(PREFIX + '_university',  connection=dj.conn(**CONN_INFO))  # deferred activation
    # ---------------  Fill University -------------------
    for table in Student, Department, StudentMajor, Course, Term, CurrentTerm, Section, Enroll, Grade:
        import csv
        with open('./data/' + table.__name__ + '.csv') as f:
            reader = csv.DictReader(f)
            table().insert(reader)


def test_fill():
    """ check that the randomized tables are consistently defined """
    # check randomized tables
    assert_true(len(Student()) == 300 and _hash4(Student) == '1e1a')
    assert_true(len(StudentMajor()) == 226 and _hash4(StudentMajor) == '3129')
    assert_true(len(Section()) == 756 and _hash4(Section) == 'dc7e')
    assert_true(len(Enroll()) == 3364 and _hash4(Enroll) == '177d')
    assert_true(len(Grade()) == 3027 and _hash4(Grade) == '4a9d')


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
    s1 = (Student & [{'home_state': s} for s in ('OK', 'NM', 'TX')]).fetch(
        "KEY", order_by="student_id")
    s2 = (Student & 'home_state in ("OK", "NM", "TX")').fetch('KEY', order_by="student_id")
    assert_true(len(s1) == 11)
    assert_list_equal(s1, s2)

    millenials = Student & 'date_of_birth between "1981-01-01" and "1996-12-31"'
    assert_true(len(millenials) == 170)
    millenials_no_math = millenials - (Enroll & 'dept="MATH"')
    assert_true(len(millenials_no_math) == 53)

    inactive_students = Student - (Enroll & CurrentTerm)
    assert_true(len(inactive_students) == 204)

    # Females who are active or major in non-math
    special = Student & [Enroll, StudentMajor - {'dept': "MATH"}] & {'sex': "F"}
    assert_true(len(special) == 158)


def test_advanced_join():
    """test advanced joins"""
    # Students with ungraded courses in current term
    ungraded = Enroll * CurrentTerm - Grade
    assert_true(len(ungraded) == 34)

    # add major
    major = StudentMajor.proj(..., major='dept')
    assert_true(len(ungraded.join(major, left=True)) == len(ungraded) == 34)
    assert_true(len(ungraded.join(major)) == len(ungraded & major) == 31)


def test_union():
    # effective left join Enroll with Major
    q1 = (Enroll & 'student_id=101') + (Enroll & 'student_id=102')
    q2 = (Enroll & 'student_id in (101, 102)')
    assert_true(len(q1) == len(q2) == 41)


def test_aggr():
    avg_grade_per_course = Course.aggr(Grade*LetterGrade, avg_grade='round(avg(points), 2)')
    assert_true(len(avg_grade_per_course) == 45)

    # GPA
    student_gpa = Student.aggr(
        Course * Grade * LetterGrade,
        gpa='round(sum(points*credits)/sum(credits), 2)')
    gpa = student_gpa.fetch('gpa')
    assert_true(len(gpa) == 261)
    assert_true(2 < gpa.mean() < 3)

    # Sections in biology department with zero students in them
    section = (Section & {"dept": "BIOL"}).aggr(
        Enroll, n='count(student_id)', keep_all_rows=True) & 'n=0'
    assert_true(len(set(section.fetch('dept'))) == 1)
    assert_true(len(section) == 17)
    assert_true(bool(section))

    # Test correct use of ellipses in a similar query
    section = (Section & {"dept": "BIOL"}).aggr(
        Grade, ..., n='count(student_id)', keep_all_rows=True) & 'n>1'
    assert_false(
        any(name in section.heading.names for name in Grade.heading.secondary_attributes))
    assert_true(len(set(section.fetch('dept'))) == 1)
    assert_true(len(section) == 168)
    assert_true(bool(section))
