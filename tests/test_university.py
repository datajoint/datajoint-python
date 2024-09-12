import pytest
import hashlib
from pathlib import Path
from datajoint import DataJointError
import datajoint as dj
from .schema_university import *
from . import schema_university


def _hash4(table):
    """Hash of table contents"""
    data = table.fetch(order_by="KEY", as_dict=True)
    blob = dj.blob.pack(data, compress=False)
    return hashlib.md5(blob).digest().hex()[:4]


@pytest.fixture
def schema_uni_inactive():
    schema = dj.Schema(context=schema_university.LOCALS_UNI)
    schema(Student)
    schema(Department)
    schema(StudentMajor)
    schema(Course)
    schema(Term)
    schema(Section)
    schema(CurrentTerm)
    schema(Enroll)
    schema(LetterGrade)
    schema(Grade)
    yield schema
    schema.drop()


@pytest.fixture
def schema_uni(db_creds_test, schema_uni_inactive, connection_test, prefix):
    # Deferred activation
    schema_uni_inactive.activate(
        prefix + "_university", connection=dj.conn(**db_creds_test)
    )
    # ---------------  Fill University -------------------
    test_data_dir = Path(__file__).parent / "data"
    for table in (
        Student,
        Department,
        StudentMajor,
        Course,
        Term,
        CurrentTerm,
        Section,
        Enroll,
        Grade,
    ):
        path = test_data_dir / Path(table.__name__ + ".csv")
        assert path.is_file(), f"File {path} is not a file"
        assert path.exists(), f"File {path} does not exist"
        table().insert(path)
    return schema_uni_inactive


def test_activate_unauthorized(schema_uni_inactive, db_creds_test, connection_test):
    with pytest.raises(DataJointError):
        schema_uni_inactive.activate(
            "unauthorized", connection=dj.conn(**db_creds_test)
        )


def test_fill(schema_uni):
    """check that the randomized tables are consistently defined"""
    # check randomized tables
    assert len(Student()) == 300 and _hash4(Student) == "1e1a"
    assert len(StudentMajor()) == 226 and _hash4(StudentMajor) == "3129"
    assert len(Section()) == 756 and _hash4(Section) == "dc7e"
    assert len(Enroll()) == 3364 and _hash4(Enroll) == "177d"
    assert len(Grade()) == 3027 and _hash4(Grade) == "4a9d"


def test_restrict(schema_uni):
    """
    test diverse restrictions from the university database.
    This test relies on a specific instantiation of the database.
    """
    utahns1 = Student & {"home_state": "UT"}
    utahns2 = Student & 'home_state="UT"'
    assert len(utahns1) == len(utahns2.fetch("KEY")) == 7

    # male nonutahns
    sex1, state1 = ((Student & 'sex="M"') - {"home_state": "UT"}).fetch(
        "sex", "home_state", order_by="student_id"
    )
    sex2, state2 = ((Student & 'sex="M"') - {"home_state": "UT"}).fetch(
        "sex", "home_state", order_by="student_id"
    )
    assert len(set(state1)) == len(set(state2)) == 44
    assert set(sex1).pop() == set(sex2).pop() == "M"

    # students from OK, NM, TX
    s1 = (Student & [{"home_state": s} for s in ("OK", "NM", "TX")]).fetch(
        "KEY", order_by="student_id"
    )
    s2 = (Student & 'home_state in ("OK", "NM", "TX")').fetch(
        "KEY", order_by="student_id"
    )
    assert len(s1) == 11
    assert s1 == s2

    millennials = Student & 'date_of_birth between "1981-01-01" and "1996-12-31"'
    assert len(millennials) == 170
    millennials_no_math = millennials - (Enroll & 'dept="MATH"')
    assert len(millennials_no_math) == 53

    inactive_students = Student - (Enroll & CurrentTerm)
    assert len(inactive_students) == 204

    # Females who are active or major in non-math
    special = Student & [Enroll, StudentMajor - {"dept": "MATH"}] & {"sex": "F"}
    assert len(special) == 158


def test_advanced_join(schema_uni):
    """test advanced joins"""
    # Students with ungraded courses in current term
    ungraded = Enroll * CurrentTerm - Grade
    assert len(ungraded) == 34

    # add major
    major = StudentMajor.proj(..., major="dept")
    assert len(ungraded.join(major, left=True)) == len(ungraded) == 34
    assert len(ungraded.join(major)) == len(ungraded & major) == 31


def test_union(schema_uni):
    # effective left join Enroll with Major
    q1 = (Enroll & "student_id=101") + (Enroll & "student_id=102")
    q2 = Enroll & "student_id in (101, 102)"
    assert len(q1) == len(q2) == 41


def test_aggr(schema_uni):
    avg_grade_per_course = Course.aggr(
        Grade * LetterGrade, avg_grade="round(avg(points), 2)"
    )
    assert len(avg_grade_per_course) == 45

    # GPA
    student_gpa = Student.aggr(
        Course * Grade * LetterGrade, gpa="round(sum(points*credits)/sum(credits), 2)"
    )
    gpa = student_gpa.fetch("gpa")
    assert len(gpa) == 261
    assert 2 < gpa.mean() < 3

    # Sections in biology department with zero students in them
    section = (Section & {"dept": "BIOL"}).aggr(
        Enroll, n="count(student_id)", keep_all_rows=True
    ) & "n=0"
    assert len(set(section.fetch("dept"))) == 1
    assert len(section) == 17
    assert bool(section)

    # Test correct use of ellipses in a similar query
    section = (Section & {"dept": "BIOL"}).aggr(
        Grade, ..., n="count(student_id)", keep_all_rows=True
    ) & "n>1"
    assert not any(
        name in section.heading.names for name in Grade.heading.secondary_attributes
    )
    assert len(set(section.fetch("dept"))) == 1
    assert len(section) == 168
    assert bool(section)
