import datajoint as dj
import inspect


class Student(dj.Manual):
    definition = """
    student_id : int unsigned   # university-wide ID number
    ---
    first_name      : varchar(40)
    last_name       : varchar(40)
    sex             : enum('F', 'M', 'U')
    date_of_birth   : date
    home_address    : varchar(120) # mailing street address
    home_city       : varchar(60)  # mailing address
    home_state      : char(2)      # US state acronym: e.g. OH
    home_zip        : char(10)     # zipcode e.g. 93979-4979
    home_phone      : varchar(20)  # e.g. 414.657.6883x0881
    """


class Department(dj.Manual):
    definition = """
    dept : varchar(6)   # abbreviated department name, e.g. BIOL
    ---
    dept_name    : varchar(200)  # full department name
    dept_address : varchar(200)  # mailing address
    dept_phone   : varchar(20)
    """


class StudentMajor(dj.Manual):
    definition = """
    -> Student
    ---
    -> Department
    declare_date :  date  # when student declared her major
    """


class Course(dj.Manual):
    definition = """
    -> Department
    course  : int unsigned   # course number, e.g. 1010
    ---
    course_name :  varchar(200)  # e.g. "Neurobiology of Sensation and Movement."
    credits     :  decimal(3,1)  # number of credits earned by completing the course
    """


class Term(dj.Manual):
    definition = """
    term_year : year
    term      : enum('Spring', 'Summer', 'Fall')
    """


class Section(dj.Manual):
    definition = """
    -> Course
    -> Term
    section : char(1)
    ---
    auditorium   :  varchar(12)
    """


class CurrentTerm(dj.Manual):
    definition = """
    omega=0 : tinyint
    ---
    -> Term
    """


class Enroll(dj.Manual):
    definition = """
    -> Student
    -> Section
    """


class LetterGrade(dj.Lookup):
    definition = """
    grade : char(2)
    ---
    points : decimal(3,2)
    """
    contents = [
        ["A", 4.00],
        ["A-", 3.67],
        ["B+", 3.33],
        ["B", 3.00],
        ["B-", 2.67],
        ["C+", 2.33],
        ["C", 2.00],
        ["C-", 1.67],
        ["D+", 1.33],
        ["D", 1.00],
        ["F", 0.00],
    ]


class Grade(dj.Manual):
    definition = """
    -> Enroll
    ---
    -> LetterGrade
    """


LOCALS_UNI = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_UNI)
