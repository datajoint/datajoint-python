import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.Schema(connection=dj.conn(**CONN_INFO))


@schema
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


@schema
class Department(dj.Manual):
    definition = """
    dept : varchar(6)   # abbreviated department name, e.g. BIOL
    ---
    dept_name    : varchar(200)  # full department name
    dept_address : varchar(200)  # mailing address
    dept_phone   : varchar(20)
    """


@schema
class StudentMajor(dj.Manual):
    definition = """
    -> Student
    ---
    -> Department
    declare_date :  date  # when student declared her major
    """


@schema
class Course(dj.Manual):
    definition = """
    -> Department
    course  : int unsigned   # course number, e.g. 1010
    ---
    course_name :  varchar(200)  # e.g. "Neurobiology of Sensation and Movement."
    credits     :  decimal(3,1)  # number of credits earned by completing the course
    """


@schema
class Term(dj.Manual):
    definition = """
    term_year : year
    term      : enum('Spring', 'Summer', 'Fall')
    """


@schema
class Section(dj.Manual):
    definition = """
    -> Course
    -> Term
    section : char(1)
    ---
    auditorium   :  varchar(12)
    """


@schema
class CurrentTerm(dj.Manual):
    definition = """
    omega=0 : tinyint
    ---
    -> Term
    """


@schema
class Enroll(dj.Manual):
    definition = """
    -> Student
    -> Section
    """


@schema
class LetterGrade(dj.Manual):
    definition = """
    grade : char(2)
    ---
    points : decimal(3,2)
    """


@schema
class Grade(dj.Manual):
    definition = """
    -> Enroll
    ---
    -> LetterGrade
    """


# ------------- Deferred activation -----------
schema.activate(PREFIX + '_university')
schema.drop(force=True)
schema.activate(PREFIX + '_university')


# ---------------  Fill University -------------------

import faker
import random
import datetime

random.seed(42)
faker.Faker.seed(42)

fake = faker.Faker()

LetterGrade().insert([
    ['A',  4.00], ['A-', 3.67],
    ['B+', 3.33], ['B',  3.00], ['B-', 2.67],
    ['C+', 2.33], ['C',  2.00], ['C-', 1.67],
    ['D+', 1.33], ['D',  1.00], ['F',  0.00]])


def yield_students():
    fake_name = {'F': fake.name_female, 'M': fake.name_male}
    while True:  # ignore invalid values
        try:
            sex = random.choice(('F', 'M'))
            first_name, last_name = fake_name[sex]().split(' ')[:2]
            street_address, city = fake.address().split('\n')
            city, state = city.split(', ')
            state, zipcode = state.split(' ')
        except ValueError:
            continue
        else:
            yield dict(
                first_name=first_name,
                last_name=last_name,
                sex=sex,
                home_address=street_address,
                home_city=city,
                home_state=state,
                home_zip=zipcode,
                date_of_birth=str(
                    fake.date_time_between(start_date="-35y", end_date="-15y").date()),
                home_phone=fake.phone_number()[:20])


Student().insert(
    dict(k, student_id=i) for i, k in zip(range(100, 400), yield_students()))

Department().insert(
    dict(dept=dept,
         dept_name=name,
         dept_address=fake.address(),
         dept_phone=fake.phone_number()[:20])
    for dept, name in [
        ["CS", "Computer Science"],
        ["BIOL", "Life Sciences"],
        ["PHYS", "Physics"],
        ["MATH", "Mathematics"]])


def choices(seq, k):
    """necessary because Python3.5 does not provide random.choices"""
    yield from (random.choice(seq) for _ in range(k))


StudentMajor().insert({**s, **d,
                       'declare_date': fake.date_between(start_date=datetime.date(1999, 1, 1))}
                      for s, d in zip(Student.fetch('KEY', order_by="KEY"),
                                      choices(Department.fetch('KEY', order_by="KEY"), k=len(Student())))
                      if random.random() < 0.75)


# from https://www.utah.edu/
Course().insert([
    ['BIOL', 1006, 'World of Dinosaurs', 3],
    ['BIOL', 1010, 'Biology in the 21st Century', 3],
    ['BIOL', 1030, 'Human Biology', 3],
    ['BIOL', 1210, 'Principles of Biology', 4],
    ['BIOL', 2010, 'Evolution & Diversity of Life', 3],
    ['BIOL', 2020, 'Principles of Cell Biology', 3],
    ['BIOL', 2021, 'Principles of Cell Science', 4],
    ['BIOL', 2030, 'Principles of Genetics', 3],
    ['BIOL', 2210, 'Human Genetics', 3],
    ['BIOL', 2325, 'Human Anatomy', 4],
    ['BIOL', 2330, 'Plants & Society', 3],
    ['BIOL', 2355, 'Field Botany', 2],
    ['BIOL', 2420, 'Human Physiology', 4],
    ['PHYS', 2040, 'Classical Theoretical Physics II', 4],
    ['PHYS', 2060, 'Quantum Mechanics', 3],
    ['PHYS', 2100, 'General Relativity and Cosmology', 3],
    ['PHYS', 2140, 'Statistical Mechanics', 4],
    ['PHYS', 2210, 'Physics for Scientists and Engineers I', 4],
    ['PHYS', 2220, 'Physics for Scientists and Engineers II', 4],
    ['PHYS', 3210, 'Physics for Scientists I (Honors)', 4],
    ['PHYS', 3220, 'Physics for Scientists II (Honors)', 4],
    ['MATH', 1250, 'Calculus for AP Students I', 4],
    ['MATH', 1260, 'Calculus for AP Students II', 4],
    ['MATH', 1210, 'Calculus I', 4],
    ['MATH', 1220, 'Calculus II', 4],
    ['MATH', 2210, 'Calculus III', 3],
    ['MATH', 2270, 'Linear Algebra', 4],
    ['MATH', 2280, 'Introduction to Differential Equations', 4],
    ['MATH', 3210, 'Foundations of Analysis I', 4],
    ['MATH', 3220, 'Foundations of Analysis II', 4],
    ['CS', 1030, 'Foundations of Computer Science', 3],
    ['CS', 1410, 'Introduction to Object-Oriented Programming', 4],
    ['CS', 2420, 'Introduction to Algorithms & Data Structures', 4],
    ['CS', 2100, 'Discrete Structures', 3],
    ['CS', 3500, 'Software Practice', 4],
    ['CS', 3505, 'Software Practice II', 3],
    ['CS', 3810, 'Computer Organization', 4],
    ['CS', 4400, 'Computer Systems', 4],
    ['CS', 4150, 'Algorithms', 3],
    ['CS', 3100, 'Models of Computation', 3],
    ['CS', 3200, 'Introduction to Scientific Computing', 3],
    ['CS', 4000, 'Senior Capstone Project - Design Phase', 3],
    ['CS', 4500, 'Senior Capstone Project', 3],
    ['CS', 4940, 'Undergraduate Research', 3],
    ['CS', 4970, 'Computer Science Bachelor''s Thesis', 3]])


Term().insert(dict(term_year=year, term=term)
              for year in range(2015, 2021)
              for term in ['Spring', 'Summer', 'Fall'])

CurrentTerm().insert1({
    'omega': 1,
    **Term().fetch(order_by=('term_year DESC', 'term DESC'), as_dict=True, limit=1)[0]})


def make_section(prob):
    for c in (Course * Term).proj():
        for sec in 'abcd':
            if random.random() < prob:
                break
            yield {**c, 'section': sec,
                   'auditorium': random.choice('ABCDEF') + str(random.randint(1, 100))}


# random enrollment
Section().insert(make_section(0.5))

terms = Term().fetch('KEY', order_by="KEY")
quit_prob = 0.1
for student in Student.fetch('KEY', order_by="KEY"):
    start_term = random.randrange(len(terms))
    for term in terms[start_term:]:
        if random.random() < quit_prob:
            break
        sections = ((Section & term) - (Course & (Enroll & student))).fetch('KEY', order_by="KEY")
        if sections:
            Enroll().insert({**student, **section} for section in
                            random.sample(sections, random.randrange(min(5, len(sections)))))

# assign random grades
grades = LetterGrade().fetch('grade', order_by="KEY")

grade_keys = Enroll().fetch('KEY', order_by="KEY")
random.shuffle(grade_keys)
grade_keys = grade_keys[:len(grade_keys)*9//10]

Grade().insert({**key, 'grade': grade}
               for key, grade in zip(grade_keys, random.choices(grades, k=len(grade_keys))))
