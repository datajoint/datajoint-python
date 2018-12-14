
.. code-block:: python

  # Number of students in each course section
  Section.aggr(Enroll, n="count(*)")
  # Average grade in each course
  Course.aggr(Grade * LetterGrade, avg_grade="avg(points)")
