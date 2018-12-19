
.. code-block:: python

  # All home cities of students
  dj.U('home_city', 'home_state') & Student

  # Total number of students from each city
  dj.U('home_city', 'home_state').aggr(Student, n="count(*)")

  # Total number of students from each state
  U('home_state').aggr(Student, n="count(*)")

  # Total number of students in the database
  U().aggr(Student, n="count(*)")
