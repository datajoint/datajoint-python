# Altering Populated Pipelines

Tables can be altered after they have been declared and populated. This is useful when
you want to add new secondary attributes or change the data type of existing attributes.
Users can use the `definition` property to update a table's attributes and then use
`alter` to apply the changes in the database. Currently, `alter` does not support
changes to primary key attributes.

Let's say we have a table `Student` with the following attributes:

```python
@schema
class Student(dj.Manual):
    definition = """
    student_id: int
    ---
    first_name: varchar(40)
    last_name: varchar(40)
    home_address: varchar(100)
    """
```

We can modify the table to include a new attribute `email`:

```python
Student.definition = """
student_id: int
---
first_name: varchar(40)
last_name: varchar(40)
home_address: varchar(100)
email: varchar(100)
"""
Student.alter()
```

The `alter` method will update the table in the database to include the new attribute
`email` added by the user in the table's `definition` property.

Similarly, you can modify the data type or length of an existing attribute. For example,
to alter the `home_address` attribute to have a length of 200 characters:

```python
Student.definition = """
student_id: int
---
first_name: varchar(40)
last_name: varchar(40)
home_address: varchar(200)
email: varchar(100)
"""
Student.alter()
```
