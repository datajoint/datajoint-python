# Indexes

Table indexes are data structures that allow fast lookups by an indexed attribute or
combination of attributes.

In DataJoint, indexes are created by one of the three mechanisms:

1. Primary key
2. Foreign key
3. Explicitly defined indexes

The first two mechanisms are obligatory. Every table has a primary key, which serves as
an unique index. Therefore, restrictions by a primary key are very fast. Foreign keys
create additional indexes unless a suitable index already exists.

## Indexes for single primary key tables

Let’s say a mouse in the lab has a lab-specific ID but it also has a separate id issued
by the animal facility.

```python
@schema
class Mouse(dj.Manual):
    definition = """
    mouse_id : int  # lab-specific ID
    ---
    tag_id : int  # animal facility ID
    """
```

In this case, searching for a mouse by `mouse_id` is much faster than by `tag_id`
because `mouse_id` is a primary key, and is therefore indexed.

To make searches faster on fields other than the primary key or a foreign key, you can
add a secondary index explicitly.

Regular indexes are declared as `index(attr1, ..., attrN)` on a separate line anywhere in
the table declaration (below the primary key divide).

Indexes can be declared with unique constraint as `unique index (attr1, ..., attrN)`.

Let’s redeclare the table with a unique index on `tag_id`.

```python
@schema
class Mouse(dj.Manual):
    definition = """
    mouse_id : int  # lab-specific ID
    ---
    tag_id : int  # animal facility ID
    unique index (tag_id)
    """
```
Now, searches with `mouse_id` and `tag_id` are similarly fast.

## Indexes for tables with multiple primary keys

Let’s now imagine that rats in a lab are identified by the combination of `lab_name` and
`rat_id` in a table `Rat`.

```python
@schema
class Rat(dj.Manual):
    definition = """
    lab_name : char(16)
    rat_id : int unsigned # lab-specific ID
    ---
    date_of_birth = null : date
    """
```
Note that despite the fact that `rat_id` is in the index, searches by `rat_id` alone are not
helped by the index because it is not first in the index. This is similar to searching for
a word in a dictionary that orders words alphabetically. Searching by the first letters
of a word is easy but searching by the last few letters of a word requires scanning the
whole dictionary.

In this table, the primary key is a unique index on the combination `(lab_name, rat_id)`.
Therefore searches on these attributes or on `lab_name` alone are fast. But this index
cannot help searches on `rat_id` alone. Similarly, searing by `date_of_birth` requires a
full-table scan and is inefficient.

To speed up searches by the `rat_id` and `date_of_birth`, we can explicit indexes to
`Rat`:

```python
@schema
class Rat2(dj.Manual):
    definition = """
    lab_name : char(16)
    rat_id : int unsigned # lab-specific ID
    ---
    date_of_birth = null : date

    index(rat_id)
    index(date_of_birth)
    """
```
