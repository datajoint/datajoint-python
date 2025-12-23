# Foreign Keys

Foreign keys define dependencies between tables. They link entities in one table
to entities in another, enabling both data relationships and workflow dependencies.

## Basic Syntax

Foreign keys use the arrow `->` notation in table definitions:

```python
@schema
class Session(dj.Manual):
    definition = """
    -> Subject                 # references Subject table
    session_date : date
    ---
    notes='' : varchar(2000)
    """
```

This creates a dependency where each `Session` must reference an existing `Subject`.

## Foreign Key Effects

When table `B` references table `A` with `-> A`:

1. **Attribute inheritance**: `A`'s primary key attributes become part of `B`
2. **Referential constraint**: Entities in `B` cannot exist without a matching entity in `A`
3. **Cascading delete**: Deleting from `A` automatically deletes dependent entities in `B`
4. **Automatic indexing**: Indexes are created to accelerate lookups

## Primary vs Secondary Foreign Keys

### Primary Key Foreign Keys (Solid Lines in Diagrams)

Foreign keys **above** the `---` line become part of the child's primary key:

```python
@schema
class Trial(dj.Imported):
    definition = """
    -> Session             # part of primary key
    trial_id : smallint    # additional primary key attribute
    ---
    start_time : float     # (seconds)
    """
```

The `Trial` table has primary key `(subject_id, session_date, trial_id)`.

### Secondary Foreign Keys (Dashed Lines in Diagrams)

Foreign keys **below** the `---` line are secondary attributes:

```python
@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_date : date
    ---
    -> [nullable] User     # optional reference
    notes='' : varchar(2000)
    """
```

## Complete Example Schema

```python
import datajoint as dj
schema = dj.Schema('lab')

@schema
class User(dj.Lookup):
    definition = """
    username : varchar(20)
    ---
    full_name : varchar(100)
    """
    contents = [
        ('alice', 'Alice Smith'),
        ('bob', 'Bob Jones'),
    ]

@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int
    ---
    species : varchar(30)
    date_of_birth : date
    sex : enum('M', 'F', 'U')
    """

@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_date : date
    ---
    -> [nullable] User
    session_notes='' : varchar(2000)
    """

@schema
class Trial(dj.Imported):
    definition = """
    -> Session
    trial_id : smallint
    ---
    start_time : float  # seconds
    duration : float    # seconds
    """
```

## Referential Integrity

Foreign keys enforce **referential integrity**—the guarantee that related data
remains consistent:

```python
# Insert a subject
Subject.insert1({'subject_id': 1, 'species': 'mouse',
                 'date_of_birth': '2023-01-15', 'sex': 'M'})

# Insert a session - requires existing subject
Session.insert1({'subject_id': 1, 'session_date': '2024-01-01',
                 'username': 'alice'})

# This fails - subject_id=999 doesn't exist
Session.insert1({'subject_id': 999, 'session_date': '2024-01-01'})
# IntegrityError: Cannot add or update a child row: foreign key constraint fails
```

### Cascading Deletes

Deleting a parent automatically deletes all dependent children:

```python
# Delete subject 1 - also deletes all its sessions and trials
(Subject & 'subject_id=1').delete()
```

DataJoint prompts for confirmation showing all affected tables and entity counts.

## Foreign Key Options

### nullable

Makes the reference optional:

```python
@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_date : date
    ---
    -> [nullable] User     # experimenter may be unknown
    """
```

With `nullable`, the `User` attributes can be `NULL` if no user is specified.

### unique

Enforces one-to-one relationships:

```python
@schema
class Equipment(dj.Manual):
    definition = """
    equipment_id : int
    ---
    name : varchar(100)
    -> [unique] User       # each user owns at most one equipment
    """
```

### Combined Options

```python
@schema
class Rig(dj.Manual):
    definition = """
    rig_id : char(4)
    ---
    -> [unique, nullable] User   # optionally assigned to at most one user
    """
```

**Note**: Primary key foreign keys cannot be `nullable` since primary keys cannot
contain NULL values. They can be `unique`.

## Renamed Foreign Keys

Rename inherited attributes using projection syntax:

### Single Attribute Rename

```python
@schema
class Experiment(dj.Manual):
    definition = """
    experiment_id : int
    ---
    -> User.proj(experimenter='username')  # rename 'username' to 'experimenter'
    start_date : date
    """
```

### Multiple References to Same Table

When referencing a table multiple times, use renames to distinguish:

```python
@schema
class Synapse(dj.Manual):
    definition = """
    -> Cell.proj(pre_cell='cell_id')   # presynaptic cell
    -> Cell.proj(post_cell='cell_id')  # postsynaptic cell
    ---
    strength : float   # synaptic strength
    """
```

If `Cell` has primary key `(animal_id, slice_id, cell_id)`, then `Synapse` has
primary key `(animal_id, slice_id, pre_cell, post_cell)`.

### Fully Disambiguated References

To allow connections across slices, rename additional attributes:

```python
@schema
class Synapse(dj.Manual):
    definition = """
    -> Cell.proj(pre_slice='slice_id', pre_cell='cell_id')
    -> Cell.proj(post_slice='slice_id', post_cell='cell_id')
    ---
    strength : float
    """
```

Primary key: `(animal_id, pre_slice, pre_cell, post_slice, post_cell)`

## Viewing Dependencies

### Examine Table Heading

```python
Session.heading
# Shows all attributes including those inherited via foreign keys
```

### Entity Relationship Diagram

```python
dj.Diagram(schema)
# Visualize all tables and their dependencies
```

In diagrams:
- **Solid lines**: Primary key foreign keys
- **Dashed lines**: Secondary foreign keys
- **Red lines with dots**: Renamed foreign keys

## Dependency Patterns

### Hub Pattern

Multiple tables reference a central table:

```python
@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int
    ---
    ...
    """

@schema
class Surgery(dj.Manual):
    definition = """
    -> Subject
    surgery_date : date
    ---
    ...
    """

@schema
class Behavior(dj.Imported):
    definition = """
    -> Subject
    behavior_date : date
    ---
    ...
    """

@schema
class Imaging(dj.Imported):
    definition = """
    -> Subject
    imaging_date : date
    ---
    ...
    """
```

### Chain Pattern

Sequential processing pipeline:

```python
@schema
class RawData(dj.Imported):
    definition = """
    -> Session
    ---
    data : longblob
    """

@schema
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    processed : longblob
    """

@schema
class Analysis(dj.Computed):
    definition = """
    -> ProcessedData
    ---
    result : float
    """
```

### Fork/Join Pattern

Multiple paths converging:

```python
@schema
class NeuralData(dj.Imported):
    definition = """
    -> Session
    ---
    spikes : longblob
    """

@schema
class BehaviorData(dj.Imported):
    definition = """
    -> Session
    ---
    events : longblob
    """

@schema
class NeuralBehaviorAnalysis(dj.Computed):
    definition = """
    -> NeuralData
    -> BehaviorData
    ---
    correlation : float
    """
```

## Best Practices

1. **Use meaningful names**: Choose descriptive table and attribute names
2. **Keep primary keys minimal**: Include only attributes necessary to identify entities
3. **Design for queries**: Consider what joins you'll need when placing foreign keys
4. **Avoid circular dependencies**: DataJoint requires a directed acyclic graph
5. **Use nullable sparingly**: Only when the reference is truly optional
