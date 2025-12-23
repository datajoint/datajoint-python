# Insert

The `insert` operation adds new entities to tables. It is the primary way data
enters a DataJoint pipeline from external sources.

## Single Entity: insert1

Use `insert1` to insert one entity at a time:

```python
# Insert as dictionary (recommended)
Subject.insert1({
    'subject_id': 1,
    'species': 'mouse',
    'date_of_birth': '2023-06-15',
    'sex': 'M'
})

# Insert as ordered sequence (matches attribute order)
Subject.insert1([1, 'mouse', '2023-06-15', 'M'])

# Insert with dict() constructor
Subject.insert1(dict(
    subject_id=1,
    species='mouse',
    date_of_birth='2023-06-15',
    sex='M'
))
```

Dictionary format is recommended because it's explicit and doesn't depend on
attribute order.

## Multiple Entities: insert

Use `insert` for batch operations with a list of entities:

```python
# Insert multiple entities
Subject.insert([
    {'subject_id': 1, 'species': 'mouse', 'date_of_birth': '2023-01-15', 'sex': 'M'},
    {'subject_id': 2, 'species': 'mouse', 'date_of_birth': '2023-02-20', 'sex': 'F'},
    {'subject_id': 3, 'species': 'rat', 'date_of_birth': '2023-03-10', 'sex': 'M'},
])

# Insert from generator (memory efficient)
def generate_subjects():
    for i in range(1000):
        yield {'subject_id': i, 'species': 'mouse',
               'date_of_birth': '2023-01-01', 'sex': 'U'}

Subject.insert(generate_subjects())

# Insert from pandas DataFrame
import pandas as pd
df = pd.DataFrame({
    'subject_id': [1, 2, 3],
    'species': ['mouse', 'mouse', 'rat'],
    'date_of_birth': ['2023-01-15', '2023-02-20', '2023-03-10'],
    'sex': ['M', 'F', 'M']
})
Subject.insert(df)

# Insert from numpy record array
import numpy as np
data = np.array([
    (1, 'mouse', '2023-01-15', 'M'),
    (2, 'mouse', '2023-02-20', 'F'),
], dtype=[('subject_id', 'i4'), ('species', 'U30'),
          ('date_of_birth', 'U10'), ('sex', 'U1')])
Subject.insert(data)
```

## Insert Options

### skip_duplicates

Silently skip entities with existing primary keys:

```python
# Insert new subjects, skip if already exists
Subject.insert(subjects, skip_duplicates=True)
```

Use for idempotent scripts that can safely be re-run.

### ignore_extra_fields

Ignore dictionary keys that don't match table attributes:

```python
# External data with extra fields
external_data = {
    'subject_id': 1,
    'species': 'mouse',
    'date_of_birth': '2023-01-15',
    'sex': 'M',
    'extra_field': 'ignored',  # not in table
    'another_field': 123       # not in table
}
Subject.insert1(external_data, ignore_extra_fields=True)
```

### replace

Replace existing entities with matching primary keys:

```python
# Update subject if exists, insert if new
Subject.insert1({
    'subject_id': 1,
    'species': 'mouse',
    'date_of_birth': '2023-01-15',
    'sex': 'F'  # corrected value
}, replace=True)
```

**Warning**: Use `replace` carefully. It circumvents DataJoint's data integrity
model. Prefer delete-and-insert for most corrections.

### allow_direct_insert

Allow inserts into auto-populated tables outside of `make()`:

```python
# Normally auto-populated tables only allow inserts in make()
# This overrides that restriction
ComputedTable.insert1(data, allow_direct_insert=True)
```

Use sparingly, typically for data migration or recovery.

## Batch Insert Behavior

Batched inserts differ from individual inserts:

1. **Reduced network overhead**: One round-trip instead of many
2. **Atomic transaction**: All-or-nothing (if one fails, none are inserted)

```python
# Efficient: single transaction
Subject.insert([entity1, entity2, entity3])  # ~10ms total

# Less efficient: multiple transactions
for entity in [entity1, entity2, entity3]:
    Subject.insert1(entity)  # ~10ms each = ~30ms total
```

For very large batches, break into chunks to avoid buffer limits:

```python
def chunked_insert(table, entities, chunk_size=500):
    """Insert entities in chunks."""
    chunk = []
    for entity in entities:
        chunk.append(entity)
        if len(chunk) >= chunk_size:
            table.insert(chunk, skip_duplicates=True)
            chunk = []
    if chunk:
        table.insert(chunk, skip_duplicates=True)

chunked_insert(Subject, large_entity_list)
```

## Server-Side Insert

Insert data from one table to another without local transfer:

```python
# Server-side: data never leaves the database
TargetTable.insert(SourceTable & 'condition="value"')

# Equivalent but slower: fetch then insert
data = (SourceTable & 'condition="value"').fetch()
TargetTable.insert(data)
```

Server-side inserts are efficient for:
- Copying between schemas
- Populating from query results
- Data migration

```python
# Copy all protocols from phase 1 to phase 2
phase2.Protocol.insert(phase1.Protocol)

# Copy subset with projection
phase2.Summary.insert(
    phase1.Experiment.proj('experiment_id', 'start_date')
    & 'start_date > "2024-01-01"'
)
```

## Referential Integrity

Inserts must satisfy foreign key constraints:

```python
# Subject must exist before Session can reference it
Subject.insert1({'subject_id': 1, 'species': 'mouse', ...})
Session.insert1({'subject_id': 1, 'session_date': '2024-01-15', ...})

# This fails - subject_id=999 doesn't exist
Session.insert1({'subject_id': 999, 'session_date': '2024-01-15'})
# IntegrityError: foreign key constraint fails
```

## Object Attributes

Tables with [`object`](../datatypes/object.md) type attributes accept various input formats:

```python
@schema
class Recording(dj.Manual):
    definition = """
    recording_id : int
    ---
    raw_data : <object@store>
    """

# Insert from local file
Recording.insert1({
    'recording_id': 1,
    'raw_data': '/local/path/to/data.dat'
})

# Insert from local folder
Recording.insert1({
    'recording_id': 2,
    'raw_data': '/local/path/to/data_folder/'
})

# Insert from remote URL (S3, GCS, Azure, HTTP)
Recording.insert1({
    'recording_id': 3,
    'raw_data': 's3://bucket/path/to/data.dat'
})

# Insert from stream with extension
with open('/path/to/data.bin', 'rb') as f:
    Recording.insert1({
        'recording_id': 4,
        'raw_data': ('.bin', f)
    })
```

### Staged Inserts

For large objects (Zarr arrays, HDF5), write directly to storage:

```python
import zarr

with Recording.staged_insert1 as staged:
    # Set key values
    staged.rec['recording_id'] = 5

    # Create Zarr array directly in object storage
    z = zarr.open(staged.store('raw_data', '.zarr'), mode='w',
                  shape=(10000, 10000), dtype='f4')
    z[:] = compute_large_array()

    # Assign to record
    staged.rec['raw_data'] = z

# On success: metadata computed, record inserted
# On exception: storage cleaned up, nothing inserted
```

## Common Patterns

### Ingestion Script

```python
def ingest_subjects(csv_file):
    """Ingest subjects from CSV file."""
    import pandas as pd
    df = pd.read_csv(csv_file)

    # Validate and transform
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth']).dt.date
    df['sex'] = df['sex'].str.upper()

    # Insert with conflict handling
    Subject.insert(df.to_dict('records'),
                   skip_duplicates=True,
                   ignore_extra_fields=True)
```

### Conditional Insert

```python
def insert_if_missing(table, entity):
    """Insert entity only if not already present."""
    key = {k: entity[k] for k in table.primary_key}
    if not (table & key):
        table.insert1(entity)
```

### Insert with Default Values

```python
# Table with defaults
@schema
class Experiment(dj.Manual):
    definition = """
    experiment_id : int
    ---
    notes='' : varchar(2000)
    status='pending' : enum('pending', 'running', 'complete')
    created=CURRENT_TIMESTAMP : timestamp
    """

# Defaults are applied automatically
Experiment.insert1({'experiment_id': 1})
# Result: notes='', status='pending', created=<current time>
```

## Best Practices

1. **Use dictionaries**: Explicit attribute names prevent ordering errors
2. **Batch when possible**: Reduce network overhead with multi-entity inserts
3. **Use skip_duplicates for idempotency**: Safe to re-run scripts
4. **Validate before insert**: Check data quality before committing
5. **Handle errors gracefully**: Wrap inserts in try/except for production code
6. **Use server-side inserts**: When copying between tables
