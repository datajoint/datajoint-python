# Fetch

The `fetch` operation retrieves data from query results into Python. It's the
second step after constructing a query with [operators](operators.md).

## Basic Fetch

### Fetch All Entities

```python
# As NumPy recarray (default)
data = Subject.fetch()

# As list of dictionaries
data = Subject.fetch(as_dict=True)

# As pandas DataFrame
data = Subject.fetch(format='frame')
```

### Fetch Single Entity

Use `fetch1` when the query returns exactly one entity:

```python
# Fetch entire entity
subject = (Subject & 'subject_id=1').fetch1()
# Returns: {'subject_id': 1, 'species': 'mouse', ...}

# Raises error if zero or multiple entities match
```

### Fetch Specific Attributes

```python
# Single attribute returns 1D array
names = Subject.fetch('species')
# Returns: array(['mouse', 'mouse', 'rat', ...])

# Multiple attributes return tuple of arrays
ids, species = Subject.fetch('subject_id', 'species')

# With fetch1, returns scalar values
subject_id, species = (Subject & 'subject_id=1').fetch1('subject_id', 'species')
# Returns: (1, 'mouse')
```

### Fetch Primary Keys

```python
# List of key dictionaries
keys = Subject.fetch('KEY')
# Returns: [{'subject_id': 1}, {'subject_id': 2}, ...]

# Single key
key = (Subject & 'subject_id=1').fetch1('KEY')
# Returns: {'subject_id': 1}
```

## Output Formats

### NumPy Recarray (Default)

```python
data = Subject.fetch()
# Access attributes by name
data['subject_id']
data['species']

# Iterate over entities
for entity in data:
    print(entity['subject_id'], entity['species'])
```

### List of Dictionaries

```python
data = Subject.fetch(as_dict=True)
# [{'subject_id': 1, 'species': 'mouse', ...}, ...]

for entity in data:
    print(entity['subject_id'])
```

### Pandas DataFrame

```python
df = Subject.fetch(format='frame')
# DataFrame indexed by primary key

# Query on the DataFrame
df[df['species'] == 'mouse']
df.groupby('sex').count()
```

## Sorting and Limiting

### Order By

```python
# Ascending (default)
data = Subject.fetch(order_by='date_of_birth')

# Descending
data = Subject.fetch(order_by='date_of_birth desc')

# Multiple attributes
data = Subject.fetch(order_by=('species', 'date_of_birth desc'))

# By primary key
data = Subject.fetch(order_by='KEY')

# SQL reserved words require backticks
data = Table.fetch(order_by='`select` desc')
```

### Limit and Offset

```python
# First 10 entities
data = Subject.fetch(limit=10)

# Entities 11-20 (skip first 10)
data = Subject.fetch(limit=10, offset=10)

# Most recent 5 subjects
data = Subject.fetch(order_by='date_of_birth desc', limit=5)
```

**Note**: `offset` requires `limit` to be specified.

## Practical Examples

### Query and Filter

```python
# Fetch subjects of a specific species
mice = (Subject & 'species="mouse"').fetch()

# Fetch with complex restriction
recent_mice = (Subject & 'species="mouse"'
                       & 'date_of_birth > "2023-01-01"').fetch(as_dict=True)
```

### Fetch with Projection

```python
# Fetch only specific attributes
data = Subject.proj('species', 'sex').fetch()

# Rename attributes
data = Subject.proj(animal_species='species').fetch()
```

### Fetch from Joins

```python
# Fetch combined data from multiple tables
data = (Session * Subject).fetch()

# Select attributes from join
ids, dates, species = (Session * Subject).fetch(
    'session_id', 'session_date', 'species'
)
```

### Aggregation Results

```python
# Count sessions per subject
session_counts = (Subject.aggr(Session, count='count(*)')).fetch()

# Average duration per subject
avg_durations = (Subject.aggr(Trial, avg_dur='avg(duration)')).fetch()
```

## Working with Blobs

Blob attributes contain serialized Python objects:

```python
@schema
class Image(dj.Manual):
    definition = """
    image_id : int
    ---
    pixels : longblob  # numpy array
    metadata : longblob  # dict
    """

# Fetch returns deserialized objects
image = (Image & 'image_id=1').fetch1()
pixels = image['pixels']  # numpy array
metadata = image['metadata']  # dict

# Fetch specific blob attribute
pixels = (Image & 'image_id=1').fetch1('pixels')
```

## Object Attributes

[Object](../datatypes/object.md) attributes return `ObjectRef` handles for
efficient access to large files:

```python
record = Recording.fetch1()
obj = record['raw_data']

# Metadata (no I/O)
print(obj.path)      # Storage path
print(obj.size)      # Size in bytes
print(obj.checksum)  # Content hash
print(obj.is_dir)    # True if folder

# Read content
content = obj.read()  # Returns bytes

# Open as file
with obj.open() as f:
    data = f.read()

# Download locally
local_path = obj.download('/local/destination/')
```

### Zarr and Xarray Integration

```python
import zarr
import xarray as xr

obj = Recording.fetch1()['neural_data']

# Open as Zarr
z = zarr.open(obj.store, mode='r')
data = z[:]

# Open with xarray
ds = xr.open_zarr(obj.store)
```

## Performance Considerations

### Check Size Before Fetching

```python
# Check table size before fetch
print(f"Table size: {Subject.size_on_disk / 1e6:.2f} MB")
print(f"Entity count: {len(Subject)}")
```

### Stream Large Results

```python
# Process entities one at a time (memory efficient)
for entity in Subject.fetch(as_dict=True):
    process(entity)

# Or with a cursor
for key in Subject.fetch('KEY'):
    entity = (Subject & key).fetch1()
    process(entity)
```

### Fetch Only What You Need

```python
# Bad: fetch everything, use only ID
all_data = Subject.fetch()
ids = all_data['subject_id']

# Good: fetch only needed attribute
ids = Subject.fetch('subject_id')
```

## Common Patterns

### Conditional Fetch

```python
def get_subject(subject_id):
    """Fetch subject if exists, else None."""
    query = Subject & {'subject_id': subject_id}
    if query:
        return query.fetch1()
    return None
```

### Fetch with Defaults

```python
def fetch_with_default(query, attribute, default=None):
    """Fetch attribute with default value."""
    try:
        return query.fetch1(attribute)
    except DataJointError:
        return default
```

### Batch Processing

```python
def process_in_batches(table, batch_size=100):
    """Process table in batches."""
    keys = table.fetch('KEY')
    for i in range(0, len(keys), batch_size):
        batch_keys = keys[i:i + batch_size]
        batch_data = (table & batch_keys).fetch(as_dict=True)
        yield batch_data
```

## Entity Ordering Note

Fetch results are **not guaranteed to be in any particular order** unless
`order_by` is specified. The order may vary between queries. If you need
matching pairs of attributes, fetch them in a single call:

```python
# Correct: attributes are matched
ids, names = Subject.fetch('subject_id', 'species')

# Risky: separate fetches may return different orders
ids = Subject.fetch('subject_id')
names = Subject.fetch('species')  # May not match ids!
```
