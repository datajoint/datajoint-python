# DataJoint 2.0 Fetch API Specification

## Overview

DataJoint 2.0 replaces the complex `fetch()` method with a set of explicit, composable output methods. This provides better discoverability, clearer intent, and more efficient iteration.

## Design Principles

1. **Explicit over implicit**: Each output format has its own method
2. **Composable**: Use existing `.proj()` for column selection
3. **Lazy iteration**: Single cursor streaming instead of fetch-all-keys
4. **Modern formats**: First-class support for polars and Arrow

---

## New API Reference

### Output Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `to_dicts()` | `list[dict]` | All rows as list of dictionaries |
| `to_pandas()` | `DataFrame` | pandas DataFrame with primary key as index |
| `to_polars()` | `polars.DataFrame` | polars DataFrame (requires `datajoint[polars]`) |
| `to_arrow()` | `pyarrow.Table` | PyArrow Table (requires `datajoint[arrow]`) |
| `to_arrays()` | `np.ndarray` | numpy structured array (recarray) |
| `to_arrays('a', 'b')` | `tuple[array, array]` | Tuple of arrays for specific columns |
| `keys()` | `list[dict]` | Primary key values only |
| `fetch1()` | `dict` | Single row as dict (raises if not exactly 1) |
| `fetch1('a', 'b')` | `tuple` | Single row attribute values |

### Common Parameters

All output methods accept these optional parameters:

```python
table.to_dicts(
    order_by=None,      # str or list: column(s) to sort by, e.g. "KEY", "name DESC"
    limit=None,         # int: maximum rows to return
    offset=None,        # int: rows to skip
    squeeze=False,      # bool: remove singleton dimensions from arrays
    download_path="."   # str: path for downloading external data
)
```

### Iteration

```python
# Lazy streaming - yields one dict per row from database cursor
for row in table:
    process(row)  # row is a dict
```

---

## Migration Guide

### Basic Fetch Operations

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `table.fetch()` | `table.to_arrays()` or `table.to_dicts()` |
| `table.fetch(format="array")` | `table.to_arrays()` |
| `table.fetch(format="frame")` | `table.to_pandas()` |
| `table.fetch(as_dict=True)` | `table.to_dicts()` |

### Attribute Fetching

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `table.fetch('a')` | `table.to_arrays('a')` |
| `a, b = table.fetch('a', 'b')` | `a, b = table.to_arrays('a', 'b')` |
| `table.fetch('a', 'b', as_dict=True)` | `table.proj('a', 'b').to_dicts()` |

### Primary Key Fetching

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `table.fetch('KEY')` | `table.keys()` |
| `table.fetch(dj.key)` | `table.keys()` |
| `keys, a = table.fetch('KEY', 'a')` | See note below |

For mixed KEY + attribute fetch:
```python
# Old: keys, a = table.fetch('KEY', 'a')
# New: Combine keys() with to_arrays()
keys = table.keys()
a = table.to_arrays('a')
# Or use to_dicts() which includes all columns
```

### Ordering, Limiting, Offset

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `table.fetch(order_by='name')` | `table.to_arrays(order_by='name')` |
| `table.fetch(limit=10)` | `table.to_arrays(limit=10)` |
| `table.fetch(order_by='KEY', limit=10, offset=5)` | `table.to_arrays(order_by='KEY', limit=10, offset=5)` |

### Single Row Fetch (fetch1)

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `table.fetch1()` | `table.fetch1()` (unchanged) |
| `a, b = table.fetch1('a', 'b')` | `a, b = table.fetch1('a', 'b')` (unchanged) |
| `table.fetch1('KEY')` | `table.fetch1()` then extract pk columns |

### Configuration

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `dj.config['fetch_format'] = 'frame'` | Use `.to_pandas()` explicitly |
| `with dj.config.override(fetch_format='frame'):` | Use `.to_pandas()` in the block |

### Iteration

| Old Pattern (1.x) | New Pattern (2.0) |
|-------------------|-------------------|
| `for row in table:` | `for row in table:` (same syntax, now lazy!) |
| `list(table)` | `table.to_dicts()` |

### Column Selection with proj()

Use `.proj()` for column selection, then apply output method:

```python
# Select specific columns
table.proj('col1', 'col2').to_pandas()
table.proj('col1', 'col2').to_dicts()

# Computed columns
table.proj(total='price * quantity').to_pandas()
```

---

## Removed Features

### Removed Methods and Parameters

- `fetch()` method - use explicit output methods
- `fetch('KEY')` - use `keys()`
- `dj.key` class - use `keys()` method
- `format=` parameter - use explicit methods
- `as_dict=` parameter - use `to_dicts()`
- `config['fetch_format']` setting - use explicit methods

### Removed Imports

```python
# Old (removed)
from datajoint import key
result = table.fetch(dj.key)

# New
result = table.keys()
```

---

## Examples

### Example 1: Basic Data Retrieval

```python
# Get all data as DataFrame
df = Experiment().to_pandas()

# Get all data as list of dicts
rows = Experiment().to_dicts()

# Get all data as numpy array
arr = Experiment().to_arrays()
```

### Example 2: Filtered and Sorted Query

```python
# Get recent experiments, sorted by date
recent = (Experiment() & 'date > "2024-01-01"').to_pandas(
    order_by='date DESC',
    limit=100
)
```

### Example 3: Specific Columns

```python
# Fetch specific columns as arrays
names, dates = Experiment().to_arrays('name', 'date')

# Or with primary key included
names, dates = Experiment().to_arrays('name', 'date', include_key=True)
```

### Example 4: Primary Keys for Iteration

```python
# Get keys for restriction
keys = Experiment().keys()
for key in keys:
    process(Session() & key)
```

### Example 5: Single Row

```python
# Get one row as dict
row = (Experiment() & key).fetch1()

# Get specific attributes
name, date = (Experiment() & key).fetch1('name', 'date')
```

### Example 6: Lazy Iteration

```python
# Stream rows efficiently (single database cursor)
for row in Experiment():
    if should_process(row):
        process(row)
    if done:
        break  # Early termination - no wasted fetches
```

### Example 7: Modern DataFrame Libraries

```python
# Polars (fast, modern)
import polars as pl
df = Experiment().to_polars()
result = df.filter(pl.col('value') > 100).group_by('category').agg(pl.mean('value'))

# PyArrow (zero-copy interop)
table = Experiment().to_arrow()
# Can convert to pandas or polars with zero copy
```

---

## Performance Considerations

### Lazy Iteration

The new iteration is significantly more efficient:

```python
# Old (1.x): N+1 queries
# 1. fetch("KEY") gets ALL keys
# 2. fetch1() for EACH key

# New (2.0): Single query
# Streams rows from one cursor
for row in table:
    ...
```

### Memory Efficiency

- `to_dicts()`: Returns full list in memory
- `for row in table:`: Streams one row at a time
- `to_arrays(limit=N)`: Fetches only N rows

### Format Selection

| Use Case | Recommended Method |
|----------|-------------------|
| Data analysis | `to_pandas()` or `to_polars()` |
| JSON API responses | `to_dicts()` |
| Numeric computation | `to_arrays()` |
| Large datasets | `for row in table:` (streaming) |
| Interop with other tools | `to_arrow()` |

---

## Error Messages

When attempting to use removed methods, users see helpful error messages:

```python
>>> table.fetch()
AttributeError: fetch() has been removed in DataJoint 2.0.
Use to_dicts(), to_pandas(), to_arrays(), or keys() instead.
See table.fetch.__doc__ for details.
```

---

## Optional Dependencies

Install optional dependencies for additional output formats:

```bash
# For polars support
pip install datajoint[polars]

# For PyArrow support
pip install datajoint[arrow]

# For both
pip install datajoint[polars,arrow]
```
