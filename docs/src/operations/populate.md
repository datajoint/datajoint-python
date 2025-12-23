# Auto-populate

Auto-populated tables (`dj.Imported` and `dj.Computed`) automatically compute and
insert their data based on upstream tables. They define a `make()` method that
specifies how to compute each entry.

## Defining Auto-populated Tables

### Basic Structure

```python
@schema
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    mean_value : float
    std_value : float
    """

    def make(self, key):
        # 1. Fetch data from upstream tables
        data = (Recording & key).fetch1('data')

        # 2. Compute results
        result = dict(
            key,
            mean_value=data.mean(),
            std_value=data.std()
        )

        # 3. Insert into self
        self.insert1(result)
```

### Imported vs Computed

```python
# Use Imported when accessing external files
@schema
class RawData(dj.Imported):
    definition = """
    -> Session
    ---
    data : longblob
    """

    def make(self, key):
        # Access external file system
        filepath = (Session & key).fetch1('data_path')
        data = load_from_file(filepath)
        self.insert1(dict(key, data=data))

# Use Computed when only using upstream tables
@schema
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    processed : longblob
    """

    def make(self, key):
        # Only access DataJoint tables
        raw = (RawData & key).fetch1('data')
        self.insert1(dict(key, processed=process(raw)))
```

## The make() Method

The `make(self, key)` method receives a primary key dictionary and must:

1. **Fetch** data from upstream tables using `key` for restriction
2. **Compute** the results
3. **Insert** into `self`

```python
def make(self, key):
    # key contains primary key values, e.g., {'subject_id': 1, 'session_date': '2024-01-15'}

    # Fetch upstream data
    raw_data = (RawData & key).fetch1('data')
    params = (ProcessingParams & key).fetch1()

    # Compute
    result = analyze(raw_data, **params)

    # Insert - add computed values to key
    self.insert1(dict(key, result=result))
```

### Multiple Inserts per make()

When a table adds dimensions to the primary key:

```python
@schema
class TrialAnalysis(dj.Computed):
    definition = """
    -> Session
    trial_num : int
    ---
    metric : float
    """

    def make(self, key):
        # key only has session info, we generate trial_num
        trials = (Trial & key).fetch(as_dict=True)

        for trial in trials:
            metric = compute_metric(trial)
            self.insert1(dict(key, trial_num=trial['trial_num'], metric=metric))
```

### Master-Part Pattern

For tables with part tables:

```python
@schema
class Segmentation(dj.Computed):
    definition = """
    -> Image
    ---
    num_cells : int
    """

    class Cell(dj.Part):
        definition = """
        -> master
        cell_id : int
        ---
        center_x : float
        center_y : float
        area : float
        """

    def make(self, key):
        image = (Image & key).fetch1('pixels')
        cells = segment_image(image)

        # Insert master
        self.insert1(dict(key, num_cells=len(cells)))

        # Insert parts
        self.Cell.insert([
            dict(key, cell_id=i, **cell)
            for i, cell in enumerate(cells)
        ])
```

## Running populate()

### Basic Usage

```python
# Populate all missing entries
Analysis.populate()

# Show progress bar
Analysis.populate(display_progress=True)

# Restrict to specific keys
Analysis.populate(Recording & 'session_date > "2024-01-01"')
```

### Populate Options

| Option | Default | Description |
|--------|---------|-------------|
| `restrictions` | None | Restrict which keys to populate |
| `display_progress` | False | Show progress bar |
| `limit` | None | Maximum keys to check |
| `max_calls` | None | Maximum make() calls |
| `order` | 'original' | Order: 'original', 'reverse', 'random' |
| `suppress_errors` | False | Continue on errors |
| `reserve_jobs` | False | Enable distributed job reservation |

```python
# Populate with options
Analysis.populate(
    restrictions='subject_id < 100',
    display_progress=True,
    max_calls=50,
    order='random',
    suppress_errors=True,
    reserve_jobs=True
)
```

### Check Progress

```python
# Print progress summary
Analysis.progress()
# Output: Analysis: 150/200 (75.0%)

# Get counts without printing
done, total = Analysis.progress(display=False)

# Progress for restricted subset
Analysis.progress('subject_id < 10')
```

## Distributed Processing

For parallel processing across multiple workers, use job reservation:

```python
# Worker 1
Analysis.populate(reserve_jobs=True)

# Worker 2 (different machine/process)
Analysis.populate(reserve_jobs=True)
```

Each worker reserves keys before processing, preventing duplicates.
See [Jobs](jobs.md) for detailed job management.

## Error Handling

### Suppress and Log Errors

```python
# Continue processing despite errors
errors = Analysis.populate(
    suppress_errors=True,
    reserve_jobs=True
)

# errors contains list of error messages
for error in errors:
    print(error)

# Get exception objects instead
exceptions = Analysis.populate(
    suppress_errors=True,
    return_exception_objects=True
)
```

### View Failed Jobs

```python
# Access jobs table
schema.jobs

# View errors
(schema.jobs & 'status="error"').fetch()

# Retry failed jobs
(schema.jobs & 'status="error"').delete()
Analysis.populate(reserve_jobs=True)
```

## Three-Part Make Pattern

For long-running computations, split `make()` into three phases to minimize
database lock time:

```python
@schema
class LongAnalysis(dj.Computed):
    definition = """
    -> Recording
    ---
    result : longblob
    duration : float
    """

    def make_fetch(self, key):
        """Phase 1: Fetch data (short transaction)"""
        data = (Recording & key).fetch1('data')
        return (data,)  # Must return tuple/list

    def make_compute(self, key, data):
        """Phase 2: Compute (no transaction - can take hours)"""
        import time
        start = time.time()
        result = expensive_analysis(data)
        duration = time.time() - start
        return (result, duration)  # Must return tuple/list

    def make_insert(self, key, result, duration):
        """Phase 3: Insert (short transaction)"""
        self.insert1(dict(key, result=result, duration=duration))
```

### How It Works

1. `make_fetch()` runs in a short transaction to get data
2. `make_compute()` runs outside any transaction (can take hours)
3. Before `make_insert()`, data is re-fetched and verified unchanged
4. `make_insert()` runs in a short transaction

This prevents long-held database locks during expensive computations.

### Generator Pattern (Alternative)

```python
def make(self, key):
    # Fetch
    data = (Recording & key).fetch1('data')
    computed = yield (data,)  # Yield fetched data

    if computed is None:
        # Compute (outside transaction)
        result = expensive_analysis(data)
        computed = yield (result,)

    # Insert
    self.insert1(dict(key, result=computed[0]))
    yield  # Signal completion
```

## Common Patterns

### Conditional Computation

```python
def make(self, key):
    params = (Params & key).fetch1()

    if params['method'] == 'fast':
        result = fast_analysis(key)
    else:
        result = thorough_analysis(key)

    self.insert1(dict(key, result=result))
```

### Skip Invalid Keys

```python
def make(self, key):
    data = (Recording & key).fetch1('data')

    if not is_valid(data):
        # Insert placeholder or skip
        self.insert1(dict(key, result=None, valid=False))
        return

    result = analyze(data)
    self.insert1(dict(key, result=result, valid=True))
```

### External Tool Integration

```python
def make(self, key):
    import subprocess

    # Export data
    data = (Recording & key).fetch1('data')
    input_file = f'/tmp/input_{key["recording_id"]}.dat'
    save_data(data, input_file)

    # Run external tool
    output_file = f'/tmp/output_{key["recording_id"]}.dat'
    subprocess.run(['analyze', input_file, '-o', output_file])

    # Import results
    result = load_data(output_file)
    self.insert1(dict(key, result=result))

    # Cleanup
    os.remove(input_file)
    os.remove(output_file)
```

## Best Practices

1. **Keep make() idempotent**: Same input should produce same output
2. **Use transactions wisely**: Long computations outside transactions
3. **Handle errors gracefully**: Use `suppress_errors` for batch processing
4. **Monitor progress**: Use `display_progress=True` for long jobs
5. **Distribute work**: Use `reserve_jobs=True` for parallel processing
6. **Clean up resources**: Remove temporary files after processing
