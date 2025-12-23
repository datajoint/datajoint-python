# Data Tiers

DataJoint assigns all tables to one of four data tiers that differentiate how
the data originate. The tier determines both the table's behavior and how it
should be treated in terms of backup and data management.

## Table Tiers Overview

| Tier | Superclass | Origin | Auto-populated |
|------|------------|--------|----------------|
| Lookup | `dj.Lookup` | Predefined facts and parameters | No |
| Manual | `dj.Manual` | External entry (users, scripts) | No |
| Imported | `dj.Imported` | External data sources + upstream | Yes |
| Computed | `dj.Computed` | Upstream tables only | Yes |

## Lookup Tables

Lookup tables store **predefined facts, parameters, and options** that are
independent of any specific experiment or dataset. Their contents are typically
defined in code alongside the table definition.

```python
@schema
class Species(dj.Lookup):
    definition = """
    species : varchar(30)
    ---
    species_class : enum('mammal', 'bird', 'fish', 'reptile')
    typical_lifespan : smallint  # years
    """
    contents = [
        ('mouse', 'mammal', 3),
        ('rat', 'mammal', 3),
        ('zebrafish', 'fish', 5),
        ('macaque', 'mammal', 30),
    ]
```

The `contents` attribute automatically populates the table when the schema is
first activated. Use lookup tables for:

- Species, strains, genotypes
- Experiment parameters and configurations
- Equipment and device catalogs
- Standard protocols and methods

```python
@schema
class StimProtocol(dj.Lookup):
    definition = """
    protocol_name : varchar(50)
    ---
    duration : float  # seconds
    frequency : float  # Hz
    amplitude : float  # arbitrary units
    description : varchar(255)
    """
    contents = [
        ('baseline', 0, 0, 0, 'No stimulation'),
        ('low_freq', 10.0, 1.0, 0.5, 'Low frequency stimulation'),
        ('high_freq', 10.0, 10.0, 0.5, 'High frequency stimulation'),
    ]
```

## Manual Tables

Manual tables store **externally entered data** that originates outside the
DataJoint pipeline. This includes data entered by users through interfaces,
imported from external systems, or ingested from raw data files.

```python
@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int  # unique subject identifier
    ---
    species : varchar(30)
    date_of_birth : date
    sex : enum('M', 'F', 'U')
    subject_notes='' : varchar(4000)
    """
```

Manual data is the **most valuable** since it cannot be regenerated from other
tables. Always ensure manual tables are backed up. Common uses:

- Subject/animal information
- Session metadata
- User-entered annotations
- Raw data file references

```python
@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_date : date
    ---
    -> [nullable] User
    session_notes='' : varchar(2000)
    data_path='' : varchar(255)
    """
```

## Imported Tables

Imported tables are **auto-populated** but require access to **external data
sources** (files, instruments, APIs) in addition to upstream DataJoint tables.
They define a `make()` method that reads external data.

```python
@schema
class Recording(dj.Imported):
    definition = """
    -> Session
    recording_id : smallint
    ---
    duration : float  # seconds
    sampling_rate : float  # Hz
    """

    def make(self, key):
        # Read from external data files
        data_path = (Session & key).fetch1('data_path')
        recording_files = list_recordings(data_path)

        for i, rec_file in enumerate(recording_files):
            metadata = read_recording_metadata(rec_file)
            self.insert1(dict(
                key,
                recording_id=i,
                duration=metadata['duration'],
                sampling_rate=metadata['sampling_rate']
            ))
```

Use imported tables when data comes from:

- Raw data files (electrophysiology, imaging)
- External databases or APIs
- Instrument outputs
- File system scans

## Computed Tables

Computed tables are **auto-populated** using **only upstream DataJoint tables**.
No external data sources are accessed. This makes computed data the safest to
regenerate if lost.

```python
@schema
class FilteredSignal(dj.Computed):
    definition = """
    -> Recording
    ---
    filtered_data : longblob
    snr : float  # signal-to-noise ratio
    """

    def make(self, key):
        # Fetch data from upstream tables only
        raw_data = (RawSignal & key).fetch1('signal')

        # Compute results
        filtered = bandpass_filter(raw_data, low=1, high=100)
        snr = compute_snr(filtered)

        self.insert1(dict(key, filtered_data=filtered, snr=snr))
```

Computed tables are ideal for:

- Signal processing results
- Statistical analyses
- Machine learning outputs
- Derived metrics and features

## Auto-Population

Imported and Computed tables support the `populate()` method:

```python
# Populate all pending entries
FilteredSignal.populate()

# Show progress
FilteredSignal.populate(display_progress=True)

# Restrict to specific keys
FilteredSignal.populate(Recording & 'session_date > "2024-01-01"')

# Distributed processing with job reservation
FilteredSignal.populate(reserve_jobs=True)
```

See [Populate](../../operations/populate.md) for details.

## Choosing the Right Tier

| Scenario | Tier |
|----------|------|
| Experiment parameters that rarely change | Lookup |
| Subject information entered by users | Manual |
| Raw data imported from files | Imported |
| Processed results from raw data | Computed |
| Derived metrics from processed data | Computed |
| External database sync | Imported |

## Data Value and Backup

| Tier | Data Value | Backup Priority |
|------|------------|-----------------|
| Manual | Highest (irreplaceable) | Critical |
| Imported | High (external source needed) | High |
| Computed | Lower (can regenerate) | Optional |
| Lookup | Low (defined in code) | Low |

Database administrators use tier information to set appropriate backup policies.
Computed data can often be excluded from backups since it can be regenerated
from source tables.

## Internal Table Naming

DataJoint prefixes table names on the server to indicate tier:

| Tier | Prefix | Example |
|------|--------|---------|
| Manual | (none) | `subject` |
| Lookup | `#` | `#species` |
| Imported | `_` | `_recording` |
| Computed | `__` | `__filtered_signal` |

Users don't need to know these conventions—DataJoint handles naming automatically.
