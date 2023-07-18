# Key Source

## Default key source

**Key source** refers to the set of primary key values over which 
[autopopulate](./populate.md) iterates, calling the `make` method at each iteration.
Each `key` from the key source is passed to the table's `make` call.
By default, the key source for a table is the [join](../query/join.md) of its primary 
[dependencies](../design/tables/dependencies.md).

For example, consider a schema with three tables.
The `Stimulus` table contains one attribute `stimulus_type` with one of two values, 
"Visual" or "Auditory".
The `Modality` table contains one attribute `modality` with one of three values, "EEG", 
"fMRI", and "PET".
The `Protocol` table has primary dependencies on both the `Stimulus` and `Modality` tables.

The key source for `Protocol` will then be all six combinations of `stimulus_type` and 
`modality` as shown in the figure below.

![Combination of stimulus_type and modality](../images/key_source_combination.png){: style="align:center"}

## Custom key source

A custom key source can be configured by setting the `key_source` property within a 
table class, after the `definition` string.

Any [query object](../query/fetch.md) can be used as the key source.
In most cases the new key source will be some alteration of the default key source.
Custom key sources often involve restriction to limit the key source to only relevant 
entities.
Other designs may involve using only one of a table's primary dependencies.

In the example below, the `EEG` table depends on the `Recording` table that lists all 
recording sessions.
However, the `populate` method of `EEG` should only ingest recordings where the 
`recording_type` is `EEG`.
Setting a custom key source prevents the `populate` call from iterating over recordings 
of the wrong type.

```python
@schema
class EEG(dj.Imported):
definition = """
-> Recording
---
sample_rate : float
eeg_data : longblob
"""
key_source = Recording & 'recording_type = "EEG"'
```
