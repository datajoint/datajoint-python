# DataJoint Python Docstring Style Guide

This document defines the canonical docstring format for datajoint-python.
All public APIs must follow this NumPy-style format for consistency and
automated documentation generation via mkdocstrings.

## Quick Reference

```python
def function(param1, param2, *, keyword_only=None):
    """
    Short one-line summary (imperative mood, no period).

    Extended description providing context and details. May span
    multiple lines. Explain what the function does, not how.

    Parameters
    ----------
    param1 : type
        Description of param1.
    param2 : type
        Description of param2.
    keyword_only : type, optional
        Description. Default is None.

    Returns
    -------
    type
        Description of return value.

    Raises
    ------
    ExceptionType
        When and why this exception is raised.

    Examples
    --------
    >>> result = function("value", 42)
    >>> print(result)
    expected_output

    See Also
    --------
    related_function : Brief description.

    Notes
    -----
    Additional technical notes, algorithms, or implementation details.
    """
```

---

## Module Docstrings

Every module must begin with a docstring explaining its purpose.

```python
"""
Connection management for DataJoint.

This module provides the Connection class that manages database connections,
transaction handling, and query execution. It also provides the ``conn()``
function for accessing a persistent shared connection.

Key Components
--------------
Connection : class
    Manages a single database connection with transaction support.
conn : function
    Returns a persistent connection object shared across modules.

Example
-------
>>> import datajoint as dj
>>> connection = dj.conn()
>>> connection.query("SHOW DATABASES")
"""
```

---

## Class Docstrings

```python
class Table(QueryExpression):
    """
    Base class for all DataJoint tables.

    Table implements data manipulation (insert, delete, update) and inherits
    query functionality from QueryExpression. Concrete table classes must
    define the ``definition`` property specifying the table structure.

    Parameters
    ----------
    None
        Tables are typically instantiated via schema decoration, not directly.

    Attributes
    ----------
    definition : str
        DataJoint table definition string (DDL).
    primary_key : list of str
        Names of primary key attributes.
    heading : Heading
        Table heading with attribute metadata.

    Examples
    --------
    Define a table using the schema decorator:

    >>> @schema
    ... class Mouse(dj.Manual):
    ...     definition = '''
    ...     mouse_id : int
    ...     ---
    ...     dob : date
    ...     sex : enum("M", "F", "U")
    ...     '''

    Insert data:

    >>> Mouse.insert1({"mouse_id": 1, "dob": "2024-01-15", "sex": "M"})

    See Also
    --------
    Manual : Table for manually entered data.
    Computed : Table for computed results.
    QueryExpression : Query operator base class.
    """
```

---

## Method Docstrings

### Standard Method

```python
def insert(self, rows, *, replace=False, skip_duplicates=False, ignore_extra_fields=False):
    """
    Insert one or more rows into the table.

    Parameters
    ----------
    rows : iterable
        Rows to insert. Each row can be:
        - dict: ``{"attr": value, ...}``
        - numpy.void: Record array element
        - sequence: Values in heading order
        - QueryExpression: Results of a query
        - pathlib.Path: Path to CSV file
    replace : bool, optional
        If True, replace existing rows with matching primary keys.
        Default is False.
    skip_duplicates : bool, optional
        If True, silently skip rows that would cause duplicate key errors.
        Default is False.
    ignore_extra_fields : bool, optional
        If True, ignore fields not in the table heading.
        Default is False.

    Returns
    -------
    None

    Raises
    ------
    DuplicateError
        When inserting a row with an existing primary key and neither
        ``replace`` nor ``skip_duplicates`` is True.
    DataJointError
        When required attributes are missing or types are incompatible.

    Examples
    --------
    Insert a single row:

    >>> Mouse.insert1({"mouse_id": 1, "dob": "2024-01-15", "sex": "M"})

    Insert multiple rows:

    >>> Mouse.insert([
    ...     {"mouse_id": 2, "dob": "2024-02-01", "sex": "F"},
    ...     {"mouse_id": 3, "dob": "2024-02-15", "sex": "M"},
    ... ])

    Insert from a query:

    >>> TargetTable.insert(SourceTable & "condition > 5")

    See Also
    --------
    insert1 : Insert exactly one row.
    """
```

### Method with Complex Return

```python
def fetch(self, *attrs, offset=None, limit=None, order_by=None, format=None, as_dict=False):
    """
    Retrieve data from the table.

    Parameters
    ----------
    *attrs : str
        Attribute names to fetch. If empty, fetches all attributes.
        Use "KEY" to fetch primary key as dict.
    offset : int, optional
        Number of rows to skip. Default is None (no offset).
    limit : int, optional
        Maximum number of rows to return. Default is None (no limit).
    order_by : str or list of str, optional
        Attribute(s) to sort by. Use "KEY" for primary key order,
        append " DESC" for descending. Default is None (unordered).
    format : {"array", "frame"}, optional
        Output format when fetching all attributes:
        - "array": numpy structured array (default)
        - "frame": pandas DataFrame
    as_dict : bool, optional
        If True, return list of dicts instead of structured array.
        Default is False.

    Returns
    -------
    numpy.ndarray or list of dict or pandas.DataFrame
        Query results in the requested format:
        - Single attribute: 1D array of values
        - Multiple attributes: tuple of 1D arrays
        - No attributes specified: structured array, DataFrame, or list of dicts

    Examples
    --------
    Fetch all data as structured array:

    >>> data = Mouse.fetch()

    Fetch specific attributes:

    >>> ids, dobs = Mouse.fetch("mouse_id", "dob")

    Fetch as list of dicts:

    >>> rows = Mouse.fetch(as_dict=True)
    >>> for row in rows:
    ...     print(row["mouse_id"])

    Fetch with ordering and limit:

    >>> recent = Mouse.fetch(order_by="dob DESC", limit=10)

    See Also
    --------
    fetch1 : Fetch exactly one row.
    head : Fetch first N rows ordered by key.
    tail : Fetch last N rows ordered by key.
    """
```

### Generator Method

```python
def make(self, key):
    """
    Compute and insert results for one key.

    This method must be implemented by subclasses of Computed or Imported
    tables. It is called by ``populate()`` for each key in ``key_source``
    that is not yet in the table.

    The method can be implemented in two ways:

    **Simple mode** (regular method):
    Fetch, compute, and insert within a single transaction.

    **Tripartite mode** (generator method):
    Split into ``make_fetch``, ``make_compute``, ``make_insert`` for
    long-running computations with deferred transactions.

    Parameters
    ----------
    key : dict
        Primary key values identifying the entity to compute.

    Yields
    ------
    tuple
        In tripartite mode, yields fetched data and computed results.

    Raises
    ------
    NotImplementedError
        If neither ``make`` nor the tripartite methods are implemented.

    Examples
    --------
    Simple implementation:

    >>> class ProcessedData(dj.Computed):
    ...     definition = '''
    ...     -> RawData
    ...     ---
    ...     result : float
    ...     '''
    ...
    ...     def make(self, key):
    ...         raw = (RawData & key).fetch1("data")
    ...         result = expensive_computation(raw)
    ...         self.insert1({**key, "result": result})

    See Also
    --------
    populate : Execute make for all pending keys.
    key_source : Query defining keys to populate.
    """
```

---

## Property Docstrings

```python
@property
def primary_key(self):
    """
    list of str : Names of primary key attributes.

    The primary key uniquely identifies each row in the table.
    Derived from the table definition.

    Examples
    --------
    >>> Mouse.primary_key
    ['mouse_id']
    """
    return self.heading.primary_key
```

---

## Parameter Types

Use these type annotations in docstrings:

| Python Type | Docstring Format |
|-------------|------------------|
| `str` | `str` |
| `int` | `int` |
| `float` | `float` |
| `bool` | `bool` |
| `None` | `None` |
| `list` | `list` or `list of str` |
| `dict` | `dict` or `dict[str, int]` |
| `tuple` | `tuple` or `tuple of (str, int)` |
| Optional | `str or None` or `str, optional` |
| Union | `str or int` |
| Literal | `{"option1", "option2"}` |
| Callable | `callable` |
| Class | `ClassName` |
| Any | `object` |

---

## Section Order

Sections must appear in this order (include only relevant sections):

1. **Short Summary** (required) - One line, imperative mood
2. **Deprecation Warning** - If applicable
3. **Extended Summary** - Additional context
4. **Parameters** - Input arguments
5. **Returns** / **Yields** - Output values
6. **Raises** - Exceptions
7. **Warns** - Warnings issued
8. **See Also** - Related functions/classes
9. **Notes** - Technical details
10. **References** - Citations
11. **Examples** (strongly encouraged) - Usage demonstrations

---

## Style Rules

### Do

- Use imperative mood: "Insert rows" not "Inserts rows"
- Start with capital letter, no period at end of summary
- Document all public methods
- Include at least one example for public APIs
- Use backticks for code: ``parameter``, ``ClassName``
- Reference related items in See Also

### Don't

- Don't document private methods extensively (brief is fine)
- Don't repeat the function signature in the description
- Don't use "This function..." or "This method..."
- Don't include implementation details in Parameters
- Don't use first person ("I", "we")

---

## Examples Section Best Practices

```python
"""
Examples
--------
Basic usage:

>>> table.insert1({"id": 1, "value": 42})

With options:

>>> table.insert(rows, skip_duplicates=True)

Error handling:

>>> try:
...     table.insert1({"id": 1})  # duplicate
... except dj.errors.DuplicateError:
...     print("Already exists")
Already exists
"""
```

---

## Converting from Sphinx Style

Replace Sphinx-style docstrings:

```python
# Before (Sphinx style)
def method(self, param1, param2):
    """
    Brief description.

    :param param1: Description of param1.
    :type param1: str
    :param param2: Description of param2.
    :type param2: int
    :returns: Description of return value.
    :rtype: bool
    :raises ValueError: When param1 is empty.
    """

# After (NumPy style)
def method(self, param1, param2):
    """
    Brief description.

    Parameters
    ----------
    param1 : str
        Description of param1.
    param2 : int
        Description of param2.

    Returns
    -------
    bool
        Description of return value.

    Raises
    ------
    ValueError
        When param1 is empty.
    """
```

---

## Validation

Docstrings are validated by:

1. **mkdocstrings** - Parses for API documentation
2. **ruff** - Linting (D100-D417 rules when enabled)
3. **pytest --doctest-modules** - Executes examples

Run locally:

```bash
# Build docs to check parsing
mkdocs build --config-file docs/mkdocs.yaml

# Check docstring examples
pytest --doctest-modules src/datajoint/
```

---

## References

- [NumPy Docstring Guide](https://numpydoc.readthedocs.io/en/latest/format.html)
- [mkdocstrings Python Handler](https://mkdocstrings.github.io/python/)
- [PEP 257 - Docstring Conventions](https://peps.python.org/pep-0257/)
