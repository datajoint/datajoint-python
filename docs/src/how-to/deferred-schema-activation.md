# Deferred Schema Activation

Define table classes without an immediate database connection, then activate
the schema later when ready to connect.

## When to use deferred activation

Deferred schema activation is useful when you want to:

- Define reusable table modules that work with different databases
- Write testable code where the database connection is injected at runtime
- Deploy the same pipeline to multiple environments (development, staging,
  production)
- Import table definitions without triggering database connections

## Define tables without a database connection

Create a schema object without providing a schema name:

```python
import datajoint as dj

# Create schema without activation
schema = dj.Schema()

@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int
    ---
    subject_name : varchar(64)
    """

@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_date : date
    ---
    session_notes : varchar(256)
    """
```

The `@schema` decorator queues table classes for later declaration. No database
connection is made until you call `activate()`.

## Check activation status

To check whether a schema has been activated:

```python
schema.is_activated()  # Returns False before activation
```

## Activate the schema

When ready to connect, call `activate()` with the database schema name:

```python
schema.activate('my_project')
```

This:

1. Connects to the database (using `dj.conn()` by default)
2. Creates the schema if it doesn't exist
3. Declares all queued tables in the order they were decorated

## Activate with a specific connection

To use a specific database connection:

```python
connection = dj.conn(
    host='production-server.example.com',
    user='pipeline_user',
    password='secret'
)

schema.activate('my_project', connection=connection)
```

## Activate with options

Control schema and table creation behavior:

```python
# Connect to existing schema only (don't create if missing)
schema.activate('my_project', create_schema=False)

# Don't create tables automatically
schema.activate('my_project', create_tables=False)
```

## Example: environment-based activation

```python
# pipeline/tables.py
import datajoint as dj

schema = dj.Schema()

@schema
class Experiment(dj.Manual):
    definition = """
    experiment_id : int
    ---
    experiment_date : date
    """

# pipeline/activate.py
import os
from pipeline.tables import schema

env = os.environ.get('ENVIRONMENT', 'development')

schema_names = {
    'development': 'dev_experiments',
    'staging': 'staging_experiments',
    'production': 'prod_experiments',
}

schema.activate(schema_names[env])
```

## Example: test fixtures

```python
import pytest
import datajoint as dj
from mypackage.tables import schema, Subject, Session

@pytest.fixture
def test_schema(db_credentials):
    """Activate schema with test database."""
    schema.activate(
        'test_pipeline',
        connection=dj.conn(**db_credentials)
    )
    yield schema
    schema.drop()  # Clean up after tests
```

## Restrictions

- A schema can only be activated once. Attempting to activate for a different
  database raises `DataJointError`.
- Calling `activate()` without a schema name on an unactivated schema raises
  `DataJointError`.
- Part tables should not be decorated directly; they are processed automatically
  with their master table.
