# Restriction

Restriction selects entities from a table that satisfy specific conditions.
It's the most frequently used query operator in DataJoint.

## Basic Syntax

```python
# Restriction (inclusion): select matching entities
result = Table & condition

# Exclusion: select non-matching entities
result = Table - condition
```

![Restriction and exclusion](../images/op-restrict.png){: style="width:400px; align:center"}

## Condition Types

### Dictionary Conditions

Dictionaries specify exact equality matches:

```python
# Single attribute match
Session & {'subject_id': 1}

# Multiple attribute match (AND)
Session & {'subject_id': 1, 'session_date': '2024-01-15'}

# Primary key lookup (returns at most one entity)
subject = (Subject & {'subject_id': 1}).fetch1()
```

**Note**: Unmatched dictionary keys are silently ignored:

```python
# Typo in key name - returns ALL entities (no filter applied)
Session & {'sesion_date': '2024-01-15'}  # 's' missing
```

### String Conditions

Strings allow SQL-like expressions:

```python
# Equality
Session & 'user = "Alice"'

# Inequality
Experiment & 'duration >= 60'

# Range
Subject & 'date_of_birth BETWEEN "2023-01-01" AND "2023-12-31"'

# Pattern matching
Subject & 'species LIKE "mouse%"'

# NULL checks
Session & 'notes IS NOT NULL'

# Arithmetic
Trial & 'end_time - start_time > 10'
```

### Table Conditions (Semijoins)

Restrict by related entities in another table:

```python
# Sessions that have at least one trial
Session & Trial

# Sessions with no trials
Session - Trial

# Subjects that have sessions
Subject & Session

# Subjects with no sessions
Subject - Session
```

![Restriction by another table](../images/restrict-example1.png){: style="width:546px; align:center"}

### Query Conditions

Use query expressions as conditions:

```python
# Sessions by Alice
alice_sessions = Session & 'user = "Alice"'

# Experiments in Alice's sessions
Experiment & alice_sessions

# Trials from experiments longer than 60 seconds
long_experiments = Experiment & 'duration >= 60'
Trial & long_experiments
```

## Combining Conditions

### AND Logic (Chain Restrictions)

```python
# Multiple conditions combined with AND
Session & 'user = "Alice"' & 'session_date > "2024-01-01"'

# Equivalent using AndList
Session & dj.AndList([
    'user = "Alice"',
    'session_date > "2024-01-01"'
])
```

### OR Logic (List/Tuple)

```python
# Entities matching ANY condition (OR)
Subject & ['subject_id = 1', 'subject_id = 2', 'subject_id = 3']

# Multiple users
Session & ['user = "Alice"', 'user = "Bob"']

# Equivalent using tuple
Session & ('user = "Alice"', 'user = "Bob"')
```

### NOT Logic

```python
# Exclusion operator
Session - 'user = "Alice"'  # Sessions NOT by Alice

# Not object
Session & dj.Not('user = "Alice"')  # Same result
```

### Complex Combinations

```python
# (Alice's sessions) OR (sessions after 2024)
(Session & 'user = "Alice"') + (Session & 'session_date > "2024-01-01"')

# Alice's sessions that are NOT in 2024
(Session & 'user = "Alice"') - 'session_date > "2024-01-01"'

# Sessions with trials but no experiments
(Session & Trial) - Experiment
```

## Practical Examples

### Filter by Primary Key

```python
# Fetch specific subject
subject = (Subject & {'subject_id': 5}).fetch1()

# Fetch multiple specific subjects
subjects = (Subject & [{'subject_id': 1}, {'subject_id': 2}]).fetch()
```

### Filter by Date Range

```python
# Sessions in January 2024
jan_sessions = Session & 'session_date BETWEEN "2024-01-01" AND "2024-01-31"'

# Sessions in the last 30 days
recent = Session & 'session_date >= CURDATE() - INTERVAL 30 DAY'
```

### Filter by Related Data

```python
# Subjects with at least 5 sessions
active_subjects = Subject & (
    Subject.aggr(Session, n='count(*)') & 'n >= 5'
).proj()

# Sessions with successful trials
successful_sessions = Session & (Trial & 'success = 1')

# Experiments with all trials complete
complete_experiments = Experiment - (Trial & 'status != "complete"')
```

### Filter by Computed Values

```python
# Trials longer than average
avg_duration = Trial.proj().aggr(Trial, avg='avg(duration)').fetch1('avg')
long_trials = Trial & f'duration > {avg_duration}'

# Sessions with above-average trial count
Session & (
    Session.aggr(Trial, n='count(*)') &
    f'n > {len(Trial) / len(Session)}'
).proj()
```

## Query Patterns

### Existence Check

```python
# Does subject 1 exist?
if Subject & {'subject_id': 1}:
    print("Subject exists")

# Are there any sessions today?
if Session & f'session_date = "{date.today()}"':
    print("Sessions recorded today")
```

### Find Missing Data

```python
# Subjects without sessions
orphan_subjects = Subject - Session

# Sessions without trials
empty_sessions = Session - Trial

# Experiments missing analysis
unanalyzed = Experiment - Analysis
```

### Universal Quantification

```python
# Subjects where ALL sessions are complete
# (subjects with no incomplete sessions)
complete_subjects = Subject - (Session - 'status = "complete"')

# Experiments where ALL trials succeeded
successful_experiments = Experiment - (Trial - 'success = 1')
```

### Find Related Entities

```python
# All sessions for a specific subject
subject_sessions = Session & (Subject & {'subject_id': 1})

# All trials across all sessions for a subject
subject_trials = Trial & (Session & {'subject_id': 1})
```

## Special Restrictions

### dj.Top

Limit results with optional ordering:

```python
# First 10 sessions by date
Session & dj.Top(limit=10, order_by='session_date')

# Latest 5 sessions
Session & dj.Top(limit=5, order_by='session_date DESC')

# Pagination: skip first 10, get next 10
Session & dj.Top(limit=10, offset=10, order_by='session_date')
```

### Boolean Values

```python
# True: returns all entities
Session & True  # Same as Session

# False: returns no entities
Session & False  # Empty result
```

### Empty Conditions

```python
# Empty dict: returns all entities
Session & {}  # Same as Session

# Empty list: returns no entities
Session & []  # Empty result

# Empty AndList: returns all entities
Session & dj.AndList([])  # Same as Session
```

## Performance Tips

1. **Primary key restrictions are fastest**: Use when possible
2. **Indexed attributes**: Restrictions on indexed columns are faster
3. **Chain restrictions**: `A & cond1 & cond2` is often faster than complex strings
4. **Avoid fetching then filtering**: Filter in the query, not in Python

```python
# Good: filter in query
results = (Session & 'session_date > "2024-01-01"').fetch()

# Bad: filter after fetch
all_sessions = Session.fetch(as_dict=True)
results = [s for s in all_sessions if s['session_date'] > date(2024, 1, 1)]
```

## Common Mistakes

### Typos in Dictionary Keys

```python
# Wrong: key doesn't match, returns ALL rows
Session & {'sesion_date': '2024-01-01'}

# Right: correct spelling
Session & {'session_date': '2024-01-01'}
```

### Quoting in String Conditions

```python
# Wrong: missing quotes around string value
Session & 'user = Alice'

# Right: quoted string value
Session & 'user = "Alice"'
```

### List vs AndList

```python
# List = OR (any match)
Session & ['user = "Alice"', 'user = "Bob"']  # Alice OR Bob

# AndList = AND (all must match)
Session & dj.AndList(['session_date > "2024-01-01"', 'user = "Alice"'])
```
