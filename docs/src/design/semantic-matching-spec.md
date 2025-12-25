# Semantic Matching for Joins - Specification

## Overview

This document specifies **semantic matching** for joins in DataJoint 2.0, replacing the current name-based matching rules. Semantic matching ensures that attributes are only matched when they share both the same name and the same **lineage** (origin), preventing accidental joins on unrelated attributes that happen to share names.

### Goals

1. **Prevent incorrect joins** on attributes that share names but represent different entities
2. **Enable valid joins** that are currently blocked due to overly restrictive rules
3. **Maintain backward compatibility** for well-designed schemas
4. **Provide clear error messages** when semantic conflicts are detected

## Problem Statement

### Current Behavior

The current join implementation matches attributes purely by name:

```python
join_attributes = set(n for n in self.heading.names if n in other.heading.names)
```

This is essentially a SQL `NATURAL JOIN` - any attributes with the same name in both tables are used for matching. The only constraint is the "join compatibility" check which prevents joining on secondary attributes that appear in both tables.

**Location**: `src/datajoint/expression.py:301` and `src/datajoint/condition.py:100-120`

### Problems with Current Approach

#### Problem 1: Primary Key Name Collision

Consider two tables:
- `Student(id, name)` - where `id` is the student's primary key
- `Course(id, instructor)` - where `id` is the course's primary key

**Current behavior**: `Student * Course` joins on `id`, producing meaningless results where student IDs are matched with course IDs.

**Desired behavior**: An error should be raised because `Student.id` and `Course.id` have different origins (lineages).

#### Problem 2: Valid Joins Currently Blocked

Consider two tables:
- `FavoriteCourse(student_id*, course_id)` - student's favorite course (course_id is secondary, with FK to Course)
- `DependentCourse(dep_course_id*, course_id)` - course dependencies (course_id is secondary, with FK to Course)

**Current behavior**: `FavoriteCourse * DependentCourse` is **rejected** because `course_id` is a secondary attribute in both tables.

**Desired behavior**: The join should proceed because both `course_id` attributes share the same lineage (tracing to `Course.course_id`).

## Key Concepts

### Terminology

| Term | Definition |
|------|------------|
| **Homologous attributes** | Attributes with the same lineage (whether or not they have the same name) |
| **Namesake attributes** | Attributes with the same name (whether or not they have the same lineage) |
| **Homologous namesakes** | Attributes with the same name AND the same lineage — used for join matching |
| **Non-homologous namesakes** | Attributes with the same name BUT different lineage — cause join errors |

### Attribute Lineage

Lineage identifies the **origin** of an attribute - where it was first defined. It is represented as a string in the format:

```
schema_name.table_name.attribute_name
```

**Note**: `table_name` refers to the actual database table name, not the Python class name. DataJoint converts class names (CamelCase) to table names (snake_case) with tier prefixes:
- `Session` → `session` (manual table)
- `#SessionType` → `#session_type` (lookup table)
- `_ProcessingTask` → `_processing_task` (imported table)
- `__ProcessedData` → `__processed_data` (computed table)

#### Lineage Assignment Rules

1. **Native primary key attributes** have lineage:
   ```
   lineage = "this_schema.this_table.attr_name"
   ```
   The table where they are originally defined.

2. **Attributes inherited via foreign key** retain their origin lineage:
   ```
   lineage = "parent_schema.parent_table.attr_name"
   ```
   Traced to the original definition through the FK chain.

3. **Native secondary attributes** do NOT have lineage:
   ```
   lineage = None
   ```
   Secondary attributes defined directly (not via FK) cannot be used for join matching.

#### Lineage Propagation

Lineage propagates through:

1. **Foreign key references**: Inherited attributes retain their origin lineage regardless of whether they end up as primary or secondary in the referencing table.

2. **Query expressions**:
   - Projections preserve lineage for included attributes
   - Renamed attributes (`new_name='old_name'`) preserve lineage
   - Computed attributes (`result='expr'`) have no lineage

### Semantic Matching Rules

#### Match Decision Matrix

| Scenario | Same Name | Same Lineage | Action |
|----------|-----------|--------------|--------|
| Homologous namesakes | Yes | Yes | **Match** - use for join |
| Non-homologous namesakes (both have lineage) | Yes | No (different) | **Error** |
| Non-homologous namesakes (both null lineage) | Yes | No (both null) | **Error** |
| Non-homologous namesakes (one null) | Yes | No (one null) | **Error** |
| Different names | No | - | **No match** |

#### Error Resolution

When non-homologous namesakes are detected, users must resolve the conflict using `.proj()` to rename one of the colliding attributes:

```python
# Error: Student.id and Course.id have different lineages
Student * Course  # DataJointError!

# Resolution: rename one attribute
Student * Course.proj(course_id='id')  # OK
```

## Affected Operations

Semantic matching applies to all binary operations that match attributes:

| Operator | Operation | Semantic Matching |
|----------|-----------|-------------------|
| `A * B` | Join | Matches on homologous namesakes |
| `A & B` | Restriction | Matches on homologous namesakes |
| `A - B` | Anti-restriction | Matches on homologous namesakes |
| `A.aggr(B, ...)` | Aggregation | Requires functional dependency (see below) |

### The `.join()` Method

The `.join()` method provides additional control:

```python
# Default: semantic checking enabled (same as *)
result = A.join(B)

# Bypass semantic check for legacy compatibility
result = A.join(B, semantic_check=False)
```

### Removal of `@` Operator

The `@` operator (permissive join) is **removed** in DataJoint 2.0:

```python
# Old (deprecated):
A @ B  # Raises DataJointError with migration guidance

# New:
A.join(B, semantic_check=False)  # Explicit bypass
```

The error message directs users to the explicit `.join()` method.

## Universal Set `dj.U`

`dj.U()` or `dj.U('attr1', 'attr2', ...)` represents the universal set of all possible values and lineages.

### Homology with `dj.U`

Since `dj.U` conceptually contains all possible lineages, its attributes are **homologous to any namesake attribute** in other expressions.

### Valid Operations

```python
# Restriction: promotes a, b to PK; lineage transferred from A
dj.U('a', 'b') & A

# Aggregation: groups by a, b
dj.U('a', 'b').aggr(A, count='count(*)')

# Empty U for distinct primary keys
dj.U() & A
```

### Invalid Operations

```python
# Anti-restriction: produces infinite set
dj.U('a', 'b') - A  # DataJointError

# Join: deprecated, use & instead
dj.U('a', 'b') * A  # DataJointError with migration guidance
```

## Implementation Architecture

### Two Methods for Lineage Determination

The implementation provides **two separate methods** for determining attribute lineage:

#### Method 1: Lineage Tables (`~lineage`)

For DataJoint-managed schemas:

- Lineage is stored explicitly in a hidden table (`~lineage`) per schema
- Populated at table declaration time by copying from parent tables
- Only attributes WITH lineage are stored (native secondary attributes have no entry)
- Fast O(1) lookup at query time
- Authoritative source when present

**Schema**:
```sql
CREATE TABLE `schema_name`.`~lineage` (
    table_name VARCHAR(64) NOT NULL,
    attribute_name VARCHAR(64) NOT NULL,
    lineage VARCHAR(255) NOT NULL,
    PRIMARY KEY (table_name, attribute_name)
);
```

**Lifecycle**:
- On table creation: delete any existing entries for that table, then insert new entries
- On table drop: delete all entries for that table

#### Method 2: Dependency Graph Traversal

Fallback for non-DataJoint schemas or when `~lineage` doesn't exist:

- Lineage computed by traversing FK relationships
- Uses `connection.dependencies` which loads from `INFORMATION_SCHEMA`
- Works with any database schema
- May be incomplete if upstream schemas aren't loaded

**Algorithm**:
```python
def compute_lineage(table, attribute):
    """Compute lineage by FK traversal."""
    # Check if attribute is inherited via FK
    for parent, props in dependencies.parents(table).items():
        attr_map = props['attr_map']
        if attribute in attr_map:
            parent_attr = attr_map[attribute]
            # Recursively trace to origin
            return compute_lineage(parent, parent_attr)

    # Not inherited - check if primary key
    if attribute in table.primary_key:
        return f"{schema}.{table}.{attribute}"

    # Native secondary - no lineage
    return None
```

### Selection Logic

These methods are **mutually exclusive**:

```python
def get_lineage(schema, table, attribute):
    try:
        # Returns lineage string if entry exists, None otherwise
        return query_lineage_table(schema, table, attribute)
    except TableDoesNotExist:
        return compute_from_dependencies(schema, table, attribute)
```

## Changes to Existing Code

### `Attribute` Class (`heading.py`)

Add `lineage` field to the `Attribute` namedtuple:

```python
default_attribute_properties = dict(
    # ... existing fields ...
    lineage=None,  # NEW: Origin of attribute, e.g. "schema.table.attr"
)
```

### `Heading` Class (`heading.py`)

1. **Load lineage when fetching heading from database**:
   - Query `~lineage` table if it exists
   - Fall back to dependency graph computation

2. **Preserve lineage in `select()` method**:
   - Included attributes keep their lineage
   - Renamed attributes keep their lineage
   - Computed attributes have `lineage=None`

3. **Merge lineage in `join()` method**:
   - Verify homologous namesakes have matching lineage
   - Combined heading includes lineage from both sides

### `assert_join_compatibility()` (`condition.py`)

Replace current implementation with semantic matching:

```python
def assert_join_compatibility(expr1, expr2):
    """
    Check semantic compatibility of two expressions for joining.

    Raises DataJointError if non-homologous namesakes are detected.
    """
    if isinstance(expr1, U):
        return  # U is always compatible

    # Find namesake attributes (same name in both)
    namesakes = set(expr1.heading.names) & set(expr2.heading.names)

    for name in namesakes:
        lineage1 = expr1.heading[name].lineage
        lineage2 = expr2.heading[name].lineage

        if lineage1 != lineage2:
            raise DataJointError(
                f"Cannot join on attribute `{name}`: "
                f"different lineages ({lineage1} vs {lineage2}). "
                f"Use .proj() to rename one of the attributes or "
                f".join(semantic_check=False) to force a natural join."
            )
```

### `join()` Method (`expression.py`)

Update to use semantic matching:

```python
def join(self, other, semantic_check=True, left=False):
    # ... existing setup ...

    if semantic_check:
        assert_join_compatibility(self, other)

    # Find homologous namesakes for join
    join_attributes = set()
    for name in self.heading.names:
        if name in other.heading.names:
            # Only join on attributes with matching lineage
            if self.heading[name].lineage == other.heading[name].lineage:
                join_attributes.add(name)

    # ... rest of join logic ...
```

### `@` Operator Removal (`expression.py`)

```python
def __matmul__(self, other):
    """Removed: Use .join(other, semantic_check=False) instead."""
    raise DataJointError(
        "The @ operator has been removed in DataJoint 2.0. "
        "Use .join(other, semantic_check=False) for permissive joins."
    )
```

## Lineage Table Population

### At Table Declaration Time

When a table is declared, populate the `~lineage` table:

```python
def declare_table(table_class, context):
    # ... parse definition ...

    # Remove any leftover entries from previous declaration
    delete_lineage_entries(schema, table_name)

    lineage_entries = []

    for attr in definition.attributes:
        if attr.from_foreign_key:
            # Inherited: copy parent's lineage
            parent_lineage = get_lineage(
                attr.fk_schema, attr.fk_table, attr.fk_attribute
            )
            if parent_lineage:  # Only store if parent has lineage
                lineage_entries.append((table_name, attr.name, parent_lineage))
        elif attr.in_key:
            # Native primary key: this table is the origin
            lineage_entries.append((
                table_name, attr.name,
                f"{schema}.{table_name}.{attr.name}"
            ))
        # Native secondary attributes: no entry (no lineage)

    # Insert into ~lineage table
    insert_lineage_entries(schema, lineage_entries)
```

### At Table Drop Time

When a table is dropped, remove its lineage entries:

```python
def drop_table(table_class):
    # ... drop the table ...

    # Clean up lineage entries
    delete_lineage_entries(schema, table_name)
```

### Migration for Existing Tables

For existing schemas without `~lineage` tables:

1. **Automatic creation**: When DataJoint accesses a schema, check if `~lineage` exists
2. **Lazy population**: Populate entries as tables are accessed
3. **Bulk migration tool**: Provide utility to migrate entire schema

```python
def migrate_schema_lineage(schema):
    """Populate ~lineage table for all tables in schema."""
    create_lineage_table(schema)

    for table in schema.list_tables():
        populate_lineage_from_dependencies(schema, table)
```

## Query Expression Lineage Propagation

### Projection (`proj`)

```python
def proj(self, *attributes, **named_attributes):
    # ... existing logic ...

    # Lineage handling in select():
    # - Included attributes: preserve lineage
    # - Renamed (new_name='old_name'): preserve old_name's lineage
    # - Computed (new_name='expr'): lineage = None
```

### Aggregation (`aggr`)

In `A.aggr(B, ...)`, entries from B are grouped by A's primary key and aggregate functions are computed.

**Functional Dependency Requirement**: Every entry in B must match exactly one entry in A. This requires:

1. **B must have all of A's primary key attributes**: If A's primary key is `(a, b)`, then B must contain attributes named `a` and `b`.

2. **Primary key attributes must be homologous**: The namesake attributes in B must have the same lineage as in A. This ensures they represent the same entity.

```python
# Valid: Session.aggr(Trial, ...) where Trial has session_id from Session
Session.aggr(Trial, n='count(*)')  # OK - Trial.session_id traces to Session.session_id

# Invalid: Missing primary key attribute
Session.aggr(Stimulus, n='count(*)')  # Error if Stimulus lacks session_id

# Invalid: Non-homologous primary key
TableA.aggr(TableB, n='count(*)')  # Error if TableB.id has different lineage than TableA.id
```

**Result lineage**:
- Group attributes retain their lineage from the grouping expression (A)
- Aggregated attributes have `lineage=None` (they are computations)

### Union (`+`)

Union requires all namesake attributes to have matching lineage (enforced via `assert_join_compatibility`).

## Error Messages

### Non-Homologous Namesakes

```
DataJointError: Cannot join on attribute `id`: different lineages
(university.student.id vs university.course.id).
Use .proj() to rename one of the attributes or .join(semantic_check=False) to force a natural join.
```

### Deprecated `@` Operator

```
DataJointError: The @ operator has been removed in DataJoint 2.0.
Use .join(other, semantic_check=False) for permissive joins.
```

### Deprecated `dj.U * table`

```
DataJointError: dj.U(...) * table is deprecated in DataJoint 2.0.
Use dj.U(...) & table instead.
```

### Aggregation Missing Primary Key

```
DataJointError: Aggregation requires functional dependency: `group` must have all primary key
attributes of the grouping expression. Missing: {'session_id'}.
Use .proj() to add the missing attributes or verify the schema design.
```

### Aggregation Non-Homologous Primary Key

```
DataJointError: Aggregation requires homologous primary key attributes.
Attribute `id` has different lineages: university.student.id (grouping) vs university.course.id (group).
Use .proj() to rename one of the attributes or .join(semantic_check=False) in a manual aggregation.
```

## Testing Strategy

### Unit Tests

1. **Lineage computation tests**:
   - Native PK attribute has correct lineage
   - FK-inherited attribute traces to origin
   - Native secondary attribute has null lineage
   - Multi-hop FK inheritance traces correctly

2. **Join matching tests**:
   - Homologous namesakes are matched
   - Non-homologous namesakes raise error
   - `semantic_check=False` bypasses check

3. **Projection lineage preservation**:
   - Included attributes keep lineage
   - Renamed attributes keep lineage
   - Computed attributes have null lineage

4. **`dj.U` compatibility**:
   - `dj.U & table` works
   - `dj.U.aggr(table, ...)` works
   - `dj.U - table` raises error
   - `dj.U * table` raises deprecation error

5. **Aggregation functional dependency**:
   - `A.aggr(B)` works when B has all of A's PK attributes with same lineage
   - `A.aggr(B)` raises error when B is missing PK attributes
   - `A.aggr(B)` raises error when PK attributes have different lineage
   - `dj.U('a', 'b').aggr(B)` works when B has `a` and `b` attributes

### Integration Tests

1. **Schema migration**: Existing schema gets `~lineage` table populated correctly
2. **Cross-schema joins**: Lineage traced across schema boundaries
3. **Complex queries**: Multi-join expressions with various lineage scenarios

### Regression Tests

Ensure existing well-designed schemas continue to work without modification.

## Migration Guide

### For Users

1. **Review joins on generic attribute names**: Attributes like `id`, `name`, `value`, `type` may trigger non-homologous namesake errors.

2. **Replace `@` operator**:
   ```python
   # Old
   table1 @ table2

   # New
   table1.join(table2, semantic_check=False)
   ```

3. **Replace `dj.U * table`**:
   ```python
   # Old
   dj.U('attr') * table

   # New
   dj.U('attr') & table
   ```

4. **Resolve namesake conflicts**:
   ```python
   # If error on Student * Course (both have 'id')
   Student * Course.proj(course_id='id')
   ```

### For Schema Designers

1. **Use descriptive attribute names**: Prefer `student_id` over `id` to avoid collisions.

2. **Leverage foreign keys**: Inherited attributes maintain lineage, enabling semantic joins.

3. **Run migration tool**: Use `dj.migrate_lineage(schema)` to populate lineage tables for existing schemas.

## Performance Considerations

### Lineage Table Lookup

- O(1) lookup per attribute
- Cached in `Heading` object
- No additional queries during normal operations

### Dependency Graph Fallback

- First access loads full dependency graph for schema
- Lineage computation is O(depth) per attribute
- Results cached to avoid recomputation

### Join Operations

- Lineage comparison adds negligible overhead
- Same attribute matching loop, just with additional comparison

## Future Considerations

### Lineage in Query Cache

Query cache keys should include lineage information to prevent cache collisions between semantically different queries.

### Lineage Visualization

Extend DataJoint diagrams to show lineage relationships, helping users understand attribute origins.

## Appendix: Lineage Examples

These examples show Python class names with their corresponding database table names (in lineage strings).

### Example 1: Simple FK Chain

```
Session(session_id*, date)           # table: session
    ↓ FK
Trial(session_id*, trial_num*, stimulus)    # table: trial
    ↓ FK
Response(session_id*, trial_num*, response_time)  # table: __response (computed)
```

Lineages (using database table names):
- `Session.session_id` → `university.session.session_id`
- `Trial.session_id` → `university.session.session_id` (inherited)
- `Trial.trial_num` → `university.trial.trial_num` (native PK)
- `Response.session_id` → `university.session.session_id` (inherited)
- `Response.trial_num` → `university.trial.trial_num` (inherited)

### Example 2: Secondary FK

```
Course(course_id*, title)            # table: course
    ↓ FK (secondary)
Enrollment(student_id*, course_id)   # table: enrollment
```

Lineages:
- `Course.course_id` → `university.course.course_id`
- `Enrollment.student_id` → `university.enrollment.student_id` (native PK)
- `Enrollment.course_id` → `university.course.course_id` (inherited via FK)

### Example 3: Aliased FK

```
Person(person_id*, name)             # table: person
    ↓ FK (aliased)
Marriage(husband*, wife*, date)      # table: __marriage (computed)
    where husband -> Person, wife -> Person
```

Lineages:
- `Person.person_id` → `family.person.person_id`
- `Marriage.husband` → `family.person.person_id` (aliased FK)
- `Marriage.wife` → `family.person.person_id` (aliased FK)

Note: `husband` and `wife` have the **same lineage** even though different names.

### Example 4: Non-Homologous Namesakes

```
Student(id*, name)  -- id is native PK, table: student
Course(id*, title)  -- id is native PK, table: course
```

Lineages:
- `Student.id` → `university.student.id`
- `Course.id` → `university.course.id`

`Student * Course` → **Error**: non-homologous namesakes (`id` has different lineages)
