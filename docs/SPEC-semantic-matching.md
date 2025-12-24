# Semantic Matching for Joins - Specification

## Overview

This document analyzes approaches to implementing "semantic matching" for joins in DataJoint 2.0, replacing the current name-based matching rules.

## Problem Statement

### Current Behavior

The current join implementation (`expression.py:318`) matches attributes purely by name:

```python
join_attributes = set(n for n in self.heading.names if n in other.heading.names)
```

This is essentially a SQL `NATURAL JOIN` - any attributes with the same name in both tables are used for matching. The only constraint is the "join compatibility" check (`condition.py:104-136`) which prevents joining on secondary attributes that appear in both tables.

### Target Behavior: Semantic Matching

DataJoint 2.0 introduces **semantic matching** where two attributes are matched only when they satisfy **both** conditions:

1. **Same name** in both tables
2. **Same lineage** - traced to the same original definition through an uninterrupted chain of foreign keys

This prevents accidental joins on attributes that happen to share the same name but represent different entities.

#### Example: Primary Key Name Collision

Consider two tables:
- `Student(id, name)` - where `id` is the student's primary key
- `Course(id, instructor)` - where `id` is the course's primary key

With current behavior: `Student * Course` joins on `id`, producing meaningless results where student IDs are matched with course IDs.

With semantic matching: `Student.id` and `Course.id` have different lineages (one originates in Student, the other in Course). They are **non-homologous namesakes**, so an error is raised rather than performing an incorrect join.

Note: The current implementation already prevents joining on common *secondary* attributes (e.g., if both had a `name` attribute). The problem semantic matching solves is coincidental name collisions in *primary key* attributes.

#### Example: Valid Join Currently Blocked

Consider two tables:
- `FavoriteCourse(student_id*, course_id)` - student's favorite course (course_id is secondary, with lineage to Course)
- `DependentCourse(dep_course_id*, course_id)` - course dependencies (course_id is secondary, with lineage to Course)

With current behavior: `FavoriteCourse * DependentCourse` is **rejected** because `course_id` is a secondary attribute in both tables.

With semantic matching: Both `course_id` attributes share the same lineage (tracing to `Course.course_id`). They are **homologous namesakes**, so the join proceeds. The result answers: "all courses that are dependent on the student's favorite course."

This shows semantic matching is not just about preventing bad joins—it also **enables valid joins** that the current implementation incorrectly blocks.

## Key Concepts

### Terminology

- **Homologous attributes**: attributes with the same lineage (whether or not they have the same name)
- **Namesake attributes**: attributes with the same name (whether or not they have the same lineage)
- **Homologous namesakes**: attributes with the same name AND the same lineage — used for join matching
- **Non-homologous namesakes**: attributes with the same name BUT different lineage — cause join errors

### Attribute Lineage

Lineage is determined by how an attribute is introduced:

1. **Attributes inherited via foreign key** have lineage:
   - Whether they end up as primary or secondary in the referencing table
   - `lineage = (origin_schema, origin_table, origin_attr)` traced to the original definition
   - Example: `-> Subject` in the dependent section introduces `subject_id` as a secondary attribute WITH lineage

2. **Native primary key attributes** have lineage:
   - `lineage = (this_schema, this_table, attr_name)` - the table where they are defined

3. **Native secondary attributes** do NOT have lineage:
   - `lineage = None` for secondary attributes defined directly (not via FK)
   - These are table-specific data, not entity identifiers

Lineage propagates through:
- **Foreign key references**: inherited attributes retain their origin lineage regardless of PK/secondary status
- **Query expressions**: projections preserve lineage for renamed attributes; computed attributes have no lineage

### Semantic Matching Rules

Semantic matching applies to all binary operations that match attributes between two query expressions:

| Operator | Operation | Semantic Matching |
|----------|-----------|-------------------|
| `A * B` | Join | Matches on homologous namesakes |
| `A & B` | Restriction | Matches on homologous namesakes |
| `A - B` | Anti-restriction | Matches on homologous namesakes |
| `A.aggr(B, ...)` | Aggregation | Matches on homologous namesakes |

Note: `A - B` is the negated form of restriction (equivalent to `A & ~B`), not a true set difference.

**All operators**:
1. Match on **homologous namesakes** (same name AND same lineage)
2. Raise an error on **non-homologous namesakes** (same name, different lineage)

**The `.join()` method** provides additional control via kwargs:
- Defaults to semantic matching (same as `*`)
- `semantic_check=False` bypasses the non-homologous namesake error

**Non-homologous namesake cases**:
- Both have lineage but different origins → error
- Both have no lineage (native secondary attrs) → error
- One has lineage, other doesn't → error

**Resolution**: Use `.proj()` to rename one of the colliding attributes.

**Deprecated**: The `@` operator is deprecated. Use `.join(semantic_check=False)` instead.

**Note**: A warning may be raised for joins on unindexed attributes (performance consideration).

### Primary Key Formation in Joins

The primary key of `A * B` is determined by functional dependency analysis, not simple union.

Let J = join attributes (homologous namesakes matched during the join).

**Rule**:
```
PK(A * B) =
  PK(A)           if PK(B) ⊆ J    -- B's entire PK is in the join
  PK(B)           if PK(A) ⊆ J    -- A's entire PK is in the join
  PK(A) ∪ PK(B)   otherwise       -- neither PK is fully covered
```

**When both conditions hold** (both PKs are subsets of J), use **PK(A)** — the left operand's primary key. This makes join non-commutative with respect to primary key formation.

**Rationale** (Armstrong's axioms):
- If PK(B) ⊆ J, then by reflexivity: J → PK(B)
- From A: PK(A) → J (A determines its attributes including join attrs)
- By transitivity: PK(A) → J → PK(B) → all of B
- Therefore PK(A) alone determines all attributes in the result

**Examples**:

1. **B's PK covered by join**:
   - A: PK = {session_id}, secondary {subject_id}
   - B: PK = {subject_id}
   - J = {subject_id}, PK(B) ⊆ J ✓
   - Result PK = {session_id}

2. **Neither PK covered**:
   - A: PK = {a, b}
   - B: PK = {b, c}
   - J = {b}
   - PK(A) ⊄ J, PK(B) ⊄ J
   - Result PK = {a, b, c}

3. **Both PKs covered** (non-commutative case):
   - A: PK = {a}, secondary {b}
   - B: PK = {b}, secondary {a}
   - J = {a, b}
   - Both PK(A) ⊆ J and PK(B) ⊆ J
   - Result PK = PK(A) = {a} (left operand wins)

### Aggregation Rules

For `A.aggr(B, ...)`, semantic matching applies with an additional constraint.

**Primary key**: PK(result) = PK(A) — always the left operand's primary key.

**Constraint**: Every attribute in PK(A) must have a homologous namesake in B.

This ensures that each B tuple belongs to exactly one A entity, so aggregation groups are non-overlapping. If B is missing any part of A's primary key, a B tuple could match multiple A tuples and be counted in multiple aggregates.

**Keep all rows**: The same constraint applies when using `A.aggr(B, ..., keep_all_rows=True)`. This keeps all A tuples in the result even if they have no matching B tuples (with NULL aggregates), but the grouping constraint remains: B must contain A's complete primary key.

**Example**:
```python
# Valid: Session.aggr(Spike, count="count(*)")
# Session PK = {subject_id, session_id}
# Spike has {subject_id, session_id, spike_id, ...}
# Spike contains Session's entire PK ✓

# Invalid: Subject.aggr(Session, count="count(*)")
# Subject PK = {subject_id}
# Session has {subject_id, session_id, ...}
# Session contains Subject's PK ✓ — this is actually valid!

# Invalid: Session.aggr(Subject, ...)
# Session PK = {subject_id, session_id}
# Subject has {subject_id, ...}
# Subject is missing session_id from Session's PK ✗
```

## Current Implementation Analysis

### Attribute Representation (`heading.py:48`)

```python
class Attribute(namedtuple("_Attribute", default_attribute_properties)):
```

Current properties:
- `name`, `type`, `in_key`, `nullable`, `default`, `comment`
- `database`, `attribute_expression`
- Various type-specific flags

**Missing**: No lineage/origin tracking.

### Join Logic (`expression.py:302-350`)

```python
def join(self, other, semantic_check=True, left=False):
    # ...
    join_attributes = set(n for n in self.heading.names if n in other.heading.names)
```

The current logic:
1. Finds common attribute names
2. Checks join compatibility (no common secondary attributes)
3. Creates subqueries if needed for derived attributes
4. Combines headings and restrictions

### Heading Join (`heading.py:482-504`)

Combines two headings by unioning primary keys and merging secondary attributes.

### Foreign Key Processing (`declare.py:154-225`)

When processing `-> TableRef`:
- Copies primary key attributes from referenced table
- Creates SQL FOREIGN KEY constraint
- No lineage information is preserved

## Implementation Approaches

### Approach 1: Add Lineage to Attribute Namedtuple

**Add a `lineage` field to `Attribute`** that identifies the origin of each attribute.

#### Lineage Representation Options

**Option 1A: Tuple-based lineage**
```python
# (schema_name, table_name, attribute_name)
lineage = ("lab", "Subject", "subject_id")
```

**Option 1B: String-based lineage**
```python
lineage = "lab.Subject.subject_id"
```

**Option 1C: Hash-based lineage**
```python
# SHA256 hash of canonical identifier
lineage = "a3f2c1..."
```

#### Pros
- Clean, self-contained representation
- Easy to compare (simple equality check)
- Serializable for debugging

#### Cons
- Requires modifying the core `Attribute` type
- All code that creates Attributes must be updated
- Migration complexity for existing code

### Approach 2: Separate Lineage Registry

**Maintain a separate mapping from attribute names to lineage** in the Heading class.

```python
class Heading:
    def __init__(self):
        self.attributes = {}  # name -> Attribute
        self.lineage = {}     # name -> origin_identifier
```

#### Pros
- Less invasive change to Attribute namedtuple
- Can be added incrementally

#### Cons
- Two data structures to keep in sync
- Potential for inconsistency

### Approach 3: Graph-Based Lineage Tracking

**Build a schema graph** that tracks foreign key relationships, then compute lineage dynamically.

```python
class SchemaGraph:
    def __init__(self):
        self.edges = []  # [(from_table, to_table, attrs)]

    def get_lineage(self, table, attribute):
        # Traverse graph to find original definition
        pass
```

#### Pros
- Single source of truth (the actual schema)
- Dynamic computation avoids stale data

#### Cons
- Higher runtime cost for lineage queries
- More complex implementation
- Requires access to full schema during queries

## Recommended Approach

**Approach 1A (Tuple-based lineage in Attribute)** is recommended because:

1. **Simplicity**: Direct storage of lineage avoids graph traversal at query time
2. **Immutability**: Once an attribute's lineage is set, it doesn't change
3. **Explicit**: Makes lineage a first-class concept in the data model
4. **Debuggability**: Easy to inspect and understand

## Implementation Plan

### Phase 1: Add Lineage Infrastructure

1. **Add `lineage` field to `Attribute`** (`heading.py`)
   - `lineage`: string `"schema.table.attribute"` or `None`
   - Add to `default_attribute_properties` with default `None`

   **Comparison strategy**:
   - Direct string comparison (simple equality check)
   - Lineage strings are short (~50-100 chars) and comparisons short-circuit on first difference
   - `None` lineage never matches anything (including other `None`)

2. **Create `~lineage` table management** (new file: `datajoint/lineage.py`)
   - `LineageTable` class (similar to `ExternalTable`)
   - Methods: `declare()`, `insert()`, `lookup()`, `compute_from_fk()`
   - Database-agnostic SQL generation for MySQL and PostgreSQL

3. **Integrate with Schema** (`schemas.py`)
   - Create `~lineage` table when schema is activated
   - Provide `schema.migrate_lineage()` method for existing schemas

### Phase 2: Populate Lineage During Table Declaration

1. **Modify `compile_foreign_key`** (`declare.py`)
   - When copying attributes from referenced table, record their lineage
   - Return lineage info along with attribute SQL

2. **Modify `declare`** (`declare.py`)
   - For native attributes: lineage = `(current_schema, current_table, attr_name)`
   - For FK attributes: lineage = inherited from referenced table
   - Insert lineage records into `~lineage` table after table creation

3. **Load lineage into Heading** (`heading.py`)
   - When `Heading` is initialized from `table_info`, query `~lineage`
   - Store lineage in each `Attribute` instance

### Phase 3: Propagate Lineage Through Query Operations

Update these methods to preserve lineage:

1. **`Heading.join()`**: Lineage already determined; just verify homology
2. **`Heading.project()`**: Preserve lineage for copied attributes; set new lineage for computed attributes
3. **`Heading.set_primary_key()`**: Preserve existing lineage
4. **`Heading.make_subquery_heading()`**: Preserve existing lineage

### Phase 4: Implement Semantic Join Matching

1. **Ensure dependencies are loaded** before join operations:
   ```python
   def join(self, other, semantic_check=True, left=False):
       # Dependencies must be loaded for lineage computation
       self.connection.dependencies.load(force=False)
       # ...
   ```

2. **Modify `join()` in `expression.py`**:
   ```python
   def join(self, other, semantic_check=True, left=False):
       if semantic_check:
           self._check_semantic_compatibility(other)

       # Find homologous attributes (same name AND same lineage)
       join_attributes = set(
           n for n in self.heading.names
           if n in other.heading.names
           and self.heading.get_lineage(n) == other.heading.get_lineage(n)
       )
       # ...
   ```

3. **Modify `assert_join_compatibility()` in `condition.py`**:
   - **Remove** the secondary attribute restriction (deprecated)
   - Check for namesake collisions (same name, different lineage)
   - Optionally warn about unindexed join attributes

### Phase 5: Error Handling

1. **Clear error messages** for:
   - PK lineage mismatch: `"Cannot join: attribute 'subject_id' exists in both operands with different lineages (lab.Subject.subject_id vs other.Experiment.subject_id). Use .proj() to rename one."`
   - Secondary attr collision: `"Cannot join: attribute 'value' has no lineage in both operands (secondary attributes). Use .proj() to rename one."`

2. **Resolution guidance** in error messages:
   - Suggest specific projection syntax to resolve
   - Mention `.join(semantic_check=False)` as escape hatch for advanced users

### Phase 6: Migration Utility

1. **`dj.migrate_lineage(schema)`** function
   - Analyzes existing FK constraints via `INFORMATION_SCHEMA` (MySQL) or `pg_catalog` (PostgreSQL)
   - Computes lineage for each attribute using recursive FK traversal
   - Populates `~lineage` table

2. **Automatic migration on schema activation** (optional)
   - If `~lineage` table is empty but tables exist, offer to run migration
   - Configuration flag: `datajoint.config['auto_migrate_lineage'] = True/False`

3. **CLI command**
   ```bash
   python -m datajoint migrate-lineage myschema
   ```

## Design Decisions

### D1: Lineage Storage - Hidden Metadata Table with Fallback

**Decision**: Use a hidden metadata table (`~lineage`) per schema, with **in-memory fallback** when table doesn't exist.

This approach:
- Works consistently for both **MySQL** and **PostgreSQL**
- Provides explicit, queryable lineage data
- Follows the existing pattern for hidden tables (e.g., `~external_*`, `~log`)
- Easier to migrate existing schemas
- **Works with databases not created by DataJoint** via fallback computation

#### Fallback: Compute Lineage from Dependencies

When the `~lineage` table does not exist (e.g., external databases, legacy schemas), lineage is computed **in-memory** from the FK graph using the existing `Dependencies` class:

```python
def compute_lineage_from_dependencies(connection, schema, table_name, attribute_name):
    """
    Compute lineage by traversing the FK graph.
    Uses connection.dependencies which already loads FK info from INFORMATION_SCHEMA.

    Returns lineage string "schema.table.attribute" or None for native secondary attrs.
    """
    connection.dependencies.load(force=False)  # ensure dependencies are loaded

    full_table_name = f"`{schema}`.`{table_name}`"

    # Check incoming edges (foreign keys TO this table)
    for parent, props in connection.dependencies.parents(full_table_name).items():
        attr_map = props.get('attr_map', {})
        if attribute_name in attr_map:
            # This attribute is inherited from parent - recurse to find origin
            parent_attr = attr_map[attribute_name]
            parent_schema, parent_table = parse_full_table_name(parent)
            return compute_lineage_from_dependencies(
                connection, parent_schema, parent_table, parent_attr
            )

    # Not inherited - origin is this table (for PK attrs) or None (for native secondary)
    if is_primary_key(connection, schema, table_name, attribute_name):
        return f"{schema}.{table_name}.{attribute_name}"
    else:
        return None  # native secondary attribute
```

#### Integration with Dependencies Loading

**Dependencies must be loaded before joins.** This is already the pattern for operations like `delete`, `drop`, and `populate`. The join operation will:

1. Ensure `connection.dependencies.load(force=False)` is called
2. Check if `~lineage` table exists for involved schemas
3. If exists: read lineage from table (fast)
4. If not exists: compute lineage from FK graph (slower but works for any database)

#### Table Structure

```sql
CREATE TABLE `~lineage` (
    table_name       VARCHAR(64)  NOT NULL,
    attribute_name   VARCHAR(64)  NOT NULL,
    lineage          VARCHAR(200) NOT NULL,  -- "schema.table.attribute"
    PRIMARY KEY (table_name, attribute_name)
) ENGINE=InnoDB;
```

For PostgreSQL:
```sql
CREATE TABLE "~lineage" (
    table_name       VARCHAR(64)  NOT NULL,
    attribute_name   VARCHAR(64)  NOT NULL,
    lineage          VARCHAR(200) NOT NULL,  -- "schema.table.attribute"
    PRIMARY KEY (table_name, attribute_name)
);
```

#### Lineage Lookup

When a `Heading` is initialized from a table, query the `~lineage` table:

```python
def _load_lineage(self, connection, database, table_name):
    """Load lineage information from the ~lineage metadata table."""
    query = """
        SELECT attribute_name, lineage
        FROM `{database}`.`~lineage`
        WHERE table_name = %s
    """.format(database=database)
    # ... populate self.lineage dict
```

### D2: Renamed Attributes Preserve Lineage

**Decision**: Yes, renamed attributes preserve their original lineage.

When an attribute is renamed via projection:
```python
table.proj(new_name='old_name')
```

The `new_name` attribute retains the lineage of `old_name`. The rename is cosmetic; the semantic identity (what entity the attribute represents) remains unchanged.

This enables:
```python
# These two expressions can still join on the underlying subject_id
A.proj(subj='subject_id') * B.proj(subj='subject_id')  # OK - same lineage
```

### D3: Computed Attributes Have No Lineage

**Decision**: Lineage breaks for computed attributes.

For computed attributes like:
```python
table.proj(total='price * quantity')
```

The `total` attribute has `lineage = None`. Computed attributes:
- Cannot participate in semantic matching
- Will cause a namesake collision error if another table has an attribute named `total`
- Must be renamed via projection to avoid collisions

This is intentional: a computed value is a new entity, not inherited from any source table.

### D4: `dj.U` Does Not Affect Lineage

**Decision**: The universal set `U` only affects primary key membership, not lineage.

`dj.U` promotes attributes to the primary key for grouping/aggregation purposes, but the semantic identity of the attributes remains unchanged.

### D5: Replace Secondary Attribute Heuristic with Lineage Rule

**Decision**: Replace the current heuristic with a principled lineage-based rule.

**Current behavior** (`condition.py:assert_join_compatibility`):
```python
# Raises error if both expressions have the same secondary attribute
raise DataJointError(
    "Cannot join query expressions on dependent attribute `%s`" % attr
)
```

**New behavior**: Lineage determines joinability:
- Attributes with matching lineage can participate in joins (even if secondary)
- Attributes with `lineage = None` (native secondary) always collide with namesakes
- The key distinction is HOW the attribute was introduced, not WHERE it ends up

**Key insight**: Secondary attributes introduced via foreign key DO have lineage and CAN participate in joins. Only native secondary attributes (defined directly in the table, not via FK) have no lineage.

**Example**:
```python
# Table A: -> Subject in dependent section gives secondary `subject_id` WITH lineage
# Table B: -> Subject in dependent section gives secondary `subject_id` WITH lineage
# A * B works! Both subject_id attributes trace to Subject.subject_id

# Table C: has native secondary `value` (no lineage)
# Table D: has native secondary `value` (no lineage)
# C * D fails - collision, must rename one
```

**Error message change**:
```python
# Old: "Cannot join query expressions on dependent attribute `value`"
# New: "Cannot join: attribute 'value' has no lineage in both operands. Use .proj() to rename one."
```

**Performance warning**: Consider warning when joining on attributes that lack indexes:
```python
if not has_index(table1, attr) or not has_index(table2, attr):
    warnings.warn(
        f"Join on '{attr}' may be slow: attribute is not indexed in both tables",
        PerformanceWarning
    )
```

### D6: Deprecate the `@` Operator

**Decision**: Deprecate the `@` (permissive join) operator.

**Rationale**:
- Having two join operators (`*` and `@`) with subtle differences adds confusion
- The `.join(semantic_check=False)` method provides the same functionality
- Reduces documentation burden and cognitive load
- Simplifies the API surface

**Migration**:
```python
# Old (deprecated)
A @ B

# New
A.join(B, semantic_check=False)
```

The `@` operator will emit a deprecation warning and eventually be removed.

### D7: Migration via Utility Function

**Decision**: Provide a migration utility that computes the `~lineage` table from existing schema.

For existing schemas without lineage metadata, a utility will:
1. Analyze the foreign key graph using `INFORMATION_SCHEMA`
2. Trace each attribute to its origin table
3. Populate the `~lineage` table

```python
def migrate_schema_lineage(schema):
    """
    Compute and populate the ~lineage table for an existing schema.

    Analyzes foreign key relationships to determine attribute origins.
    """
    # 1. Create ~lineage table if not exists
    # 2. For each table in schema:
    #    a. For each attribute:
    #       - If attribute is inherited via FK, trace to origin
    #       - If attribute is native, origin is this table
    #    b. Insert into ~lineage
```

#### Algorithm for Computing Lineage

```python
def compute_attribute_lineage(schema, table, attribute, is_pk):
    """
    Trace an attribute to its original definition.

    Returns lineage string "schema.table.attribute" or None for native secondary.
    """
    # Check if this attribute is part of a foreign key
    fk_info = get_foreign_key_for_attribute(schema, table, attribute)

    if fk_info is None:
        # Native attribute
        if is_pk:
            return f"{schema}.{table}.{attribute}"  # PK has lineage
        else:
            return None  # native secondary has no lineage

    # Inherited via FK - recurse to referenced table
    ref_schema, ref_table, ref_attribute = fk_info
    return compute_attribute_lineage(ref_schema, ref_table, ref_attribute, is_pk=True)
```

#### MySQL Query for FK Analysis

```sql
SELECT
    kcu.COLUMN_NAME as attribute_name,
    kcu.REFERENCED_TABLE_SCHEMA as ref_schema,
    kcu.REFERENCED_TABLE_NAME as ref_table,
    kcu.REFERENCED_COLUMN_NAME as ref_attribute
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
WHERE kcu.TABLE_SCHEMA = %s
  AND kcu.TABLE_NAME = %s
  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
```

#### PostgreSQL Query for FK Analysis

```sql
SELECT
    a.attname as attribute_name,
    cl2.relnamespace::regnamespace::text as ref_schema,
    cl2.relname as ref_table,
    a2.attname as ref_attribute
FROM pg_constraint c
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
JOIN pg_class cl2 ON cl2.oid = c.confrelid
JOIN pg_attribute a2 ON a2.attrelid = c.confrelid AND a2.attnum = ANY(c.confkey)
WHERE c.contype = 'f'
  AND c.conrelid = %s::regclass
```

### D8: Primary Key Formation Using Functional Dependencies

**Decision**: Use functional dependency analysis to form minimal primary keys in joins.

**Rule**: For `A * B` joining on attributes J:
- If PK(B) ⊆ J: result PK = PK(A)
- Else if PK(A) ⊆ J: result PK = PK(B)
- Else: result PK = PK(A) ∪ PK(B)

**Tie-breaker**: When both PK(A) ⊆ J and PK(B) ⊆ J, use PK(A) (left operand). This makes join **non-commutative** with respect to primary key formation.

**Rationale**: Based on Armstrong's axioms. If PK(B) ⊆ J, then PK(A) → J → PK(B) by transitivity, so PK(A) alone determines all result attributes. The union rule is only needed when neither PK is fully covered by the join.

### D9: Aggregation Constraint

**Decision**: For `A.aggr(B, ...)`, require that every attribute in PK(A) has a homologous namesake in B.

**Primary key**: PK(result) = PK(A) — always.

**Rationale**: This ensures non-overlapping aggregation groups. Each B tuple belongs to exactly one A entity, preventing double-counting.

**Keep all rows**: The same constraint applies for `A.aggr(B, ..., keep_all_rows=True)`. A tuples with no matching B tuples appear with NULL aggregates, but the grouping constraint remains.

## Testing Strategy

1. **Unit tests** for lineage propagation through all query operations
2. **Integration tests** for join behavior with:
   - Tables with foreign key relationships (should join)
   - Tables with coincidentally same-named attributes (should error)
   - Renamed attributes (should preserve lineage)
   - Computed attributes (should have no lineage)
3. **Backward compatibility tests** for existing pipelines

## Performance Considerations

1. **Memory**: One additional field per attribute
   - `lineage`: string `"schema.table.attribute"` (~50-100 bytes typical) or `None`

2. **Comparison**: Direct string comparison
   - Short strings (~50-100 chars) with early short-circuit on difference
   - Only compared for namesake attributes (same name in both tables)
   - `None` lineage never matches anything

3. **Storage**: Small overhead in `~lineage` table
   - ~130 bytes per attribute (table_name + attribute_name + lineage string)
   - Indexed by (table_name, attribute_name) for fast lookup

4. **Dependency loading**: Required before joins
   - Already cached per connection (`connection.dependencies`)
   - Reused across multiple join operations
   - Fallback lineage computation adds ~1 query per table (when `~lineage` missing)

## Summary

Semantic matching is a significant change to DataJoint's join semantics that improves correctness by preventing accidental joins on coincidentally-named attributes.

### Key Design Decisions

| Decision | Choice |
|----------|--------|
| **D1**: Lineage storage | Hidden `~lineage` table + in-memory fallback from FK graph |
| **D2**: Renamed attributes | Preserve original lineage |
| **D3**: Computed attributes | Lineage = `None` (breaks matching) |
| **D4**: `dj.U` interaction | Does not affect lineage |
| **D5**: Secondary attr restriction | Replaced by lineage rule - FK-inherited attrs have lineage, native secondary don't |
| **D6**: `@` operator | Deprecated - use `.join(semantic_check=False)` |
| **D7**: Migration | Utility function + automatic fallback computation |
| **D8**: PK formation | Functional dependency analysis; left operand wins ties; non-commutative |
| **D9**: Aggregation | B must contain A's entire PK; result PK = PK(A); applies to `keep_all_rows=True` too |

### Compatibility

- **MySQL**: Fully supported (INFORMATION_SCHEMA for FK analysis)
- **PostgreSQL**: Fully supported (pg_constraint/pg_attribute for FK analysis)
- **External databases**: Works via in-memory lineage computation from FK graph
- **Legacy DataJoint schemas**: Works via migration utility or automatic fallback

### Files to Create

| File | Purpose |
|------|---------|
| `datajoint/lineage.py` | `LineageTable` class, migration utilities |

### Files to Modify

| File | Changes |
|------|---------|
| `datajoint/heading.py` | Add `lineage` field to `Attribute`, load from `~lineage` |
| `datajoint/declare.py` | Record lineage during table declaration |
| `datajoint/expression.py` | Use lineage equality in join matching |
| `datajoint/condition.py` | Update compatibility checks for lineage collisions |
| `datajoint/schemas.py` | Create `~lineage` table on schema activation |

### Breaking Changes

This is a **semantically breaking change**:
- Joins that previously matched on coincidental name matches will now fail
- Users must explicitly rename colliding attributes with `.proj()`
- Migration utility provides a path for existing schemas

### Next Steps

1. Review and approve this specification
2. Implement Phase 1 (infrastructure) with tests
3. Implement Phase 2 (population) with tests
4. Implement Phase 3-4 (query propagation and join logic)
5. Implement Phase 5-6 (error handling and migration)
6. Update documentation
7. Release with deprecation warnings, then enforce in subsequent release
