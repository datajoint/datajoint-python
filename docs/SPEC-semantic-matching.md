# Semantic Matching for Joins - Specification

## Overview

This document specifies "semantic matching" for joins in DataJoint 2.0, replacing the current name-based matching rules.

## Problem Statement

### Current Behavior

The current join implementation matches attributes purely by name:

```python
join_attributes = set(n for n in self.heading.names if n in other.heading.names)
```

This is essentially a SQL `NATURAL JOIN` - any attributes with the same name in both tables are used for matching. The only constraint is the "join compatibility" check which prevents joining on secondary attributes that appear in both tables.

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

#### Example: Valid Join Currently Blocked

Consider two tables:
- `FavoriteCourse(student_id*, course_id)` - student's favorite course (course_id is secondary, with lineage to Course)
- `DependentCourse(dep_course_id*, course_id)` - course dependencies (course_id is secondary, with lineage to Course)

With current behavior: `FavoriteCourse * DependentCourse` is **rejected** because `course_id` is a secondary attribute in both tables.

With semantic matching: Both `course_id` attributes share the same lineage (tracing to `Course.course_id`). They are **homologous namesakes**, so the join proceeds.

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
   - `lineage = "schema.table.attribute"` traced to the original definition

2. **Native primary key attributes** have lineage:
   - `lineage = "this_schema.this_table.attr_name"` - the table where they are defined

3. **Native secondary attributes** do NOT have lineage:
   - `lineage = None` for secondary attributes defined directly (not via FK)

Lineage propagates through:
- **Foreign key references**: inherited attributes retain their origin lineage
- **Query expressions**: projections preserve lineage for renamed attributes; computed attributes have no lineage

### Semantic Matching Rules

Semantic matching applies to all binary operations that match attributes:

| Operator | Operation | Semantic Matching |
|----------|-----------|-------------------|
| `A * B` | Join | Matches on homologous namesakes |
| `A & B` | Restriction | Matches on homologous namesakes |
| `A - B` | Anti-restriction | Matches on homologous namesakes |
| `A.aggr(B, ...)` | Aggregation | Matches on homologous namesakes |

**All operators**:
1. Match on **homologous namesakes** (same name AND same lineage)
2. Raise an error on **non-homologous namesakes** (same name, different lineage)

**The `.join()` method** provides additional control:
- Defaults to semantic matching (same as `*`)
- `semantic_check=False` bypasses the non-homologous namesake error

**Non-homologous namesake cases** (all cause errors):
- Both have lineage but different origins
- Both have no lineage (native secondary attrs)
- One has lineage, other doesn't

**Resolution**: Use `.proj()` to rename one of the colliding attributes.

**Removed**: The `@` operator raises an error directing to `.join(semantic_check=False)`.

### Universal Set `dj.U`

`dj.U(attr1, ..., attrn)` represents the universal set of all possible values and lineages.

**Homology**: Since `dj.U` contains all possible lineages, its attributes are **homologous to any namesake attribute**.

**Valid operations**:
- `dj.U('a', 'b') & A` — promotes a, b to PK; lineage transferred from A
- `dj.U('a', 'b').aggr(A, ...)` — aggregates A grouped by a, b

**Invalid operations**:
- `dj.U('a', 'b') - A` — produces infinite set (error)
- `dj.U('a', 'b') * A` — deprecated, use `&` instead

## Implementation

### Architecture

The implementation provides **two separate methods** for determining attribute lineage:

1. **Lineage tables** (`~lineage`): For DataJoint-managed schemas
   - Lineage is stored explicitly in a hidden table per schema
   - Populated at table declaration time by copying from parent tables
   - Fast lookup at query time

2. **Dependency graph**: Fallback for non-DataJoint schemas
   - Lineage computed by traversing FK relationships
   - Uses `connection.dependencies` which loads from INFORMATION_SCHEMA
   - Works with any database, but may be incomplete if upstream schemas aren't loaded

**These methods are mutually exclusive**: if `~lineage` exists, it's used; otherwise the FK graph is traversed.

### Files Modified

| File | Changes |
|------|---------|
| `datajoint/heading.py` | Added `lineage` field to `Attribute`, `get_lineage()` method |
| `datajoint/lineage.py` | `LineageTable` class, lineage computation functions |
| `datajoint/table.py` | Calls `populate_table_lineage()` after table declaration |
| `datajoint/expression.py` | Uses `get_homologous_namesakes()` for semantic join matching |
| `datajoint/condition.py` | `assert_join_compatibility()` checks lineage conflicts |
| `datajoint/schemas.py` | Added `schema.compute_lineage()` method |
| `datajoint/utils.py` | Added `parse_full_table_name()` utility |

### Lineage Table Structure

```sql
CREATE TABLE `~lineage` (
    table_name       VARCHAR(64)  NOT NULL,
    attribute_name   VARCHAR(64)  NOT NULL,
    lineage          VARCHAR(200) NOT NULL,  -- "schema.table.attribute"
    PRIMARY KEY (table_name, attribute_name)
) ENGINE=InnoDB;
```

Only attributes WITH lineage are stored. Absence of an entry means no lineage (native secondary attribute).

### Lineage Population at Declaration Time

When a table is declared (`Table.declare()`), `populate_table_lineage()` is called:

```python
def populate_table_lineage(connection, schema, table_name, heading):
    """
    Populate lineage for a newly declared table.

    - Native PK attributes: lineage = "schema.table.attribute"
    - FK attributes: copy lineage from parent's lineage table
    - Native secondary attributes: no entry (no lineage)
    """
```

For FK attributes, lineage is copied from the parent table's `~lineage` table (which may be in a different schema).

### Semantic Join Matching

The `get_homologous_namesakes()` function in `condition.py`:

```python
def get_homologous_namesakes(expr1, expr2):
    """
    Find attributes that are namesakes (same name) and homologous (same lineage).
    """
    namesakes = set(expr1.heading.names) & set(expr2.heading.names)
    return {
        attr for attr in namesakes
        if expr1.heading.get_lineage(attr) == expr2.heading.get_lineage(attr)
        and expr1.heading.get_lineage(attr) is not None
    }
```

The `assert_join_compatibility()` function raises an error for non-homologous namesakes:

```python
raise DataJointError(
    f"Conflicting lineage in attribute '{attr}'. "
    f"Use .proj() to rename it in one of the operands."
)
```

### Migration Utility

For existing schemas without lineage data:

```python
schema.compute_lineage()  # Populates ~lineage from FK graph
```

This analyzes the dependency graph and populates the `~lineage` table.

## Design Decisions

### D1: Lineage Storage - Hidden Metadata Table with Fallback

**Decision**: Use a hidden `~lineage` table per schema, with fallback to FK graph computation.

- Works for both MySQL and PostgreSQL
- Follows existing pattern for hidden tables (`~log`, `~jobs`)
- Fallback enables use with non-DataJoint databases

### D2: Renamed Attributes Preserve Lineage

**Decision**: Yes. `table.proj(new_name='old_name')` preserves lineage.

### D3: Computed Attributes Have No Lineage

**Decision**: `table.proj(total='price * quantity')` has `lineage = None`.

### D4: `dj.U` Does Not Affect Lineage

**Decision**: `dj.U` only affects PK membership, not lineage.

### D5: Replace Secondary Attribute Heuristic with Lineage Rule

**Decision**: Lineage determines joinability:
- FK-inherited secondary attributes have lineage → can join
- Native secondary attributes have no lineage → always collide

### D6: Remove the `@` Operator

**Decision**: Raises error directing to `.join(semantic_check=False)`.

### D7: Migration via Utility Function

**Decision**: `schema.compute_lineage()` populates `~lineage` from FK graph.

### D8: Primary Key Formation Using Functional Dependencies

**Decision**: For `A * B` joining on J:
- If PK(B) ⊆ J: result PK = PK(A)
- Else if PK(A) ⊆ J: result PK = PK(B)
- Else: result PK = PK(A) ∪ PK(B)
- Tie-breaker: left operand wins

### D9: Aggregation Constraint

**Decision**: For `A.aggr(B, ...)`, every attribute in PK(A) must have a homologous namesake in B.

### D10: Universal Set `dj.U` Semantics

**Decision**: `dj.U` represents all lineages. Deprecate `*` on `dj.U`, use `&`.

## Implementation Status

| Component | Status |
|-----------|--------|
| `lineage` field in Attribute | ✅ Implemented |
| `LineageTable` class | ✅ Implemented |
| Lineage loading into Heading | ✅ Implemented |
| `populate_table_lineage()` at declaration | ✅ Implemented |
| `get_homologous_namesakes()` | ✅ Implemented |
| `assert_join_compatibility()` with lineage | ✅ Implemented |
| `@` operator removal | ✅ Implemented |
| `schema.compute_lineage()` | ✅ Implemented |
| FK graph fallback | ✅ Implemented |
| `parse_full_table_name()` in utils | ✅ Implemented |
| D8: PK formation rules | ⏳ Not yet implemented |
| D9: Aggregation constraint validation | ⏳ Not yet implemented |
| D10: `dj.U` deprecation warnings | ⏳ Not yet implemented |

## Breaking Changes

This is a **semantically breaking change**:
- Joins that previously matched on coincidental name matches will now fail
- Users must explicitly rename colliding attributes with `.proj()`
- The `@` operator raises an error
