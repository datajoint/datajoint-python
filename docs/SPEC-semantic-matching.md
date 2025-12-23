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

#### Example: Name Collision

Consider two tables:
- `Student(student_id, name)` - where `name` is the student's name
- `Course(course_id, name)` - where `name` is the course title

With current behavior: `Student * Course` would attempt to join on `name`, producing meaningless results or an error.

With semantic matching: The `name` attributes have different lineages (one originates in Student, the other in Course), so they would **not** be matched. Instead, the join would be a Cartesian product, or more likely, an error would be raised about incompatible namesake attributes.

## Key Concepts

### Homologous Attributes

Two attributes are **homologous** if they:
1. Have the same name
2. Trace back to the same original attribute definition through foreign key chains

Homologous attributes are also called **semantically matched** attributes.

### Attribute Lineage

Every attribute has a **lineage** - a reference to its original definition. Lineage is propagated through:
- Foreign key references: when table B references table A, the inherited primary key attributes in B have the same lineage as in A
- Query expressions: projections, joins, and other operations preserve lineage

### Join Compatibility Rules

For a join `A * B` to be valid:
1. All namesake attributes (same name in both) must be homologous
2. Homologous attributes must be in the primary key of at least one operand

If namesake attributes exist that are **not** homologous, an error should be raised (collision of non-homologous namesakes).

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

### Phase 1: Add Lineage to Attribute

1. Add `lineage` field to `default_attribute_properties` in `heading.py`
2. Update `Attribute` namedtuple (automatically from `default_attribute_properties`)
3. Add `lineage` parameter to all Attribute creation sites

### Phase 2: Populate Lineage During Table Declaration

1. Modify `compile_foreign_key` in `declare.py` to preserve lineage when copying attributes from referenced tables
2. For non-FK attributes, set lineage to `(current_schema, current_table, attr_name)`
3. Store lineage in heading metadata (potentially in attribute comments or a separate metadata table)

### Phase 3: Propagate Lineage Through Query Operations

Update these methods to preserve lineage:

1. **`Heading.join()`**: Lineage already determined; just verify homology
2. **`Heading.project()`**: Preserve lineage for copied attributes; set new lineage for computed attributes
3. **`Heading.set_primary_key()`**: Preserve existing lineage
4. **`Heading.make_subquery_heading()`**: Preserve existing lineage

### Phase 4: Implement Semantic Join Matching

1. Modify `join()` in `expression.py`:
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

2. Modify `assert_join_compatibility()` in `condition.py`:
   - Check for namesake collisions (same name, different lineage)
   - Check that homologous attributes are in primary key

### Phase 5: Error Handling and Migration

1. Raise clear errors for:
   - Namesake collision (same name, different lineage)
   - Joining on non-primary-key homologous attributes

2. Provide resolution guidance:
   - Use projection to rename colliding attributes
   - Use the permissive join operator `@` to bypass checks

3. Migration path for existing code:
   - Backward compatibility mode?
   - Deprecation warnings?

## Open Questions

### Q1: How to store lineage in the database?

**Options:**
- A. Encode in attribute comments (JSON suffix)
- B. Separate metadata table per schema
- C. Compute from foreign key constraints at runtime

**Recommendation**: Option A is simplest but limits comment space. Option B is cleaner but adds tables. Option C is dynamic but slower.

### Q2: What happens with renamed attributes?

When an attribute is renamed via projection:
```python
table.proj(new_name='old_name')
```

Should the lineage remain the same (pointing to `old_name`'s origin) or become new?

**Recommendation**: Renamed attributes should keep their original lineage. The rename is cosmetic; the semantic identity remains.

### Q3: What about computed attributes?

For computed attributes like:
```python
table.proj(total='price * quantity')
```

**Recommendation**: Computed attributes have no lineage (or a special "computed" lineage). They cannot participate in semantic matching.

### Q4: How does this interact with `dj.U` (universal set)?

The `U` class modifies which attributes are treated as primary key.

**Recommendation**: `U` should not affect lineage - it only affects the primary key membership check, not semantic matching.

### Q5: Backward compatibility?

Should there be a migration path for existing pipelines?

**Options:**
- A. Breaking change - require updates to all pipelines
- B. Deprecation period with warnings
- C. Configuration flag to switch between old/new behavior
- D. Default to permissive join (`@`) semantics when lineage is unknown

**Recommendation**: Option C or D for transition period.

## Testing Strategy

1. **Unit tests** for lineage propagation through all query operations
2. **Integration tests** for join behavior with:
   - Tables with foreign key relationships (should join)
   - Tables with coincidentally same-named attributes (should error)
   - Renamed attributes (should preserve lineage)
   - Computed attributes (should have no lineage)
3. **Backward compatibility tests** for existing pipelines

## Performance Considerations

1. **Memory**: Additional field per attribute (minimal impact)
2. **Comparison**: Lineage comparison is O(1) tuple equality
3. **Storage**: If stored in database, small overhead per attribute

## Summary

Semantic matching is a significant change to DataJoint's join semantics that improves correctness by preventing accidental joins on coincidentally-named attributes. The recommended implementation adds a `lineage` tuple to each `Attribute`, populated during table declaration and preserved through query operations.

Key files to modify:
- `datajoint/heading.py` - Add lineage to Attribute
- `datajoint/declare.py` - Populate lineage during FK processing
- `datajoint/expression.py` - Use lineage in join logic
- `datajoint/condition.py` - Update compatibility checks

This is a breaking change that will require a migration strategy for existing pipelines.
