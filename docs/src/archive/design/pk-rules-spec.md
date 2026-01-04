# Primary Key Rules in Relational Operators

In DataJoint, the result of each query operator produces a valid **entity set** with a well-defined **entity type** and **primary key**. This section specifies how the primary key is determined for each relational operator.

## General Principle

The primary key of a query result identifies unique entities in that result. For most operators, the primary key is preserved from the left operand. For joins, the primary key depends on the functional dependencies between the operands.

## Integration with Semantic Matching

Primary key determination is applied **after** semantic compatibility is verified. The evaluation order is:

1. **Semantic Check**: `assert_join_compatibility()` ensures all namesakes are homologous (same lineage)
2. **PK Determination**: The "determines" relationship is computed using attribute names
3. **Left Join Validation**: If `left=True`, verify A → B

This ordering is important because:
- After semantic matching passes, namesakes represent semantically equivalent attributes
- The name-based "determines" check is therefore semantically valid
- Attribute names in the context of a semantically-valid join represent the same entity

The "determines" relationship uses attribute **names** (not lineages directly) because:
- Lineage ensures namesakes are homologous
- Once verified, checking by name is equivalent to checking by semantic identity
- Aliased attributes (same lineage, different names) don't participate in natural joins anyway

## Notation

In the examples below, `*` marks primary key attributes:
- `A(x*, y*, z)` means A has primary key `{x, y}` and secondary attribute `z`
- `A → B` means "A determines B" (defined below)

### Rules by Operator

| Operator | Primary Key Rule |
|----------|------------------|
| `A & B` (restriction) | PK(A) — preserved from left operand |
| `A - B` (anti-restriction) | PK(A) — preserved from left operand |
| `A.proj(...)` (projection) | PK(A) — preserved from left operand |
| `A.aggr(B, ...)` (aggregation) | PK(A) — preserved from left operand |
| `A.extend(B)` (extension) | PK(A) — requires A → B |
| `A * B` (join) | Depends on functional dependencies (see below) |

### Join Primary Key Rule

The join operator requires special handling because it combines two entity sets. The primary key of `A * B` depends on the **functional dependency relationship** between the operands.

#### Definitions

**A determines B** (written `A → B`): Every attribute in PK(B) is in A.

```
A → B  iff  ∀b ∈ PK(B): b ∈ A
```

Since `PK(A) ∪ secondary(A) = all attributes in A`, this is equivalent to saying every attribute in B's primary key exists somewhere in A (as either a primary key or secondary attribute).

Intuitively, `A → B` means that knowing A's primary key is sufficient to determine B's primary key through the functional dependencies implied by A's structure.

**B determines A** (written `B → A`): Every attribute in PK(A) is in B.

```
B → A  iff  ∀a ∈ PK(A): a ∈ B
```

#### Join Primary Key Algorithm

For `A * B`:

| Condition | PK(A * B) | Attribute Order |
|-----------|-----------|-----------------|
| A → B | PK(A) | A's attributes first |
| B → A (and not A → B) | PK(B) | B's attributes first |
| Neither | PK(A) ∪ PK(B) | PK(A) first, then PK(B) − PK(A) |

When both `A → B` and `B → A` hold, the left operand takes precedence (use PK(A)).

#### Examples

**Example 1: B → A**
```
A: x*, y*
B: x*, z*, y    (y is secondary in B, so z → y)
```
- A → B? PK(B) = {x, z}. Is z in PK(A) or secondary in A? No (z not in A). **No.**
- B → A? PK(A) = {x, y}. Is y in PK(B) or secondary in B? Yes (secondary). **Yes.**
- Result: **PK(A * B) = {x, z}** with B's attributes first.

**Example 2: Both directions (bijection-like)**
```
A: x*, y*, z    (z is secondary in A)
B: y*, z*, x    (x is secondary in B)
```
- A → B? PK(B) = {y, z}. Is z in PK(A) or secondary in A? Yes (secondary). **Yes.**
- B → A? PK(A) = {x, y}. Is x in PK(B) or secondary in B? Yes (secondary). **Yes.**
- Both hold, prefer left operand: **PK(A * B) = {x, y}** with A's attributes first.

**Example 3: Neither direction**
```
A: x*, y*
B: z*, x    (x is secondary in B)
```
- A → B? PK(B) = {z}. Is z in PK(A) or secondary in A? No. **No.**
- B → A? PK(A) = {x, y}. Is y in PK(B) or secondary in B? No (y not in B). **No.**
- Result: **PK(A * B) = {x, y, z}** (union) with A's attributes first.

**Example 4: A → B (subordinate relationship)**
```
Session: session_id*
Trial: session_id*, trial_num*    (references Session)
```
- A → B? PK(Trial) = {session_id, trial_num}. Is trial_num in PK(Session) or secondary? No. **No.**
- B → A? PK(Session) = {session_id}. Is session_id in PK(Trial)? Yes. **Yes.**
- Result: **PK(Session * Trial) = {session_id, trial_num}** with Trial's attributes first.

**Join primary key determination**:
   - `A * B` where `A → B`: result has PK(A)
   - `A * B` where `B → A` (not `A → B`): result has PK(B), B's attributes first
   - `A * B` where both `A → B` and `B → A`: result has PK(A) (left preference)
   - `A * B` where neither direction: result has PK(A) ∪ PK(B)
   - Verify attribute ordering matches primary key source
   - Verify non-commutativity: `A * B` vs `B * A` may differ in PK and order

### Design Tradeoff: Predictability vs. Minimality

The join primary key rule prioritizes **predictability** over **minimality**. In some cases, the resulting primary key may not be minimal (i.e., it may contain functionally redundant attributes).

**Example of non-minimal result:**
```
A: x*, y*
B: z*, x    (x is secondary in B, so z → x)
```

The mathematically minimal primary key for `A * B` would be `{y, z}` because:
- `z → x` (from B's structure)
- `{y, z} → {x, y, z}` (z gives us x, and we have y)

However, `{y, z}` is problematic:
- It is **not the primary key of either operand** (A has `{x, y}`, B has `{z}`)
- It is **not the union** of the primary keys
- It represents a **novel entity type** that doesn't correspond to A, B, or their natural pairing

This creates confusion: what kind of entity does `{y, z}` identify?

**The simplified rule produces `{x, y, z}`** (the union), which:
- Is immediately recognizable as "one A entity paired with one B entity"
- Contains A's full primary key and B's full primary key
- May have redundancy (`x` is determined by `z`) but is semantically clear

**Rationale:** Users can always project away redundant attributes if they need the minimal key. But starting with a predictable, interpretable primary key reduces confusion and errors.

### Attribute Ordering

The primary key attributes always appear **first** in the result's attribute list, followed by secondary attributes. When `B → A` (and not `A → B`), the join is conceptually reordered as `B * A` to maintain this invariant:

- If PK = PK(A): A's attributes appear first
- If PK = PK(B): B's attributes appear first
- If PK = PK(A) ∪ PK(B): PK(A) attributes first, then PK(B) − PK(A), then secondaries

### Non-Commutativity

With these rules, join is **not commutative** in terms of:
1. **Primary key selection**: `A * B` may have a different PK than `B * A` when one direction determines but not the other
2. **Attribute ordering**: The left operand's attributes appear first (unless B → A)

The **result set** (the actual rows returned) remains the same regardless of order, but the **schema** (primary key and attribute order) may differ.

### Left Join Constraint

For left joins (`A.join(B, left=True)`), the functional dependency **A → B is required**.

**Why this constraint exists:**

In a left join, all rows from A are retained even if there's no matching row in B. For unmatched rows, B's attributes are NULL. This creates a problem for primary key validity:

| Scenario | PK by inner join rule | Left join problem |
|----------|----------------------|-------------------|
| A → B | PK(A) | ✅ Safe — A's attrs always present |
| B → A | PK(B) | ❌ B's PK attrs could be NULL |
| Neither | PK(A) ∪ PK(B) | ❌ B's PK attrs could be NULL |

**Example of invalid left join:**
```
A: x*, y*           PK(A) = {x, y}
B: x*, z*, y        PK(B) = {x, z}, y is secondary

Inner join: PK = {x, z} (B → A rule)
Left join attempt: FAILS because z could be NULL for unmatched A rows
```

**Valid left join example:**
```
Session: session_id*, date
Trial: session_id*, trial_num*, stimulus    (references Session)

Session.join(Trial, left=True)  # OK: Session → Trial
# PK = {session_id}, all sessions retained even without trials
```

**Error message:**
```
DataJointError: Left join requires the left operand to determine the right operand (A → B).
The following attributes from the right operand's primary key are not determined by
the left operand: ['z']. Use an inner join or restructure the query.
```

### Conceptual Note: Left Join as Extension

When `A → B`, the left join `A.join(B, left=True)` is conceptually distinct from the general join operator `A * B`. It is better understood as an **extension** operation rather than a join:

| Aspect | General Join (A * B) | Left Join when A → B |
|--------|---------------------|----------------------|
| Conceptual model | Cartesian product restricted to matching rows | Extend A with attributes from B |
| Row count | May increase, decrease, or stay same | Always equals len(A) |
| Primary key | Depends on functional dependencies | Always PK(A) |
| Relation to projection | Different operation | Variation of projection |

**The extension perspective:**

The operation `A.join(B, left=True)` when `A → B` is closer to **projection** than to **join**:
- It adds new attributes to A (like `A.proj(..., new_attr=...)`)
- It preserves all rows of A
- It preserves A's primary key
- It lacks the Cartesian product aspect that defines joins

DataJoint provides an explicit `extend()` method for this pattern:

```python
# These are equivalent when A → B:
A.join(B, left=True)
A.extend(B)           # clearer intent: extend A with B's attributes
```

The `extend()` method:
- Requires `A → B` (raises `DataJointError` otherwise)
- Does not expose `allow_nullable_pk` (that's an internal mechanism)
- Expresses the semantic intent: "add B's attributes to A's entities"

**Relationship to aggregation:**

A similar argument applies to `A.aggr(B, ...)`:
- It preserves A's primary key
- It adds computed attributes derived from B
- It's conceptually a variation of projection with grouping

Both `A.join(B, left=True)` (when A → B) and `A.aggr(B, ...)` can be viewed as **projection-like operations** that extend A's attributes while preserving its entity identity.

### Bypassing the Left Join Constraint

For special cases where the user takes responsibility for handling the potentially nullable primary key, the constraint can be bypassed using `allow_nullable_pk=True`:

```python
# Normally blocked - A does not determine B
A.join(B, left=True)  # Error: A → B not satisfied

# Bypass the constraint - user takes responsibility
A.join(B, left=True, allow_nullable_pk=True)  # Allowed, PK = PK(A) ∪ PK(B)
```

When bypassed, the resulting primary key is the union of both operands' primary keys (PK(A) ∪ PK(B)). The user must ensure that subsequent operations (such as `GROUP BY` or projection) establish a valid primary key. The parameter name `allow_nullable_pk` reflects the specific issue: primary key attributes from the right operand could be NULL for unmatched rows.

This mechanism is used internally by aggregation (`aggr`) with `keep_all_rows=True`, which resets the primary key via the `GROUP BY` clause.

### Aggregation Exception

`A.aggr(B, keep_all_rows=True)` uses a left join internally but has the **opposite requirement**: **B → A** (the group expression B must have all of A's primary key attributes).

This apparent contradiction is resolved by the `GROUP BY` clause:

1. Aggregation requires B → A so that B can be grouped by A's primary key
2. The intermediate left join `A LEFT JOIN B` would have an invalid PK under the normal left join rules
3. Aggregation internally allows the invalid PK, producing PK(A) ∪ PK(B)
4. The `GROUP BY PK(A)` clause then **resets** the primary key to PK(A)
5. The final result has PK(A), which consists entirely of non-NULL values from A

Note: The semantic check (homologous namesake validation) is still performed for aggregation's internal join. Only the primary key validity constraint is bypassed.

**Example:**
```
Session: session_id*, date
Trial: session_id*, trial_num*, response_time    (references Session)

# Aggregation with keep_all_rows=True
Session.aggr(Trial, keep_all_rows=True, avg_rt='avg(response_time)')

# Internally: Session LEFT JOIN Trial (with invalid PK allowed)
# Intermediate PK would be {session_id} ∪ {session_id, trial_num} = {session_id, trial_num}
# But GROUP BY session_id resets PK to {session_id}
# Result: All sessions, with avg_rt=NULL for sessions without trials
```

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
```

### Invalid Operations

```python
# Anti-restriction: produces infinite set
dj.U('a', 'b') - A  # DataJointError

# Join: deprecated, use & instead
dj.U('a', 'b') * A  # DataJointError with migration guidance
```

