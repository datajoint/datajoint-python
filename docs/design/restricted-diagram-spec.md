# Spec: Restricted Diagram Implementation

**Design:** [restricted-diagram.md](restricted-diagram.md)
**PR:** [#1407](https://github.com/datajoint/datajoint-python/pull/1407)
**Branch:** `design/restricted-diagram`

## Architecture

All changes are on `dj.Diagram`. No new classes.

`dj.Diagram` currently has two definitions gated on `diagram_active` (whether pydot/matplotlib are installed):

- **Active:** `class Diagram(nx.DiGraph)` — full graph + visualization
- **Disabled:** `class Diagram` — stub that warns on instantiation

**Change:** Always define one `class Diagram(nx.DiGraph)` with all operational methods. Gate only the visualization methods on `diagram_active`.

```python
class Diagram(nx.DiGraph):
    # Always available: __init__, +/-/*, cascade, restrict,
    #                   delete, drop, preview, topo_sort, ...
    # Gated on diagram_active: draw, make_dot, make_svg, make_png,
    #                          make_image, make_mermaid, save, _repr_svg_
```

`Dependencies` remains unchanged — it is the canonical store of the current FK graph. `Diagram` copies from it and constructs derived views.

## `Diagram` Changes

### New instance attributes

```python
self._connection        # Connection — stored during __init__
self._cascade_restrictions   # dict[str, list] — per-node OR restrictions
self._restrict_conditions    # dict[str, AndList] — per-node AND restrictions
self._restriction_attrs      # dict[str, set] — restriction attribute names per node
self._part_integrity         # str — "enforce", "ignore", or "cascade"
```

Initialized empty in `__init__`. Copied in the copy constructor (`Diagram(other_diagram)`).

### `__init__` changes

The current `__init__` extracts `connection` from the source but doesn't store it. Add:

```python
self._connection = connection
```

Also initialize the restriction dicts:

```python
self._cascade_restrictions = {}
self._restrict_conditions = {}
self._restriction_attrs = {}
```

In the copy constructor branch, copy these from the source (deep copy for the dicts).

### Restriction modes: `cascade` vs `restrict`

A diagram operates in one of three states: **unrestricted** (initial), **cascade**, or **restrict**. The modes are mutually exclusive — a diagram cannot have both cascade and restrict restrictions. `cascade` is applied once; `restrict` can be chained.

```python
# cascade: applied once, OR at convergence, for delete
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')

# restrict: chainable, AND at convergence, for export
rd = dj.Diagram(schema).restrict(Session & cond).restrict(Stimulus & cond2)

# Mixing is an error:
dj.Diagram(schema).cascade(A & c).restrict(B & c)   # raises DataJointError
dj.Diagram(schema).restrict(A & c).cascade(B & c)   # raises DataJointError
dj.Diagram(schema).cascade(A & c1).cascade(B & c2)  # raises DataJointError
```

### `cascade(self, table_expr, part_integrity="enforce") -> Diagram`

Applies a cascade restriction to a table node and propagates it downstream. Returns a new `Diagram` (preserves the original). Can only be called once — a second call raises `DataJointError`.

```python
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
```

**Semantics:** OR at convergence. A child row is affected if *any* restricted ancestor taints it. Used for delete.

**Algorithm:**

1. Verify no existing cascade or restrict restrictions (raise if present)
2. `result = Diagram(self)` — copy
3. Seed `result._cascade_restrictions[root]` with `list(table_expr.restriction)`
4. Walk descendants in topological order
5. For each node with restrictions, propagate to children via `_propagate_to_children(node, mode="cascade")`
6. Return `result`

### `restrict(self, table_expr) -> Diagram`

Applies a restrict condition and propagates downstream. Returns a new `Diagram`. Can be chained — each call narrows the selection further (AND). Cannot be called on a cascade-restricted diagram.

```python
rd = dj.Diagram(schema).restrict(Session & cond).restrict(Stimulus & cond2)
```

**Semantics:** AND at convergence. A child row is included only if it satisfies *all* restricted ancestors. Used for export.

1. Verify no existing cascade restrictions (raise if present)
2. Same algorithm as `cascade` but accumulates into `_restrict_conditions` using `AndList`

### `_propagate_restrictions(self, start_node, mode, part_integrity="enforce")`

Internal. Propagates restrictions from `start_node` to all its descendants in topological order. Only processes descendants of `start_node` to avoid duplicate propagation when chaining `restrict()`.

Uses multiple passes (up to 10) to handle `part_integrity="cascade"` upward propagation, which can add new restricted nodes that need further propagation.

For each restricted node, iterates over `out_edges(node)`:

1. If target is an alias node (`.isdigit()`), follow through to real child via `out_edges(alias_node)`
2. Delegate to `_apply_propagation_rule()` for the actual restriction computation
3. Track propagated edges to avoid duplicate work
4. Handle `part_integrity="cascade"`: if child is a part table and its master is not already restricted, propagate upward from part to master using `make_condition(master, (master.proj() & part.proj()).to_arrays(), ...)`, expand the allowed node set, and continue to next pass

### `_apply_propagation_rule(self, parent_ft, parent_attrs, child_node, attr_map, aliased, mode, restrictions)`

Internal. Applies one of three propagation rules to a parent→child edge:

| Condition | Child restriction |
|-----------|-------------------|
| Non-aliased AND `parent_restriction_attrs ⊆ child.primary_key` | Copy parent restriction directly |
| Aliased FK (`attr_map` renames columns) | `parent_ft.proj(**{fk: pk for fk, pk in attr_map.items()})` |
| Non-aliased AND `parent_restriction_attrs ⊄ child.primary_key` | `parent_ft.proj()` |

Accumulates on child:
- `cascade` mode: `restrictions.setdefault(child, []).extend(...)` — list = OR
- `restrict` mode: `restrictions.setdefault(child, AndList()).extend(...)` — AndList = AND

Updates `_restriction_attrs` for the child with the relevant attribute names.

### `delete(self, transaction=True, prompt=None) -> int`

Executes cascading delete using `_cascade_restrictions`.

```python
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.delete()
```

**Algorithm:**

1. Get non-alias nodes with restrictions in topological order
2. If `prompt`: show preview (table name + row count for each)
3. Start transaction (if `transaction=True`)
4. Iterate in **reverse** topological order (leaves first):
   - `ft = FreeTable(conn, table_name)`
   - `ft.restrict_in_place(self._cascade_restrictions[table_name])`
   - `ft.delete_quick(get_count=True)`
   - Track which tables had rows deleted
5. On `IntegrityError`: cancel transaction, diagnostic fallback — parse FK error for actionable message about unloaded schemas
6. Post-check `part_integrity="enforce"`: if any part table had rows deleted but its master did not, cancel transaction and raise `DataJointError`
7. Confirm/commit transaction (same logic as current `Table.delete`)
8. Return count from the root table

### `drop(self, prompt=None, part_integrity="enforce")`

Drops all tables in `nodes_to_show` in reverse topological order.

```python
dj.Diagram(Session).drop()
# Equivalent to current Session.drop()
```

**Algorithm:**

1. Get non-alias nodes from `nodes_to_show` in topological order
2. Pre-check `part_integrity`: if any part's master is not in the set, raise error
3. If `prompt`: show preview, ask confirmation
4. Iterate in reverse order: `FreeTable(conn, t).drop_quick()`
5. On `IntegrityError`: diagnostic fallback for unloaded schemas

### `preview(self) -> dict[str, int]`

Shows affected tables and row counts without modifying data.

```python
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.preview()  # logs and returns {table_name: count}
```

Returns dict of `{full_table_name: row_count}` for each node that has a cascade or restrict restriction.

### `prune(self) -> Diagram`

Removes tables with zero matching rows from the diagram. Returns a new `Diagram`.

```python
# Unrestricted: remove physically empty tables
dj.Diagram(schema).prune()

# After restrict: remove tables with zero matching rows
dj.Diagram(schema).restrict(Session & cond).prune()
```

**Algorithm:**

1. `result = Diagram(self)` — copy
2. If restrictions exist (`_cascade_restrictions` or `_restrict_conditions`):
   - For each restricted node, build `FreeTable` with restriction applied
   - If `len(ft) == 0`: remove node from restrictions dict, `_restriction_attrs`, and `nodes_to_show`
3. If no restrictions (unrestricted diagram):
   - For each node in `nodes_to_show`, check `len(FreeTable(conn, node))`
   - If 0: remove from `nodes_to_show`
4. Return `result`

**Properties:**
- Idempotent — pruning twice yields the same result
- Chainable — `restrict()` can be called after `prune()`
- Skips alias nodes (`.isdigit()`)

### Visualization methods (gated)

All existing visualization methods (`draw`, `make_dot`, `make_svg`, `make_png`, `make_image`, `make_mermaid`, `save`, `_repr_svg_`) raise `DataJointError("Install matplotlib and pygraphviz...")` when `diagram_active is False`. When active, they work as before.

Future enhancement: `draw()` on a restricted diagram highlights restricted nodes and shows restriction labels.

## `Table` Changes

### `Table.delete()` rewrite

Replace the ~200-line error-driven cascade (lines 979–1178) with:

```python
def delete(self, transaction=True, prompt=None, part_integrity="enforce"):
    if part_integrity not in ("enforce", "ignore", "cascade"):
        raise ValueError(...)
    from .diagram import Diagram
    diagram = Diagram._from_table(self)
    diagram = diagram.cascade(self, part_integrity=part_integrity)
    return diagram.delete(transaction=transaction, prompt=prompt)
```

`Diagram._from_table(table_expr)` is a classmethod that creates a Diagram containing the table and all its descendants (without requiring visualization packages or caller context).

### `Table.drop()` rewrite

Replace lines 1218–1253 with:

```python
def drop(self, prompt=None, part_integrity="enforce"):
    if self.restriction:
        raise DataJointError("A restricted Table cannot be dropped.")
    from .diagram import Diagram
    diagram = Diagram._from_table(self)
    diagram.drop(prompt=prompt, part_integrity=part_integrity)
```

### `Diagram._from_table(cls, table_expr) -> Diagram`

Classmethod factory for internal use by `Table.delete()` and `Table.drop()`.

```python
@classmethod
def _from_table(cls, table_expr):
    """Create a Diagram containing table_expr and all its descendants."""
    conn = table_expr.connection
    conn.dependencies.load()
    descendants = set(conn.dependencies.descendants(table_expr.full_table_name))
    result = cls.__new__(cls)
    nx.DiGraph.__init__(result, conn.dependencies)
    result._connection = conn
    result.context = {}
    result.nodes_to_show = descendants
    result._expanded_nodes = set(descendants)
    result._cascade_restrictions = {}
    result._restrict_conditions = {}
    result._restriction_attrs = {}
    return result
```

This bypasses the normal `__init__` which does caller-frame introspection and source-type resolution. It's a lightweight internal constructor that only needs `connection` and `dependencies`.

## `Part` Changes

### `Part.drop()`

Add `part_integrity` passthrough to `super().drop()`:

```python
def drop(self, part_integrity="enforce"):
    if part_integrity == "ignore":
        super().drop(part_integrity="ignore")  # passes through to Diagram.drop
    elif part_integrity == "enforce":
        raise DataJointError("Cannot drop a Part directly.")
    else:
        raise ValueError(...)
```

### `Part.delete()`

No change needed — already delegates to `super().delete(part_integrity=...)`.

## Dead code removal

After rewriting `Table.delete()`, remove from `table.py`:

- The `cascade()` inner function and retry loop (lines 1013–1120)
- The `deleted` set and `visited_masters` set (lines 1010–1011)
- The post-hoc `part_integrity` check (lines 1144–1156)
- Savepoint logic (lines 1018–1027, 1113–1114)
- The `make_condition` import — check if used elsewhere first

Retain:
- `delete_quick()` — used by `Diagram.delete()`
- `drop_quick()` — used by `Diagram.drop()`
- `IntegrityError` import — used by `insert`, diagnostic fallback

## Restriction semantics

| DataJoint type | Python type | SQL meaning |
|----------------|-------------|-------------|
| OR-combined restrictions | `list` | `WHERE (r1) OR (r2) OR ...` |
| AND-combined restrictions | `AndList` | `WHERE (r1) AND (r2) AND ...` |
| No restriction | empty `AndList()` or `None` | No WHERE clause (all rows) |

For `_cascade_restrictions`: values are `list` (OR). An unrestricted cascade stores `[]` as the value, meaning "no restriction = all rows". When applying: `ft._restriction = restrictions[node]` — an empty list means unrestricted (DataJoint treats empty restriction as "all rows" via `where_clause()` returning `""`).

For `_restrict_conditions`: values are `AndList` (AND). Each `.restrict()` call appends to the AndList.

## Edge cases

1. **Unrestricted delete**: `(Session()).delete()` — no restriction. `list(table_expr.restriction)` returns `[]`. Propagation with empty restriction means all descendants are unrestricted. `delete_quick()` on each deletes all rows.

2. **Mutual exclusivity of modes**: `cascade` and `restrict` cannot be mixed on the same diagram. `cascade` can only be applied once. `restrict` can be chained. Violations raise `DataJointError`.

3. **Alias nodes during propagation**: Walk `out_edges(parent)`. If target is alias node (`.isdigit()`), read `attr_map` from parent→alias edge, follow alias→child to find real child. Apply Rule 2 (aliased projection). Multiple alias paths from same parent to same child produce OR entries.

4. **Circular import**: `diagram.py` needs `FreeTable` from `table.py`. `table.py` needs `Diagram` from `diagram.py`. Both use lazy imports inside method bodies.

5. **Nodes not in graph**: If `table_expr.full_table_name` not in `self.nodes()`, raise `DataJointError`.

6. **Disabled visualization**: Operational methods always work. Only `draw()`, `make_dot()`, etc. check `diagram_active` and raise if unavailable.

## Files affected

| File | Change |
|------|--------|
| `src/datajoint/diagram.py` | Restructure: single `Diagram(nx.DiGraph)` class, gate only visualization. Add `_connection`, restriction dicts, `_part_integrity`, `cascade()`, `restrict()`, `_propagate_restrictions()`, `_apply_propagation_rule()`, `delete()`, `drop()`, `preview()`, `prune()`, `_from_table()` |
| `src/datajoint/table.py` | Rewrite `Table.delete()` (~200 → ~10 lines), `Table.drop()` (~35 → ~10 lines). Remove error-driven cascade code |
| `src/datajoint/user_tables.py` | `Part.drop()`: pass `part_integrity` through to `super().drop()` |
| `tests/integration/test_erd.py` | Add 5 `prune()` integration tests: unrestricted, after restrict, after cascade, idempotency, prune-then-restrict chaining |

## Verification

All phases complete. Tests passing:

1. All existing tests pass unchanged:
   - `pytest tests/integration/test_cascading_delete.py -v` — 12 tests
   - `pytest tests/integration/test_cascade_delete.py -v` — 6 tests (3 MySQL + 3 PostgreSQL)
   - `pytest tests/integration/test_erd.py -v` — 7 existing + 5 new prune tests
2. Manual: `(Session & 'subject_id=1').delete()` behaves identically
3. Manual: `dj.Diagram(schema).cascade(Session & cond).preview()` shows correct counts
4. `dj.Diagram` works without matplotlib/pygraphviz for operational methods

## Resolved design decisions

| Question | Resolution |
|----------|------------|
| Return new or mutate? | Return new `Diagram` (preserves original) |
| Lazy vs eager propagation? | Eager — propagate when `cascade()`/`restrict()` is called. Restrictions are `QueryExpression` objects, not executed until `preview()`/`delete()` |
| Transaction boundaries? | Same as current: build diagram (no DB writes), preview, confirm, execute in one transaction |
| Where do operations live? | On `Diagram`. `Dependencies` unchanged |
| Upward cascade scope? | Master's restriction propagates to all its descendants (natural from re-running propagation) |
| Can cascade and restrict be mixed? | No. Mutually exclusive modes. `cascade` applied once; `restrict` chainable |

## Implementation phases (all complete)

### Phase 1: Restructure `Diagram` class ✓
Single class. Gate only visualization methods.
Store `_connection`, restriction dicts, `_part_integrity`. Copy constructor copies all.

### Phase 2: Restriction propagation ✓
`cascade()`, `restrict()`, `_propagate_restrictions()`, `_apply_propagation_rule()`.
Propagation rules, alias node handling, `part_integrity="cascade"` upward propagation.

### Phase 3: Diagram operations ✓
`delete()`, `drop()`, `preview()`, `prune()`, `_from_table()`.
Diagnostic fallback for unloaded schemas. Transaction handling.

### Phase 4: Migrate `Table.delete()` and `Table.drop()` ✓
Rewritten to delegate to `Diagram`. Updated `Part.drop()`.
Dead cascade code removed from `table.py`.

### Phase 5: Tests ✓
Existing tests pass. 5 prune integration tests added to `test_erd.py`.
