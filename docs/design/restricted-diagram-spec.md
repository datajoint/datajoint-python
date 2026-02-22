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

### `_propagate_to_children(self, parent_node, mode)`

Internal. Propagates restriction from one node to its children.

For each `out_edge(parent_node)`:

1. Get `child_name, edge_props` from edge
2. If child is an alias node (`.isdigit()`), follow through to the real child
3. Get `attr_map`, `aliased` from `edge_props`
4. Build parent `FreeTable` with current restriction
5. Compute child restriction using propagation rules:

| Condition | Child restriction |
|-----------|-------------------|
| Non-aliased AND `parent_restriction_attrs ⊆ child.primary_key` | Copy parent restriction directly |
| Aliased FK (`attr_map` renames columns) | `parent_ft.proj(**{fk: pk for fk, pk in attr_map.items()})` |
| Non-aliased AND `parent_restriction_attrs ⊄ child.primary_key` | `parent_ft.proj()` |

6. Accumulate on child:
   - `cascade` mode: `_cascade_restrictions[child].extend(child_restr)` — list = OR
   - `restrict` mode: `_restrict_conditions[child].extend(child_restr)` — AndList = AND

7. Handle `part_integrity="cascade"`: if child is a part table and its master is not already restricted, propagate upward from part to master using `make_condition(master, (master.proj() & part.proj()).to_arrays(), ...)`, then re-propagate from the master.

### `delete(self, transaction=True, prompt=None) -> int`

Executes cascading delete using `_cascade_restrictions`.

```python
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.delete()
```

**Algorithm:**

1. Pre-check `part_integrity="enforce"`: for each node in `_cascade_restrictions`, if it's a part table and its master is not restricted, raise `DataJointError`
2. Get nodes with restrictions in topological order
3. If `prompt`: show preview (table name + row count for each)
4. Start transaction (if `transaction=True`)
5. Iterate in **reverse** topological order (leaves first):
   - `ft = FreeTable(conn, table_name)`
   - `ft._restriction = self._cascade_restrictions[table_name]`
   - `ft.delete_quick(get_count=True)`
6. On `IntegrityError`: diagnostic fallback — parse FK error for actionable message about unloaded schemas
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
| `src/datajoint/diagram.py` | Restructure: single `Diagram(nx.DiGraph)` class, gate only visualization. Add `_connection`, restriction dicts, `cascade()`, `restrict()`, `_propagate_to_children()`, `delete()`, `drop()`, `preview()`, `_from_table()` |
| `src/datajoint/table.py` | Rewrite `Table.delete()` (~200 → ~10 lines), `Table.drop()` (~35 → ~10 lines). Remove error-driven cascade code |
| `src/datajoint/user_tables.py` | `Part.drop()`: pass `part_integrity` through to `super().drop()` |
| `tests/integration/test_diagram_operations.py` | **New** — tests for `cascade`, `delete`, `drop`, `preview` |

## Verification

1. All existing tests pass unchanged:
   - `pytest tests/integration/test_cascading_delete.py -v`
   - `pytest tests/integration/test_cascade_delete.py -v`
   - `pytest tests/integration/test_erd.py -v`
2. New tests pass: `pytest tests/integration/test_diagram_operations.py -v`
3. Manual: `(Session & 'subject_id=1').delete()` behaves identically
4. Manual: `dj.Diagram(schema).cascade(Session & cond).preview()` shows correct counts
5. `dj.Diagram` works without matplotlib/pygraphviz for operational methods

## Open questions resolved

| Question | Resolution |
|----------|------------|
| Return new or mutate? | Return new `Diagram` (preserves original) |
| Lazy vs eager propagation? | Eager — propagate when `cascade()`/`restrict()` is called. Restrictions are `QueryExpression` objects, not executed until `preview()`/`delete()` |
| Transaction boundaries? | Same as current: build diagram (no DB writes), preview, confirm, execute in one transaction |
| Where do operations live? | On `Diagram`. `Dependencies` unchanged |
| Upward cascade scope? | Master's restriction propagates to all its descendants (natural from re-running propagation) |
| Can cascade and restrict be mixed? | No. Mutually exclusive modes. `cascade` applied once; `restrict` chainable |

## Implementation phases

### Phase 1: Restructure `Diagram` class
Remove the `if/else` gate. Single class. Gate only visualization methods.
Store `_connection` and restriction dicts. Adjust copy constructor.

### Phase 2: Restriction propagation
`cascade()`, `restrict()`, `_propagate_to_children()`.
Propagation rules, alias node handling, `part_integrity="cascade"` upward propagation.

### Phase 3: Diagram operations
`delete()`, `drop()`, `preview()`, `_from_table()`.
Diagnostic fallback for unloaded schemas. Transaction handling.

### Phase 4: Migrate `Table.delete()` and `Table.drop()`
Rewrite to delegate to `Diagram`. Update `Part.drop()`.
Remove dead cascade code from `table.py`.

### Phase 5: Tests
Run existing tests. Add `test_diagram_operations.py`.
