# Restricted Diagram Specification

**Design:** [restricted-diagram.md](restricted-diagram.md)

## Architecture

Single `class Diagram(nx.DiGraph)` with all operational methods always available. Only visualization methods (`draw`, `make_dot`, `make_svg`, `make_png`, `make_image`, `make_mermaid`, `save`, `_repr_svg_`) are gated on `diagram_active`.

```python
class Diagram(nx.DiGraph):
    # Always available: __init__, +/-/*, cascade, restrict, prune,
    #                   delete, drop, preview, topo_sort, _from_table, ...
    # Gated on diagram_active: draw, make_dot, make_svg, make_png,
    #                          make_image, make_mermaid, save, _repr_svg_
```

`Dependencies` is the canonical store of the FK graph. `Diagram` copies from it and constructs derived views.

## Instance Attributes

```python
self._connection             # Connection
self._cascade_restrictions   # dict[str, list] — per-node OR restrictions
self._restrict_conditions    # dict[str, AndList] — per-node AND restrictions
self._restriction_attrs      # dict[str, set] — restriction attribute names per node
self._part_integrity         # str — "enforce", "ignore", or "cascade"
```

Initialized empty in `__init__`. Deep-copied in the copy constructor (`Diagram(other_diagram)`).

## Restriction Modes

A diagram operates in one of three states: **unrestricted** (initial), **cascade**, or **restrict**. The modes are mutually exclusive. `cascade` is applied once; `restrict` can be chained.

```python
# cascade: applied once, OR at convergence, for delete
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')

# restrict: chainable, AND at convergence, for export
rd = dj.Diagram(schema).restrict(Session & cond).restrict(Stimulus & cond2)

# Mixing raises DataJointError:
dj.Diagram(schema).cascade(A & c).restrict(B & c)
dj.Diagram(schema).restrict(A & c).cascade(B & c)
dj.Diagram(schema).cascade(A & c1).cascade(B & c2)
```

## Methods

### `cascade(self, table_expr, part_integrity="enforce") -> Diagram`

Apply cascade restriction and propagate downstream. Returns a new `Diagram`.

**Semantics:** OR at convergence. A child row is affected if *any* restricted ancestor taints it. Used for delete.

1. Verify no existing cascade or restrict restrictions (raise if present)
2. `result = Diagram(self)` — copy
3. Seed `result._cascade_restrictions[root]` with `list(table_expr.restriction)`
4. Call `_propagate_restrictions(root, mode="cascade", part_integrity=part_integrity)`
5. Return `result`

### `restrict(self, table_expr) -> Diagram`

Apply restrict condition and propagate downstream. Returns a new `Diagram`. Chainable.

**Semantics:** AND at convergence. A child row is included only if it satisfies *all* restricted ancestors. Used for export.

1. Verify no existing cascade restrictions (raise if present)
2. `result = Diagram(self)` — copy
3. Seed/extend `result._restrict_conditions[root]` with `table_expr.restriction`
4. Call `_propagate_restrictions(root, mode="restrict")`
5. Return `result`

### `_propagate_restrictions(self, start_node, mode, part_integrity="enforce")`

Internal. Propagate restrictions from `start_node` to all its descendants in topological order. Only processes descendants of `start_node` to avoid duplicate propagation when chaining `restrict()`.

Uses multiple passes (up to 10) to handle `part_integrity="cascade"` upward propagation, which can add new restricted nodes requiring further propagation.

For each restricted node, iterates over `out_edges(node)`:

1. If target is an alias node (`.isdigit()`), follow through to real child via `out_edges(alias_node)`
2. Delegate to `_apply_propagation_rule()` for the restriction computation
3. Track propagated edges to avoid duplicate work
4. Handle `part_integrity="cascade"`: if child is a part table and its master is not already restricted, propagate upward from part to master using `make_condition(master, (master.proj() & part.proj()).to_arrays(), ...)`, expand the allowed node set, and continue to next pass

### `_apply_propagation_rule(self, parent_ft, parent_attrs, child_node, attr_map, aliased, mode, restrictions)`

Internal. Apply one of three propagation rules to a parent→child edge:

| Condition | Child restriction |
|-----------|-------------------|
| Non-aliased AND `parent_restriction_attrs ⊆ child.primary_key` | Copy parent restriction directly |
| Aliased FK (`attr_map` renames columns) | `parent_ft.proj(**{fk: pk for fk, pk in attr_map.items()})` |
| Non-aliased AND `parent_restriction_attrs ⊄ child.primary_key` | `parent_ft.proj()` |

Accumulates on child:
- `cascade` mode: `restrictions.setdefault(child, []).extend(...)` — list = OR
- `restrict` mode: `restrictions.setdefault(child, AndList()).extend(...)` — AndList = AND

### `delete(self, transaction=True, prompt=None) -> int`

Execute cascading delete using `_cascade_restrictions`. Requires `cascade()` first.

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
7. Confirm/commit transaction
8. Return count from the root table

### `drop(self, prompt=None, part_integrity="enforce")`

Drop all tables in `nodes_to_show` in reverse topological order.

1. Get non-alias nodes from `nodes_to_show` in topological order
2. Pre-check `part_integrity`: if any part's master is not in the set, raise error
3. If `prompt`: show preview, ask confirmation
4. Iterate in reverse order: `FreeTable(conn, t).drop_quick()`

### `preview(self) -> dict[str, int]`

Show affected tables and row counts without modifying data. Requires `cascade()` or `restrict()` first.

Returns `{full_table_name: row_count}` for each node with a restriction.

### `prune(self) -> Diagram`

Remove tables with zero matching rows from the diagram. Returns a new `Diagram`.

1. `result = Diagram(self)` — copy
2. If restrictions exist (`_cascade_restrictions` or `_restrict_conditions`):
   - For each restricted node, build `FreeTable` with restriction applied
   - If `len(ft) == 0`: remove from restrictions dict, `_restriction_attrs`, and `nodes_to_show`
3. If no restrictions (unrestricted diagram):
   - For each node in `nodes_to_show`, check `len(FreeTable(conn, node))`
   - If 0: remove from `nodes_to_show`
4. Return `result`

Properties: idempotent, chainable (`restrict()` can follow `prune()`), skips alias nodes.

### `_from_table(cls, table_expr) -> Diagram`

Classmethod factory for `Table.delete()` and `Table.drop()`. Creates a Diagram containing `table_expr` and all its descendants, bypassing the normal `__init__` (no caller-frame introspection or source-type resolution).

## `Table` Integration

### `Table.delete()`

Delegates to `Diagram`:

```python
def delete(self, transaction=True, prompt=None, part_integrity="enforce"):
    from .diagram import Diagram
    diagram = Diagram._from_table(self)
    diagram = diagram.cascade(self, part_integrity=part_integrity)
    return diagram.delete(transaction=transaction, prompt=prompt)
```

### `Table.drop()`

Delegates to `Diagram`:

```python
def drop(self, prompt=None, part_integrity="enforce"):
    if self.restriction:
        raise DataJointError("A restricted Table cannot be dropped.")
    from .diagram import Diagram
    diagram = Diagram._from_table(self)
    diagram.drop(prompt=prompt, part_integrity=part_integrity)
```

### `Part.drop()`

Passes `part_integrity` through to `super().drop()`.

## Restriction Semantics

| DataJoint type | Python type | SQL meaning |
|----------------|-------------|-------------|
| OR-combined restrictions | `list` | `WHERE (r1) OR (r2) OR ...` |
| AND-combined restrictions | `AndList` | `WHERE (r1) AND (r2) AND ...` |
| No restriction | empty `list` or `AndList()` | No WHERE clause (all rows) |

`_cascade_restrictions` values are `list` (OR). An unrestricted cascade stores `[]`, meaning all rows.

`_restrict_conditions` values are `AndList` (AND). Each `.restrict()` call extends the AndList.

## Edge Cases

1. **Unrestricted delete**: `(Session()).delete()` — empty restriction propagates as "all rows" to all descendants.

2. **Mutual exclusivity**: `cascade` and `restrict` cannot be mixed. `cascade` is one-shot. `restrict` is chainable. Violations raise `DataJointError`.

3. **Alias nodes**: Walk `out_edges(parent)`. If target is alias (`.isdigit()`), read `attr_map` from parent→alias edge, follow alias→child. Apply Rule 2 (aliased projection). Multiple alias paths from same parent to same child produce OR entries.

4. **Circular import**: `diagram.py` needs `FreeTable` from `table.py`. `table.py` needs `Diagram` from `diagram.py`. Both use lazy imports inside method bodies.

5. **Nodes not in graph**: If `table_expr.full_table_name` not in `self.nodes()`, raise `DataJointError`.

6. **Disabled visualization**: Operational methods always work. Only visualization methods check `diagram_active`.
