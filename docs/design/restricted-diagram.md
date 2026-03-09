# Restricted Diagrams

**Issues:** [#865](https://github.com/datajoint/datajoint-python/issues/865), [#1110](https://github.com/datajoint/datajoint-python/issues/1110)

## Motivation

### Error-driven cascade is fragile

The original cascade delete worked by trial-and-error: attempt `DELETE` on the parent, catch the FK integrity error, parse the MySQL error message to discover which child table is blocking, then recursively delete from that child first.

This approach has several problems:

- **MySQL 8 with limited privileges:** Returns error 1217 (`ROW_IS_REFERENCED`) instead of 1451 (`ROW_IS_REFERENCED_2`), which provides no table name. The cascade crashes ([#1110](https://github.com/datajoint/datajoint-python/issues/1110)).
- **PostgreSQL overhead:** PostgreSQL aborts the entire transaction on any error. Each failed delete attempt requires `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` round-trips.
- **Fragile parsing:** Different MySQL versions and privilege levels produce different error message formats.

### Graph-driven approach

`drop()` already uses graph-driven traversal — walking the dependency graph in reverse topological order, dropping leaves first. The same pattern applies to cascade delete, with the addition of **restriction propagation** through FK attribute mappings.

### Data subsetting

`dj.Diagram` provides set operators for specifying subsets of *tables*. Per-node restrictions complete the functionality for specifying cross-sections of *data* — enabling delete, export, backup, and sharing.

## Architecture

Single `class Diagram(nx.DiGraph)` with all operational methods always available. Only visualization methods (`draw`, `make_dot`, `make_svg`, `make_png`, `make_image`, `make_mermaid`, `save`, `_repr_svg_`) are gated on `diagram_active`.

`Dependencies` is the canonical store of the FK graph. `Diagram` copies from it and constructs derived views.

### Instance attributes

```python
self._connection             # Connection
self._cascade_restrictions   # dict[str, list] — per-node OR restrictions (cascade mode)
self._restrict_conditions    # dict[str, AndList] — per-node AND restrictions (restrict mode)
self._restriction_attrs      # dict[str, set] — restriction attribute names per node
self._part_integrity         # str — "enforce", "ignore", or "cascade" (set by cascade())
```

### Restriction modes

A diagram operates in one of three states: **unrestricted** (initial), **cascade**, or **restrict**. The modes are mutually exclusive. `cascade` is applied once; `restrict` can be chained.

```python
# cascade: applied once, OR at convergence, for delete
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')

# restrict: chainable, AND at convergence, for export
rd = dj.Diagram(schema).restrict(Session & cond).restrict(Stimulus & cond2)

# Mixing raises DataJointError
```

## Restriction Propagation

A restriction applied to one table node propagates downstream through FK edges in topological order. Each downstream node accumulates a restriction derived from its restricted parent(s).

### Propagation rules

For edge `Parent → Child` with `attr_map`:

| Condition | Child restriction |
|-----------|-------------------|
| Non-aliased AND `parent_attrs ⊆ child.primary_key` | Copy parent restriction directly |
| Aliased FK (`attr_map` renames columns) | `parent_ft.proj(**{fk: pk for fk, pk in attr_map.items()})` |
| Non-aliased AND `parent_attrs ⊄ child.primary_key` | `parent_ft.proj()` |

Restrictions are applied via `restrict()` → `make_condition()`, ensuring `AndList` and `QueryExpression` objects are properly converted to SQL. Direct assignment to `_restriction` is never used, as `where_clause()` would produce invalid SQL from `str(AndList)` or `str(QueryExpression)`.

### Converging paths

A child node may have multiple restricted ancestors. The combination rule depends on the operator:

```
Session ──→ Recording ←── Stimulus
   ↓                         ↓
subject=1               type="visual"
```

`Recording` receives two propagated restrictions: R1 from Session, R2 from Stimulus.

**`cascade` — OR (union):** A recording is deleted if tainted by *any* restricted parent. Correct for referential integrity: if the parent row is being deleted, all child rows referencing it must go. Implemented by passing the full restriction list to `restrict()`, which creates an OrList.

**`restrict` — AND (intersection):** A recording is included only if it satisfies *all* restricted ancestors. Correct for subsetting: only rows matching every condition are selected. Implemented by iterating restrictions and calling `restrict()` for each.

| DataJoint type | Python type | SQL meaning |
|----------------|-------------|-------------|
| OR-combined restrictions | `list` | `WHERE (r1) OR (r2) OR ...` |
| AND-combined restrictions | `AndList` | `WHERE (r1) AND (r2) AND ...` |
| No restriction | empty `list` or `AndList()` | No WHERE clause (all rows) |

### Multiple FK paths from same parent

A child may reference the same parent through multiple FKs (e.g., `source_mouse` and `target_mouse` both referencing `Mouse`). These are represented as alias nodes in the dependency graph. Multiple FK paths from the same restricted parent always combine with **OR** — structural, not operation-dependent.

### `part_integrity`

| Mode | Behavior |
|------|----------|
| `"enforce"` | Data-driven post-check: raises only when rows were actually deleted from a Part without its master also being deleted. Avoids false positives when a Part appears in the cascade but has zero affected rows. |
| `"ignore"` | Allow deleting parts without masters |
| `"cascade"` | Propagate restriction upward from part to master, then re-propagate downstream |

### Unloaded schemas

If a child table lives in a schema not loaded into the dependency graph, the graph-driven delete won't know about it. The final parent `delete_quick()` fails with an FK error. Error-message parsing is retained as a **diagnostic fallback** to produce an actionable error: "activate schema X."

## Methods

### `cascade(self, table_expr, part_integrity="enforce") -> Diagram`

Prepare a cascading delete. Propagate a restriction downstream, then trim the diagram to the cascade subgraph. Returns a new `Diagram` containing only the seed table and its descendants. One-shot — cannot be called twice or mixed with `restrict()`.

1. Verify no existing cascade or restrict restrictions
2. Copy diagram, seed `_cascade_restrictions[root]` with `list(table_expr.restriction)`
3. Propagate via `_propagate_restrictions(root, mode="cascade", part_integrity=part_integrity)`
4. Trim graph: keep only nodes in `_cascade_restrictions` plus alias nodes connecting them; remove all ancestors and unrelated tables

### `restrict(self, table_expr) -> Diagram`

Select a data subset for export or inspection. Propagate a restriction downstream but preserve the full diagram (ancestors and unrelated tables remain). Returns a new `Diagram`. Chainable — can be called multiple times from different seed tables. Cannot be mixed with `cascade()`.

1. Verify no existing cascade restrictions
2. Copy diagram, seed/extend `_restrict_conditions[root]` with `table_expr.restriction`
3. Propagate via `_propagate_restrictions(root, mode="restrict")`

### `delete(self, transaction=True, prompt=None, dry_run=False) -> int | dict`

Execute cascading delete. Requires `cascade()` first.

1. If `dry_run`: return `preview()` without modifying data
2. Get all non-alias nodes in topological order (graph is already trimmed by `cascade()`)
3. If `prompt`: show preview (table name + row count for each)
4. Start transaction
5. Delete in **reverse** topological order (leaves first) via `_restricted_table()` + `delete_quick()`
6. On `IntegrityError`: cancel transaction, parse FK error for actionable message about unloaded schemas
7. Post-check `part_integrity="enforce"`: if any part table had rows deleted but its master did not, cancel transaction and raise
8. Confirm/commit, return count from the root table

### `drop(self, prompt=None, part_integrity="enforce", dry_run=False)`

Drop all tables in `nodes_to_show` in reverse topological order. Pre-checks `part_integrity` structurally (tables, not rows). If `dry_run`, returns row counts without dropping.

### `preview(self) -> dict[str, int]`

Return `{full_table_name: row_count}` for each node with a restriction. Requires `cascade()` or `restrict()` first. Uses `_restricted_table()` to apply restrictions with correct OR/AND semantics.

### `prune(self) -> Diagram`

Remove tables with zero matching rows. With restrictions, removes nodes where the restricted query yields zero rows. Without restrictions, removes physically empty tables. Idempotent and chainable.

### `_restricted_table(self, node) -> FreeTable`

Instance method. Creates a `FreeTable` for the given node and applies its accumulated restrictions using `restrict()` for proper SQL generation.

- **cascade mode:** Passes the entire restriction list to `restrict()`, creating an OrList (OR semantics).
- **restrict mode:** Iterates restrictions, calling `restrict()` for each (AND semantics).

### `_from_table(cls, table_expr) -> Diagram`

Classmethod factory for `Table.delete()` and `Table.drop()`. Creates a Diagram containing `table_expr` and all its descendants.

## `Table` Integration

```python
def delete(self, transaction=True, prompt=None, part_integrity="enforce", dry_run=False):
    diagram = Diagram._from_table(self)
    diagram = diagram.cascade(self, part_integrity=part_integrity)
    return diagram.delete(transaction=transaction, prompt=prompt, dry_run=dry_run)

def drop(self, prompt=None, part_integrity="enforce", dry_run=False):
    if self.restriction:
        raise DataJointError("A restricted Table cannot be dropped.")
    diagram = Diagram._from_table(self)
    diagram.drop(prompt=prompt, part_integrity=part_integrity, dry_run=dry_run)
```

## API Examples

```python
# cascade: OR propagation for delete
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.preview()   # show affected tables and row counts
rd.delete()    # downstream only, OR at convergence

# restrict: AND propagation for data subsetting
rd = (dj.Diagram(schema)
      .restrict(Session & 'subject_id=1')
      .restrict(Stimulus & 'type="visual"'))
rd.preview()   # show selected tables and row counts

# prune: remove tables with zero matching rows
rd = (dj.Diagram(schema)
      .restrict(Subject & {'species': 'mouse'})
      .restrict(Session & 'session_date > "2024-01-01"')
      .prune())
rd.preview()   # only tables with matching rows

# dry_run: preview without executing
counts = (Session & 'subject_id=1').delete(dry_run=True)
# returns {full_table_name: affected_row_count}

# Table.delete() delegates to Diagram internally
(Session & 'subject_id=1').delete()
```

## Advantages

| | Error-driven | Graph-driven |
|---|---|---|
| MySQL 8 + limited privileges | Crashes ([#1110](https://github.com/datajoint/datajoint-python/issues/1110)) | Works — no error parsing needed |
| PostgreSQL | Savepoint overhead per attempt | No errors triggered |
| Multiple FKs to same child | One-at-a-time via retry loop | All paths resolved upfront |
| part_integrity enforcement | Post-hoc check after delete | Data-driven post-check (no false positives) |
| Unloaded schemas | Crash with opaque error | Clear error: "activate schema X" |
| Reusability | Delete-only | Delete, drop, export, prune |
| Inspectability | Opaque recursive cascade | `preview()` / `dry_run` before executing |
