# Design: Restricted Diagrams for Cascading Operations

**Issues:** [#865](https://github.com/datajoint/datajoint-python/issues/865), [#1110](https://github.com/datajoint/datajoint-python/issues/1110)

**Branch:** `design/restricted-diagram`

## Problem

### #1110 — Cascade delete fails on MySQL 8 with insufficient privileges

DataJoint's cascade delete works by trial-and-error: attempt `DELETE` on the parent, catch the FK integrity error, **parse the MySQL error message** to discover which child table is blocking, then recursively delete from that child first.

MySQL error 1451 (`ROW_IS_REFERENCED_2`) includes the child table name and constraint details. But MySQL 8 returns error 1217 (`ROW_IS_REFERENCED`) instead when the user lacks certain privileges (`CREATE VIEW`, `SHOW VIEW`, `INDEX`, `TRIGGER`). Error 1217 provides no table name — just *"Cannot delete or update a parent row: a foreign key constraint fails"* — so the cascade crashes with `AttributeError: 'NoneType' object has no attribute 'groupdict'`.

Additional problems with the error-driven approach:

- **PostgreSQL overhead**: PostgreSQL aborts the entire transaction on any error. Each failed delete attempt requires `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` round-trips.
- **Fragile parsing**: Different MySQL versions and privilege levels produce different error message formats.
- **Opaque failures**: When parsing fails, the error message gives no actionable guidance.

### #865 — Applying restrictions to a Diagram

DataJoint needs a general-purpose way to specify a subset of data across multiple tables for delete, export, backup, and sharing. `dj.Diagram` already provides powerful set operators for specifying subsets of *tables*. Adding per-node restrictions would complete the functionality for specifying cross-sections of *data*.

## Observation

**`drop()` already uses the graph-driven approach.** The cascading drop walks the dependency graph in reverse topological order, dropping leaves first:

```python
# Current Table.drop() implementation
self.connection.dependencies.load()
tables = [t for t in self.connection.dependencies.descendants(self.full_table_name)
          if not t.isdigit()]
for table in reversed(tables):
    FreeTable(self.connection, table).drop_quick()
```

No error parsing, no trial-and-error. The same pattern can be applied to cascade delete, with the addition of **restriction propagation** through FK attribute mappings.

## Design

### Core concept: Restricted Diagram

A restricted diagram is a `Diagram` augmented with per-node restrictions. Two distinct operators apply restrictions with different propagation semantics:

- **`cascade(expr)`** — OR at convergence. "This data and everything depending on it." Used for delete.
- **`restrict(expr)`** — AND at convergence. "The cross-section matching all criteria." Used for export.

Both propagate restrictions downstream through FK edges using `attr_map`. They differ only in how restrictions combine when multiple restricted ancestors converge at the same child node.

```python
# Delete: cascade (OR at convergence, downstream only)
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.preview()
rd.delete()

# Export: restrict (AND at convergence, includes upstream context)
rd = dj.Diagram(schema).restrict(Session & 'subject_id=1').restrict(Stimulus & 'type="visual"')
rd.preview()
rd.export('/path/to/backup/')
```

### Restriction propagation

A restriction is applied to one table node. It propagates downstream through FK edges in topological order. Each downstream node accumulates a restriction derived from its restricted parent(s).

**Propagation rules for edge `Parent → Child` with `attr_map`:**

1. **Non-aliased FK** (`attr_map` is identity, e.g. `{'mouse_id': 'mouse_id'}`):
   If the parent's restriction attributes are a subset of the child's primary key, copy the restriction directly. Otherwise, restrict child by `parent.proj()`.

2. **Aliased FK** (`attr_map` renames, e.g. `{'source_mouse': 'mouse_id'}`):
   Restrict child by `parent.proj(**{fk: pk for fk, pk in attr_map.items()})`.

This reuses the existing restriction logic from the current `cascade()` function (lines 1082–1090 of `table.py`), but applies it upfront during graph traversal rather than reactively from error messages.

### Converging paths

A child node may have multiple restricted ancestors. When restrictions from different parents converge at the same child, the combination depends on which operator was used:

**Example:**

```
Session ──→ Recording ←── Stimulus
   ↓                         ↓
subject=1               type="visual"
```

`Recording` depends on both `Session` and `Stimulus`. Both are restricted. `Recording` receives two propagated restrictions:
- R1: rows referencing subject=1 sessions
- R2: rows referencing visual stimuli

**`cascade` — OR (union):** A recording is deleted if it is tainted by *any* restricted parent. This is the correct semantic for referential integrity: if the parent row is being deleted, all child rows referencing it must go.

```python
rd = dj.Diagram(schema).cascade(Session & 'subject=1')
# Recording restricted to: referencing subject=1 sessions
# Stimulus: not downstream of Session, not affected
```

Note: `cascade` typically starts from one table. If multiple tables need cascading, each `cascade()` call adds OR restrictions to downstream nodes.

**`restrict` — AND (intersection):** A recording is exported only if it satisfies *all* restricted ancestors. You want specifically subject 1's visual stimulus recordings.

```python
rd = dj.Diagram(schema).restrict(Session & 'subject=1').restrict(Stimulus & 'type="visual"')
# Recording restricted to: subject=1 sessions AND visual stimuli
# Session: restricted to subject=1 (includes upstream context)
# Stimulus: restricted to type="visual" (includes upstream context)
```

**Implementation:** The diagram stores per-node restrictions tagged by operator. `cascade` appends to a list (OR), `restrict` appends to an `AndList` (AND):

```python
class RestrictedDiagram:
    # Per-node: separate lists for cascade (OR) and restrict (AND) conditions
    _cascade_restrictions: dict[str, list]     # list = OR in DataJoint
    _restrict_conditions: dict[str, AndList]   # AndList = AND in DataJoint

    def cascade(self, table_expr):
        """OR propagation — for delete. Tainted by any restricted parent."""
        # propagate downstream, accumulate as OR (append to list)
        ...

    def restrict(self, table_expr):
        """AND propagation — for export. Must satisfy all restricted ancestors."""
        # propagate downstream, accumulate as AND (append to AndList)
        ...
```

### Multiple FK paths from same parent (alias nodes)

Separate from convergence of different parents, a child may reference the *same* parent through multiple FKs (e.g., `source_mouse` and `target_mouse` both referencing `Mouse`). These are represented in the dependency graph as alias nodes.

Multiple FK paths from the same restricted parent always combine with **OR** regardless of operation — a child row that references a restricted parent through *any* FK is affected. This is structural, not operation-dependent.

During propagation:
1. Walk `out_edges(parent)` — yields edges to real tables and alias nodes.
2. For alias nodes: read `attr_map` from `parent → alias` edge, follow `alias → child` to find the real child table.
3. Accumulate restrictions per real child table. Multiple paths from the same parent produce OR-combined entries in the restriction list.

### Non-downstream tables

**`cascade` (delete):** Only the restricted table and its downstream dependents are affected. Tables in the diagram that are not downstream are excluded — they have no restriction and are not touched.

**`restrict` (export):** Non-downstream tables **remain** in the export. They provide referential context — the `Lab` and `Session` rows referenced by the exported `Recording` rows should be included to maintain referential integrity in the export. This requires upward propagation after the initial downward pass: for each restricted node, include the parent rows that are actually referenced.

```
cascade scope:   restricted node ──→ downstream only
restrict scope:  upstream context ←── restricted node ──→ downstream
```

### `part_integrity` as a Diagram-level policy

Currently, `part_integrity` is a parameter on `Table.delete()` with three modes:

| Mode | Behavior |
|------|----------|
| `"enforce"` | Error if parts would be deleted without their masters |
| `"ignore"` | Allow deleting parts without masters (breaks integrity) |
| `"cascade"` | Also delete masters when parts are deleted |

In the restricted diagram design, `part_integrity` becomes a policy on the diagram's restriction propagation rather than a post-hoc check:

**`"enforce"` (default):** During propagation, if a restriction reaches a part table but its master is not in the diagram or is unrestricted, raise an error *before* any deletes execute. This is strictly better than the current approach, which executes all deletes within a transaction and only checks *after* the cascade completes.

**`"ignore"`:** Propagate restrictions normally. Parts may be deleted without their masters.

**`"cascade"`:** During propagation, when a restriction reaches a part table whose master is not already restricted, propagate the restriction *upward* from part to master: `master &= (master.proj() & restricted_part.proj())`. Then continue propagating the master's restriction to *its* descendants. This replaces the current ad-hoc upward cascade in lines 1086–1108 of `table.py`.

```python
# part_integrity affects cascade propagation
rd = dj.Diagram(schema).cascade(PartTable & 'key=1', part_integrity="cascade")
rd.delete()
# Master is now also restricted to rows matching the part restriction
```

### `Part.delete()` integration

The current `Part.delete()` override (in `user_tables.py:219`) gates access based on `part_integrity` before delegating to `Table.delete()`. With the diagram approach, this becomes:

- `Part.delete(part_integrity="enforce")` — raises error (unchanged)
- `Part.delete(part_integrity="ignore")` — creates a single-node diagram for the part, deletes directly
- `Part.delete(part_integrity="cascade")` — creates a diagram from the part, propagates restriction upward to master, then executes the full diagram delete

### Graph traversal for delete

```python
def delete(self):
    """Execute cascading delete using cascade restrictions."""
    conn = self._connection
    conn.dependencies.load()

    # Only cascade-restricted nodes, in reverse topological order (leaves first)
    tables = [t for t in self.topo_sort()
              if not t.isdigit() and t in self._cascade_restrictions]

    with conn.transaction:
        for table_name in reversed(tables):
            ft = FreeTable(conn, table_name)
            # list = OR (delete any row tainted by any restricted ancestor)
            ft._restriction = self._cascade_restrictions[table_name]
            ft.delete_quick()
```

No `IntegrityError` catching, no error message parsing, no savepoints. Deletes proceed in dependency order — leaves first, parents last — so FK constraints are never violated.

### Handling unloaded/inaccessible schemas

If a child table lives in a schema not loaded into the dependency graph, the graph-driven delete won't know about it. The final parent `delete_quick()` would then fail with an FK error.

**Strategy:** After the graph-driven delete completes, wrap in a single try/except:

```python
try:
    # graph-driven delete (as above)
except IntegrityError as error:
    match = conn.adapter.parse_foreign_key_error(error.args[0])
    if match:
        raise DataJointError(
            f"Delete blocked by table {match['child']} in an unloaded schema. "
            f"Activate all dependent schemas before deleting."
        ) from None
    else:
        raise DataJointError(
            "Delete blocked by a foreign key in an unloaded or inaccessible schema. "
            "Activate all dependent schemas, or ensure sufficient database privileges."
        ) from None
```

This preserves error-message parsing as a **diagnostic fallback** rather than as the primary cascade mechanism. The error is actionable: the user knows to activate the missing schema.

### Unifying `drop`

The current `Table.drop()` already uses graph-driven traversal — it is the model for this design. With the diagram, `drop` becomes a natural operation alongside `delete`:

```python
# Current Table.drop() — already graph-driven
self.connection.dependencies.load()
tables = [t for t in self.connection.dependencies.descendants(self.full_table_name)
          if not t.isdigit()]
for table in reversed(tables):
    FreeTable(self.connection, table).drop_quick()
```

`drop` is DDL (drops entire tables), not DML (deletes rows). There is no restriction to propagate — but the traversal order, `part_integrity` checks, preview, and unloaded-schema error handling are shared infrastructure.

With the diagram, `Table.drop()` becomes:

```python
# Table.drop() internally:
rd = dj.Diagram(self)    # self + all descendants
rd.drop()                # reverse topo order, drop_quick() at each node
```

`Diagram.drop()` uses the same reverse-topo traversal as `Diagram.delete()` but calls `drop_quick()` (DDL) instead of `delete_quick()` (DML) and ignores restrictions — all nodes in the diagram are dropped.

The `part_integrity` checks for drop are simpler (only `"enforce"` and `"ignore"`, no `"cascade"`). These move from `Part.drop()` into the diagram's pre-check: before dropping, verify that no part table would be dropped without its master (unless `part_integrity="ignore"`).

Shared infrastructure between `delete` and `drop`:
- Dependency graph traversal in reverse topo order
- `part_integrity` pre-checks
- Unloaded-schema error handling (diagnostic fallback)
- Preview (`Diagram.preview()` shows what would be affected)

### API

```python
# cascade: OR propagation for delete
rd = dj.Diagram(schema).cascade(Session & 'subject_id=1')
rd.preview()   # show affected tables and row counts
rd.delete()    # downstream only, OR at convergence

# restrict: AND propagation for export
rd = (dj.Diagram(schema)
      .restrict(Session & 'subject_id=1')
      .restrict(Stimulus & 'type="visual"'))
rd.preview()      # show selected tables and row counts

# prune: remove tables with zero matching rows
rd = (dj.Diagram(schema)
      .restrict(Subject & {'species': 'mouse'})
      .restrict(Session & 'session_date > "2024-01-01"')
      .prune())
rd.preview()   # only tables with matching rows
rd             # visualize the export subgraph

# unrestricted prune: remove physically empty tables
dj.Diagram(schema).prune()

# drop: no restriction, drops entire tables
rd = dj.Diagram(Session)   # Session + all descendants
rd.preview()               # show tables that would be dropped
rd.drop()                  # reverse topo order, drop_quick() at each node

# cascade with part_integrity
rd = dj.Diagram(schema).cascade(PartTable & 'key=1', part_integrity="cascade")
rd.delete()

# Table.delete() internally constructs a cascade diagram
(Session & 'subject_id=1').delete()
# equivalent to:
# dj.Diagram(Session).cascade(Session & 'subject_id=1').delete()

# Table.drop() internally constructs a diagram
Session.drop()
# equivalent to:
# dj.Diagram(Session).drop()
```

## Advantages over current approach

| | Current (error-driven) | Proposed (graph-driven) |
|---|---|---|
| MySQL 8 + limited privileges | Crashes (#1110) | Works — no error parsing needed |
| PostgreSQL | Savepoint overhead per attempt | No errors triggered |
| Multiple FKs to same child | One-at-a-time via retry loop | All paths resolved upfront |
| part_integrity enforcement | Post-hoc check after delete | Pre-check before any delete |
| Unloaded schemas | Crash with opaque error | Clear error: "activate schema X" |
| Reusability | Delete-only | Delete, export, backup, sharing |
| Inspectability | Opaque recursive cascade | Preview affected data before executing |

## Implementation status

### Phase 1: Diagram restructure and restriction propagation ✓

Single `Diagram(nx.DiGraph)` class with `_cascade_restrictions`, `_restrict_conditions`, `_restriction_attrs`, `_part_integrity`. `cascade()`, `restrict()`, `_propagate_restrictions()`, `_apply_propagation_rule()`. Alias node handling, `part_integrity="cascade"` upward propagation.

### Phase 2: Graph-driven operations ✓

`delete()`, `drop()`, `preview()`, `prune()`, `_from_table()`. Unloaded-schema fallback error handling. `Table.delete()` and `Table.drop()` rewritten to delegate to `Diagram`. Dead cascade code removed.

### Phase 3: Tests ✓

All existing tests pass. 5 prune integration tests added to `test_erd.py`.

### Phase 4: Export and backup (future, #864/#560)

Not yet implemented. See "Future work" above.

## Files changed

| File | Change |
|------|--------|
| `src/datajoint/diagram.py` | Single `Diagram(nx.DiGraph)` class with `cascade()`, `restrict()`, `_propagate_restrictions()`, `_apply_propagation_rule()`, `delete()`, `drop()`, `preview()`, `prune()`, `_from_table()` |
| `src/datajoint/table.py` | `Table.delete()` (~200 → ~10 lines) and `Table.drop()` (~35 → ~10 lines) rewritten to delegate to `Diagram`. Dead cascade code removed |
| `src/datajoint/user_tables.py` | `Part.drop()`: pass `part_integrity` through to `super().drop()` |
| `tests/integration/test_erd.py` | 5 prune integration tests added |

## Resolved design decisions

| Question | Resolution |
|----------|------------|
| Return new or mutate? | Return new `Diagram` — enables chaining and keeps original reusable |
| Upward propagation scope? | Master's restriction propagates to all its descendants (natural from re-running `_propagate_restrictions`) |
| Transaction boundaries? | Build diagram (read-only), preview, confirm, execute all deletes in one transaction |
| Lazy vs eager propagation? | Eager — propagate when `cascade()`/`restrict()` is called. Restrictions are `QueryExpression` objects, not executed until `preview()`/`delete()` |
| Export upward context? | Deferred to future work (Phase 4) |

## Future work

### Export and backup (#864/#560)

Not yet implemented. Planned:

- `Diagram.export(path)` — forward topo order, fetch + write at each restrict-restricted node
- Upward pass to include referenced parent rows (referential context)
- `Diagram.restore(path)` — forward topo order, insert at each node
