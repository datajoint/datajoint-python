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

A restricted diagram is a `Diagram` augmented with per-node restrictions. A restriction is applied to one table and propagates downstream through FK edges using the `attr_map` stored on each edge.

```python
# Apply restriction to Session, propagate to all descendants
rd = dj.Diagram(schema) & (Session & 'subject_id=1')

# Preview what would be affected
rd.preview()

# Execute cascading delete
rd.delete()

# Or export the restricted cross-section
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

A child node may have multiple restricted ancestors. When restrictions from different parents converge at the same child, the combination depends on the operation:

**Example:**

```
Session ──→ Recording ←── Stimulus
   ↓                         ↓
subject=1               type="visual"
```

`Recording` depends on both `Session` and `Stimulus`. If `Session` is restricted to `subject=1` and `Stimulus` is restricted to `type="visual"`, `Recording` receives two propagated restrictions:
- R1: rows referencing subject=1 sessions
- R2: rows referencing visual stimuli

**For delete — OR (union):** A recording is deleted if it is tainted by *any* restricted parent. This is the correct semantic for referential integrity: if the parent row is being deleted, all child rows referencing it must go.

**For export — AND (intersection):** A recording is exported only if it satisfies *all* restricted ancestors. You want specifically subject 1's visual stimulus recordings.

**Implementation:** The diagram stores per-node restrictions as a list — one entry per converging path. The operation applies the appropriate combination:

```python
class RestrictedDiagram:
    # Per-node restrictions: table_name → list of restrictions (one per arriving path)
    _restrictions: dict[str, list]

    def delete(self, ...):
        """Delete: OR at convergence — any tainted row is deleted."""
        for table_name in reversed(self._restricted_topo_sort()):
            ft = FreeTable(conn, table_name)
            # list restriction = OR in DataJoint
            ft._restriction = self._restrictions[table_name]
            ft.delete_quick()

    def export(self, ...):
        """Export: AND at convergence — row must satisfy all restricted ancestors."""
        for table_name in self._restricted_topo_sort():
            ft = FreeTable(conn, table_name)
            for restriction in self._restrictions[table_name]:
                ft &= restriction  # sequential & = AND
            # ... fetch and export ft ...
```

### Multiple FK paths from same parent (alias nodes)

Separate from convergence of different parents, a child may reference the *same* parent through multiple FKs (e.g., `source_mouse` and `target_mouse` both referencing `Mouse`). These are represented in the dependency graph as alias nodes.

Multiple FK paths from the same restricted parent always combine with **OR** regardless of operation — a child row that references a restricted parent through *any* FK is affected. This is structural, not operation-dependent.

During propagation:
1. Walk `out_edges(parent)` — yields edges to real tables and alias nodes.
2. For alias nodes: read `attr_map` from `parent → alias` edge, follow `alias → child` to find the real child table.
3. Accumulate restrictions per real child table. Multiple paths from the same parent produce OR-combined entries in the restriction list.

### Non-downstream tables

**Delete:** Only the restricted table and its downstream dependents are affected. Tables in the diagram that are not downstream are excluded — they have no restriction and are not touched. The operation only visits nodes in `_restrictions`.

**Export:** Non-downstream tables **remain** in the export. They provide referential context — the `Lab` and `Session` rows referenced by the exported `Recording` rows should be included to maintain referential integrity in the export. This requires upward propagation after the initial downward pass: for each restricted node, include the parent rows that are actually referenced.

```
Delete scope:   restricted node ──→ downstream only
Export scope:   upstream context ←── restricted node ──→ downstream
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
# part_integrity affects propagation
rd = dj.Diagram(schema) & (PartTable & 'key=1')
rd.delete(part_integrity="cascade")
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
    """Execute cascading delete using the restricted diagram."""
    conn = self._connection
    conn.dependencies.load()

    # Only restricted nodes, in reverse topological order (leaves first)
    tables = [t for t in self.topo_sort()
              if not t.isdigit() and t in self._restrictions]

    with conn.transaction:
        for table_name in reversed(tables):
            ft = FreeTable(conn, table_name)
            # list = OR (delete any row tainted by any restricted parent)
            ft._restriction = self._restrictions[table_name]
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

### API

```python
# From a table with restriction
rd = dj.Diagram(Session & 'subject_id=1')

# Operator syntax (proposed in #865)
rd = dj.Diagram(schema) & (Session & 'subject_id=1')

# With part_integrity policy
rd = dj.Diagram(schema) & (PartTable & 'key=1')
rd.delete(part_integrity="cascade")

# Preview before executing
rd.preview()   # show affected tables and row counts
rd.draw()      # visualize with restricted nodes highlighted

# Operations
rd.delete()              # OR at convergence, downstream only
rd.export(path)          # AND at convergence, includes upstream context
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

## Implementation plan

### Phase 1: RestrictedDiagram core

1. Add `_restrictions: dict[str, list]` to `Diagram` — per-node restriction storage
2. Implement `_propagate_downstream()` — walk edges in topo order, compute child restrictions via `attr_map`
3. Implement `restrict(table_expr)` — entry point: extract table name + restriction, propagate
4. Implement `__and__` operator — syntax sugar for `restrict()`
5. Handle alias nodes during propagation (OR for multiple FK paths from same parent)
6. Handle `part_integrity` during propagation (upward cascade from part to master)

### Phase 2: Graph-driven delete

1. Implement `Diagram.delete()` — reverse topo order, OR at convergence, `delete_quick()` at each restricted node
2. Add unloaded-schema fallback error handling
3. Migrate `Table.delete()` to construct a `RestrictedDiagram` internally
4. Preserve `Part.delete()` behavior with diagram-based `part_integrity`
5. Remove error-message parsing from the critical path (retain as diagnostic fallback)

### Phase 3: Preview and visualization

1. `Diagram.preview()` — show restricted nodes with row counts
2. `Diagram.draw()` — highlight restricted nodes, show restriction labels

### Phase 4: Export and backup (future, #864/#560)

1. `Diagram.export(path)` — forward topo order, AND at convergence, fetch + write at each node
2. Upward pass to include referenced parent rows (referential context)
3. `Diagram.restore(path)` — forward topo order, insert at each node

## Files affected

| File | Change |
|------|--------|
| `src/datajoint/diagram.py` | Add `_restrictions`, `restrict()`, `__and__`, `_propagate_downstream()`, `delete()`, `preview()` |
| `src/datajoint/table.py` | Rewrite `Table.delete()` to use restricted diagram internally |
| `src/datajoint/user_tables.py` | Update `Part.delete()` to use diagram-based part_integrity |
| `src/datajoint/dependencies.py` | Possibly add helper methods for edge traversal with attr_map |
| `tests/integration/test_cascading_delete.py` | Update tests, add graph-driven cascade tests |
| `tests/integration/test_diagram.py` | New tests for restricted diagram |

## Open questions

1. **Should `Diagram & restriction` return a new subclass or augment `Diagram` in place?**
   A new subclass keeps the existing `Diagram` (visualization) clean. But the restriction machinery is intimately tied to the graph structure, suggesting in-place augmentation.

2. **Upward propagation scope for `part_integrity="cascade"`:**
   When a restriction propagates up from part to master, should the master's restriction then propagate to the master's *other* parts and descendants? The current implementation does this (lines 1098–1108 of `table.py`). The diagram approach would naturally do the same — restricting the master triggers downstream propagation to all its children.

3. **Transaction boundaries:**
   The current `Table.delete()` wraps everything in a single transaction with user confirmation. The diagram-based delete should preserve this: build the restricted diagram (read-only), show preview, get confirmation, then execute all deletes in one transaction.

4. **Lazy vs eager restriction propagation:**
   Eager: propagate all restrictions when `restrict()` is called (computes row counts immediately).
   Lazy: store parent restrictions and propagate during `delete()`/`export()` (defers queries).
   Eager is better for preview but may issue many queries upfront. Lazy is more efficient when the user just wants to delete without preview.

5. **Export: upward context scope.**
   When exporting, non-downstream tables should be included for referential integrity. How far upstream? Options: (a) all ancestors of restricted nodes, (b) only directly referenced parents, (c) full referential closure. Full closure is safest but may pull in large amounts of unrestricted data.
