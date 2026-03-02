# Restricted Diagrams

**Issues:** [#865](https://github.com/datajoint/datajoint-python/issues/865), [#1110](https://github.com/datajoint/datajoint-python/issues/1110)

## Motivation

### Error-driven cascade is fragile

The original cascade delete worked by trial-and-error: attempt `DELETE` on the parent, catch the FK integrity error, parse the MySQL error message to discover which child table is blocking, then recursively delete from that child first.

This approach has several problems:

- **MySQL 8 with limited privileges:** Returns error 1217 (`ROW_IS_REFERENCED`) instead of 1451 (`ROW_IS_REFERENCED_2`), which provides no table name. The cascade crashes (#1110).
- **PostgreSQL overhead:** PostgreSQL aborts the entire transaction on any error. Each failed delete attempt requires `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` round-trips.
- **Fragile parsing:** Different MySQL versions and privilege levels produce different error message formats.

### Graph-driven approach

`drop()` already uses graph-driven traversal — walking the dependency graph in reverse topological order, dropping leaves first. The same pattern applies to cascade delete, with the addition of **restriction propagation** through FK attribute mappings.

### Data subsetting

`dj.Diagram` provides set operators for specifying subsets of *tables*. Per-node restrictions complete the functionality for specifying cross-sections of *data* — enabling delete, export, backup, and sharing.

## Core Concept

A restricted diagram is a `Diagram` augmented with per-node restrictions. Two operators apply restrictions with different propagation semantics:

- **`cascade(expr)`** — OR at convergence. "This data and everything depending on it." For delete.
- **`restrict(expr)`** — AND at convergence. "The cross-section matching all criteria." For export.

Both propagate restrictions downstream through FK edges using `attr_map`. They differ only in how restrictions combine when multiple restricted ancestors converge at the same child.

## Restriction Propagation

A restriction applied to one table node propagates downstream through FK edges in topological order. Each downstream node accumulates a restriction derived from its restricted parent(s).

**Propagation rules for edge `Parent → Child` with `attr_map`:**

1. **Non-aliased FK** (`attr_map` is identity, e.g. `{'mouse_id': 'mouse_id'}`):
   If the parent's restriction attributes are a subset of the child's primary key, copy the restriction directly. Otherwise, restrict child by `parent.proj()`.

2. **Aliased FK** (`attr_map` renames, e.g. `{'source_mouse': 'mouse_id'}`):
   Restrict child by `parent.proj(**{fk: pk for fk, pk in attr_map.items()})`.

### Converging paths

A child node may have multiple restricted ancestors. The combination rule depends on the operator:

```
Session ──→ Recording ←── Stimulus
   ↓                         ↓
subject=1               type="visual"
```

`Recording` receives two propagated restrictions: R1 from Session, R2 from Stimulus.

**`cascade` — OR (union):** A recording is deleted if tainted by *any* restricted parent. Correct for referential integrity: if the parent row is being deleted, all child rows referencing it must go.

**`restrict` — AND (intersection):** A recording is included only if it satisfies *all* restricted ancestors. Correct for subsetting: only rows matching every condition are selected.

**Implementation:** `cascade` appends to a `list` (OR in DataJoint). `restrict` appends to an `AndList` (AND in DataJoint). The two modes are mutually exclusive on the same diagram.

### Multiple FK paths from same parent (alias nodes)

A child may reference the same parent through multiple FKs (e.g., `source_mouse` and `target_mouse` both referencing `Mouse`). These are represented as alias nodes in the dependency graph.

Multiple FK paths from the same restricted parent always combine with **OR** regardless of operation — structural, not operation-dependent.

### `part_integrity`

| Mode | Behavior |
|------|----------|
| `"enforce"` | Error if parts would be deleted without their masters |
| `"ignore"` | Allow deleting parts without masters |
| `"cascade"` | Propagate restriction upward from part to master, then re-propagate downstream |

### Pruning

After applying restrictions, some tables may have zero matching rows. `prune()` removes these from the diagram, leaving only the subgraph with actual data. Without prior restrictions, `prune()` removes physically empty tables.

### Unloaded schemas

If a child table lives in a schema not loaded into the dependency graph, the graph-driven delete won't know about it. The final parent `delete_quick()` fails with an FK error. Error-message parsing is retained as a **diagnostic fallback** to produce an actionable error: "activate schema X."

## API

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
rd             # visualize the export subgraph

# unrestricted prune: remove physically empty tables
dj.Diagram(schema).prune()

# drop: no restriction, drops entire tables
dj.Diagram(Session).drop()

# cascade with part_integrity
dj.Diagram(schema).cascade(PartTable & 'key=1', part_integrity="cascade").delete()

# Table.delete() delegates to Diagram internally
(Session & 'subject_id=1').delete()
# equivalent to:
# dj.Diagram._from_table(Session).cascade(Session & 'subject_id=1').delete()
```

## Advantages

| | Error-driven | Graph-driven |
|---|---|---|
| MySQL 8 + limited privileges | Crashes (#1110) | Works — no error parsing needed |
| PostgreSQL | Savepoint overhead per attempt | No errors triggered |
| Multiple FKs to same child | One-at-a-time via retry loop | All paths resolved upfront |
| part_integrity enforcement | Post-hoc check after delete | Post-check with transaction rollback |
| Unloaded schemas | Crash with opaque error | Clear error: "activate schema X" |
| Reusability | Delete-only | Delete, drop, export, prune |
| Inspectability | Opaque recursive cascade | Preview affected data before executing |
