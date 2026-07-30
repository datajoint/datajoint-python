# Design: `expand` and `restrict` — the diagram-traversal algebra

Status: proposed (targets the 2.4 line). Supersedes the separate
`Diagram.cascade` / `Diagram.trace` / `Diagram.restrict` operators with a small,
composable algebra. `cascade` and `trace` are retained as aliases.

This document derives the design from first principles so the rules — not just
the API — are the thing agreed upon.

---

## 1. The one primitive: propagating a restriction across a foreign key

A **restriction** on a table is a subset of its rows, written as a condition.

Every foreign key `child → parent` defines a function: each child row references
exactly one parent row. Propagating a restriction across that edge is a
**semijoin** (restrict-by-matching), and it works in either direction:

- **downstream** (parent restricted `T'` → child): `S' = S ⋉ T'` — the children
  whose parent is in `T'`.
- **upstream** (child restricted `S'` → parent): `T' = T ⋉ S'` — the parents
  referenced by `S'`.

Downstream and upstream are the *same* operation pointed opposite ways along the
same FK. This is the whole engine; everything below is about how the edge rule
degenerates, what parts add, and how you accumulate across many edges.

## 2. The edge rule (R1, referential), derived by adding complications

**Base case — FK is the whole primary key, not renamed, no parts.** The parent's
primary key is embedded verbatim in the child's, with the same column names. A
primary-key restriction on the parent is a predicate on exactly those columns,
which the child also has, by the same names. So:

> **Carry the restriction unchanged** — the identical predicate that selects the
> parent rows already selects the matching child rows.

This is why "the same primary-key restriction rides the whole diagram": every
table on the path shares those PK columns by name.

**Complication A — secondary foreign key.** The parent's key lands in the child's
*secondary* (non-PK) attributes. A raw predicate still selects the right child
rows, but the restriction is no longer a statement about the child's *identity*,
so it cannot be promoted to the child's PK and ridden further. Keep it
relational: **project the restricted parent to its key and semijoin the child on
the FK columns.**

**Complication B — renamed foreign key.** The referencing columns have different
names in the child. The parent's predicate names columns the child lacks. Fix,
mechanically: **rename the restriction's columns through the FK's attribute map**
before the semijoin (reverse the rename going upstream).

**Unification.** These are one rule with two degenerate fast paths:

> **R1 (edge rule):** propagate a restriction across an FK edge as a **semijoin**
> against the restricted neighbor, projected/renamed onto the shared FK columns.
> When the FK is the whole primary key and unrenamed, the projection is the
> identity and the semijoin collapses to "apply the same predicate."

(R1 absorbs what were previously six separate rules: forward F1/F2/F3 and upward
U1/U2/U3.)

## 3. The group rule (R2, compositional)

Part tables add **compositional** integrity on top of referential integrity: a
master and its parts are one entity, created and deleted all-or-nothing.

- **master → part** needs nothing new — a part carries `→ master` in its PK, so
  R1 already sweeps in all parts of a restricted master.
- **part → master** is the new rule. A restriction landing on part rows satisfies
  referential integrity by touching just those rows, but leaves a fragment of an
  entity. So it must **lift existentially to the master** (the master is in if
  *any* of its parts is), and the master re-expands to **all** its parts.

> **R2 (group rule):** a restriction touching any part of a master's group brings
> the whole group — existential lift part→master, then expand master→all parts.

R2 is a closure over the master–part grouping, which is exactly why FK semijoins
alone can't express it.

## 4. Two operations over R1 + R2

There are exactly two irreducible ways to use the rules, and they are opposites.

### `expand` — additive (grow from one seed)

A **constructor**. Seed a single restricted table and grow outward by R1 + R2,
accumulating reachable rows by **union** (a table is reached if reachable via any
path). Directional:

```
Diagram.expand(seed, direction="down" | "up" | "both")   # default "down"
```

- `direction="down"` (default) — descendants: the **delete blast radius**.
- `direction="up"` — ancestors: the **valid query sources** a `make()` may read
  under the reproducibility contract.
- `direction="both"` — a referentially-consistent **export region** around the
  seed.

A single-seed additive closure is always consistent and never needs an
intersection: tracing up pulls exactly the referenced ancestors, cascading down
pulls exactly the dependents — all exact semijoins.

**Retained aliases:** `Diagram.cascade(seed) = expand(seed, "down")` and
`Diagram.trace(seed) = expand(seed, "up")`. Inside `make()`,
`self.upstream = Diagram.expand(self & key, direction="up")`.

### `restrict` — subtractive (progressively carve any diagram)

An **instance method** on any Diagram (including one built by `expand`). Carves
the diagram down by applying conditions, accumulating by **intersection**:

```
diagram.restrict(*conditions, direction="down" | "up" | "both")   # default "down"
```

Each condition propagates by R1 (in the chosen direction, default `"down"`) and R2. The result:
**every table is restricted by the conjunction of all conditions that reach it**;
tables that go empty drop out. Properties:

- **Progressive / chainable** — `.restrict(A).restrict(B)…`, each carves further.
- **Order-independent** — the result is the conjunction of all conditions, so
  order doesn't matter.
- **Monotone** — every step only removes; every intermediate is a valid slice.

`restrict` is *not* reducible to combinations of `expand`, because it applies
**multiple independent conditions** and gives each table the AND of the ones
upstream of it. Example: "all data for `mouse_id=5` **and** `method_id=5`" —
`Mouse` and `ProcessingMethod` are independent ancestors meeting only at a shared
descendant; `expand` + combine cannot assemble it, `restrict.restrict` does.

## 5. Why this is the whole story

- **Intersection is not a convergence rule.** It only arises in the subtractive
  model with multiple independent conditions (`restrict`). The additive model
  (`expand`) is pure reachability — always union.
- **`expand` and `restrict` compose freely.** A Diagram is a set of tables, each
  holding one row-set. `expand` **unions** reachable rows in (grow); `restrict`
  **intersects** a propagated condition in (carve). One representation, so they
  chain in any order — `Diagram.expand(seed, "both").restrict(cond).restrict(cond)`
  — and the current "cascade and restrict are mutually exclusive" wall is
  removed. Per-table combine is unambiguous because composition order is explicit
  (∪ at a grow step, ∩ at a carve step).
- **Materialization is a delete-time concern, not part of traversal.** Freezing a
  group's keys before deleting (delete runs parts-before-masters) matters only
  when a traversal feeds `delete`; the read-only closures never pay for it.

## Summary

| | additive (grow) | subtractive (carve) |
|---|---|---|
| operator | `expand(seed, direction)` — constructor | `diagram.restrict(*conds, direction)` — transform |
| accumulate | union | intersection |
| serves | blast radius / make() sources / export region | multi-condition pipeline carving |
| aliases | `cascade`=down, `trace`=up | — |

One data structure, two composable transforms, two rules (R1 edge-semijoin, R2
group). `cascade`/`trace` survive as named shortcuts.

## Open / follow-ups

- **`restrict` direction default** — resolved: default is `direction="down"` (for
  both `expand` and `restrict`), matching `cascade`. A both-way carve (a
  descendant condition also trimming ancestors for a fully-consistent export
  slice) remains available via `direction="both"` but is opt-in.
- **A3 / #1481** — `direction="up"` applies R2, so `trace` descends from an
  ancestor master into its parts (reproducibility-contract grounds). This flips
  the currently-pinned `test_trace_stops_at_master_no_part_down_collection`;
  update test and `trace.md` together.
- **Platform behavior** (Pipeline Navigator: progressive vs batch carving UI) is
  a product decision; the library only guarantees the algebra above.
- **Release scope** — this is a 2.4 API evolution. The 2.3.3 line keeps the
  landed multi-FK-path fix + alias-docstring scrub and the non-traversal items;
  #1496/#1501/#1481 fold into this redesign rather than shipping as standalone
  2.3.3 patches.
