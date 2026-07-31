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
exactly one parent row. Propagating a restriction across that edge is itself a
**restriction** — restrict the neighbor by the restricted table, matched on the
foreign-key attributes (`&` with a query expression). It works in either
direction:

- **downstream** (a restriction on the parent, carried to the child):
  `child & parent_restricted` — the child rows whose parent is in the restricted set.
- **upstream** (a restriction on the child, carried to the parent):
  `parent & child_restricted` — the parent rows referenced by the restricted child.

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
relational: **project the restricted parent to its key and restrict the child by
it, matched on the FK columns.**

**Complication B — renamed foreign key.** The referencing columns have different
names in the child. The parent's predicate names columns the child lacks. Fix,
mechanically: **rename the restriction's columns through the FK's attribute map**
before restricting (reverse the rename going upstream).

**Unification.** These are one rule with two degenerate fast paths:

> **R1 (edge rule):** propagate a restriction across an FK edge by **restricting**
> the neighbor by the restricted table (`&`), projected/renamed onto the shared
> FK columns. When the FK is the whole primary key and unrenamed, the projection
> is the identity and the restriction collapses to "apply the same predicate."

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

R2 is a closure over the master–part grouping, which is exactly why FK
restrictions alone can't express it.

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
pulls exactly the dependents — all exact restrictions.

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
  removed. Per-table combine is unambiguous because composition order is explicit:
  a grow step ORs rows in (restrict by a list, `[cond, …]`), a carve step ANDs a
  further restriction on (chained `&`).
- **Materialization is a delete-time concern, not part of traversal.** Freezing a
  group's keys before deleting (delete runs parts-before-masters) matters only
  when a traversal feeds `delete`; the read-only closures never pay for it.

## 6. Renamed foreign keys and the seed restriction — a self-contained derivation

This section stands on its own; it does not depend on the rules above.

**Setup.** A foreign key copies a parent table's referenced attributes into the
child. A *renamed* foreign key gives those copied attributes new names in the
child. Record the edge's renaming as pairs `parent_attr -> child_attr`, one per
referenced attribute; a non-renamed foreign key pairs each attribute with itself.
Renaming in a foreign key is a pure attribute rename — it never computes or
changes a value or type. Example:

```python
class Session(dj.Manual):
    definition = """
    subject_id : int32
    session_id : int32
    """

class Analysis(dj.Manual):
    definition = """
    -> Session.proj(animal='subject_id', sess='session_id')
    analysis_id : int32
    """
# edge renaming (Session -> Analysis): subject_id -> animal, session_id -> sess
```

**Goal.** `Diagram.expand(A & r)` seeds table `A` with restriction `r` and grows
the related sub-diagram. Each time we cross a foreign key we must re-express the
restriction in the neighbor's attribute names. Renaming is the only thing that
changes names across an edge, so it is the only place this needs care. The shape
of `r` decides how. A restriction comes in one of three kinds, and they cross a
renamed edge differently:

- **materialized** — a dict of primary-key values, or a sequence of them
  (literal `attr: value` rows, e.g. `A.keys()`);
- **subquery** — a query expression (another table, possibly restricted);
- **string** — a raw SQL predicate over attribute names, e.g. `'weight > 10'`.

Only the materialized kind is *frozen literal values*; the other two are *live*
(evaluated against current data). This split is what decides whether a renamed
edge can be crossed by simply relabelling, or must be crossed relationally.

### Kind 1 — a materialized restriction: a dict, or a sequence of dicts

A dict is a set of "attribute equals value" conditions. Crossing a renamed
foreign key, the neighbor's restriction is obtained by **renaming the dict's keys
through the edge, values unchanged**:

- **downstream** (`A` is the parent, neighbor is the child): rewrite each
  `subject_id: 5` to `animal: 5` using the edge's `parent_attr -> child_attr`
  pairs. `A & {'subject_id': 5}` induces `child & {'animal': 5}`.
- **upstream** (`A` is the child, neighbor is the parent): apply the pairs the
  other way — `A & {'animal': 5}` induces `parent & {'subject_id': 5}`.

In both directions the rule is the same: **keep the referenced attributes
(relabelled), and drop any key field that does not exist on the neighbor.** Going
upstream this drops the child's own identity attributes (e.g. `analysis_id`),
which the parent does not have — leaving exactly the parent's key. Going
downstream nothing is dropped; the child's own key attributes are simply left
unconstrained (a partial key).

Multi-hop composes: the renamings chain, so a key is relabelled edge by edge
(`subject_id: 5`, then `animal: 5`, then `creature: 5`). This is exact because the
renaming is pure (values and types are preserved) and the attribute's identity
across the edge is fixed by the edge's pairing, not by any coincidental match of
names.

**A sequence of dicts** — as returned by `A.keys()` — is the OR (union) of its
member dicts; relabel each element the same way, and the result stays a sequence
of relabelled dicts. Because the values are frozen literals, a materialized
restriction is **stable**: it cannot be invalidated by traversal order or by
deletions elsewhere, so the delete-order hazard that forces `cascade` to
materialize (see below) does not arise here. The whole walk stays symbolic — no
subqueries, and the per-table keys stay human-legible.

**Which attributes cross an edge.** An attribute of `key` propagates across an
edge if and only if that edge **carries** it — that is, the foreign key
references it:

- A **data** attribute (`weight`) is referenced by no foreign key, so it is
  carried by no edge and never propagates. A `key` containing one is not a
  materialized-key restriction at all: that part is an opaque value-condition,
  making the whole seed an `A & cond` case (Kind 2 / Kind 3). Reduce it back to
  Kind 1 by materializing to keys first: `(A & cond).keys()`.
- A **foreign-key** attribute propagates along its own edge — including a
  *secondary* one (a below-the-line `-> Other`), which crosses that edge even
  though it is not part of `A`'s primary key.

So the test is per edge — *does `key` cover the attributes this edge carries?* —
not a global "is `key` primary-key-only?".

**Direction matters, because a foreign key is a function.** Each child row has
exactly one parent, so the two directions have different completeness needs:

- **Down** (parent to children) is the preimage — one parent, many children. A
  parent's key relabels to a *partial* child key and restricts the children that
  reference it: a parent key **always suffices**, and the child's own key
  attributes just stay free.
- **Up** (child to parent) is the image — many children, one parent. To *name*
  the referenced parent by relabelling, `key` must include the parent's **full
  primary key** (in the child's attribute names — whether those attributes are
  primary or secondary in the child). A partial parent key cannot identify the
  parent rows by relabelling alone.

**Fallback when the up-condition fails — this is where `expand` and `restrict`
differ.** When `key` does not pin the parent's full primary key, the relabel
fast-path cannot fire, and the two operations diverge by their very natures:

- **`expand` (additive) must resolve it.** Its purpose is to enumerate the
  referenced set exactly (blast radius, sources); it can include neither *all*
  parents nor *none*, so it **materializes** — queries the child for the actual
  referenced parent keys — and continues. There is no safe skip.
- **`restrict` (subtractive) may skip it.** Carving removes only rows it can
  *prove* are excluded; when it cannot determine which parent rows lack a
  surviving child, it **leaves the parent uncarved**. The slice stays
  referentially valid (a looser superset — children keep their parents), just
  less trimmed.

The relabel fast-path itself fires under the *same* condition for both (`key`
covers the edge's carried attributes; for up, the full parent primary key); only
the fallback differs — a direct consequence of additive-must-be-exact versus
subtractive-removes-only-the-provable.

Even when the fast-path fires: if a foreign key carries only part of the
neighbor's identity, the relabelled dict is a partial-key restriction on the
neighbor — still exact, just not a full key.

This is the common, cheap case: "give me everything for this entity"
(`A & {'subject_id': 5}`, or a set of such rows via `A.keys()`).

### Kind 2 — a subquery restriction: a query expression

`A & subq`, where `subq` is another table or a restricted expression, has named
attributes but no literal values yet — it is *live*. There are no keys to
relabel; cross the edge **relationally**, by projecting the restricted seed onto
the neighbor's referenced attributes (renamed) and restricting the neighbor:

- **downstream:** `child & (A & subq).proj(animal='subject_id', sess='session_id')`
  — project restricted `A` to the referenced attributes under the child's names,
  then restrict the child.
- **upstream:** `parent & (A & subq).proj(subject_id='animal', session_id='sess')`
  — project under the parent's names (the renaming reversed).

Because it is live, a subquery restriction is delete-order-sensitive: when it
feeds a `delete`, it must be materialized first (see the unifying note), which
also turns it into Kind 1.

### Kind 3 — a string query: a raw SQL predicate

`A & 'weight > 10'` is opaque text naming attributes in `A`'s namespace. It
**cannot cross the edge as text**: it may name attributes the foreign key does
not carry (`weight` here), and rewriting arbitrary SQL to the neighbor's names is
not reliable. So restrict `A` by the string first, then project the referenced
attributes (renamed) onto the neighbor and restrict — exactly as in Kind 2. The
string's *effect* crosses only through the referenced-attribute values of the
surviving `A` rows; the string itself never crosses.

### The unifying note

Kinds 2 and 3 are *live*; Kind 1 is *materialized*. Any live restriction can be
made materialized by fetching the seed's keys — `(A & r).keys()` — at which point
relabelling (Kind 1) becomes available and the result is delete-safe. That is
precisely what `cascade` does at plan time. So there are really two strategies —
**relabel** (materialized) or **restrict-then-project** (live) — and a live
restriction becomes relabel-able the moment it is materialized.

**Consequence for `expand`.** Per reached table, `expand` carries either a
relabelled materialized restriction or a relational one. Prefer the materialized
path when available: it is symbolic, composes by chaining the edge renamings,
yields legible per-table keys, and is delete-safe — this is the "update the key
names as we traverse" behavior. A live seed can be materialized up front to get
all of that.

## 7. The group rule and the relabel fast-path — how a key crosses master↔part

Section 6 handled ordinary foreign keys. Part tables are the other complication.
This section works out how a materialized key crosses a master↔part boundary,
and shows that the group rule (R2) needs **no new key machinery** — it is the
existing relabel fast-path, run twice, with the part-specific attribute
deliberately lost in between.

**Setup.** Take a master `Session` with primary key `session_id`, and its part
`Session.Trial` with primary key `(session_id, trial_id)`. A part always carries
its master's full primary key inside its own — the `-> master` reference is a
*primary, non-renamed* foreign key, so on that edge the relabel is the identity
(names are shared). The part adds its own key attribute (`trial_id`) on top. R2:
a master and its parts are one entity — a restriction touching any part lifts to
the master, and the master brings in *all* its parts.

### master to part (downstream): nothing new

`Session & {'session_id': 5}` crosses down to `Session.Trial` by the ordinary
downstream relabel: keep `session_id`, leave `trial_id` unconstrained. The result
is the partial key `{'session_id': 5}`, which selects *every* trial of session 5.
"All parts follow the master" falls straight out of the down rule.

### part to master (upstream): the existential lift is relabel-drop

`Session.Trial & {'session_id': 5, 'trial_id': 2}` going up: the master's full
primary key (`session_id`) is present, so the fast-path fires — **relabel-drop**:
keep `session_id`, drop the part-specific `trial_id` (absent on the master) →
`{'session_id': 5}`.

The "existential" needs no extra mechanism: a *sequence* of part keys spanning
several trials of session 5 all drop to the same `{'session_id': 5}`, and the
master key-set **de-duplicates**. Dropping the part-specific attribute collapses
sibling-part keys onto one master key — the OR-over-siblings is free.

### master to all parts (re-expansion): not a relabel of the seed

R2's atomicity requires that once the master is in, *all* its parts come —
including trials the seed never named. That is not obtainable by relabelling the
seed key; it is a **fresh downstream step from the recovered master key**:

`{'session_id': 5}` on `Session` → (down relabel) → `{'session_id': 5}` on
`Session.Trial`, which drops the `trial_id` constraint and so *widens* from
"trial 2" to all trials of session 5.

### The signature: the round trip is lossy, and the loss is atomicity

Follow `trial_id` around the loop:

1. seed part key `{'session_id': 5, 'trial_id': 2}`
2. lift (up, drop) → master `{'session_id': 5}` — `trial_id` gone
3. re-expand (down) → part `{'session_id': 5}` — `trial_id` cannot be restored

The part-specific constraint is destroyed by the lift and cannot be recovered on
the way back down. Stated as a rule: **a key that reaches a part via its master
carries no part-specific constraint.** That loss *is* compositional atomicity in
key terms — the whole part-group comes along precisely because the returning key
no longer distinguishes trial 2 from its siblings. The lift *narrows* the key (to
the master); the re-expansion *widens* it (to all parts); the asymmetry is the
point.

### Two corollaries

- **Materialized stays materialized.** Relabel-drop of literal values yields
  literal values, and the re-expansion is a relabel of that literal master key.
  So a materialized-key seed stays materialized through the entire
  part→master→parts round trip, and is therefore **delete-safe for free** — the
  delete-order materialization (#1496) only ever has to fire for *live*
  (subquery/string) seeds.
- **One mechanism, three call sites.** The same round trip fires in cascade-down
  (`part_integrity="cascade"`, #1429), in trace-up (#1481, a master drags in its
  parts), and in `restrict` (#1501, where a part *predicate* is promoted to its
  master and the whole group is kept — not carved part-by-part). R2 is
  direction-agnostic; only what *triggers* it differs.

So R2 adds no new key machinery: relabel-drop (lift) plus a downstream relabel
(re-expand) over the existing fast-path. The only non-relabel act is recognizing
that the re-expansion is new growth from the master, not a transform of the seed.

## Summary

| | additive (grow) | subtractive (carve) |
|---|---|---|
| operator | `expand(seed, direction)` — constructor | `diagram.restrict(*conds, direction)` — transform |
| accumulate | union | intersection |
| serves | blast radius / make() sources / export region | multi-condition pipeline carving |
| aliases | `cascade`=down, `trace`=up | — |

One data structure, two composable transforms, two rules (R1 edge-restriction, R2
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
