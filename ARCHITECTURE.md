# DataJoint Architecture

Internal design documentation for DataJoint developers.

## Design Principles

DataJoint's architecture follows several key principles:

1. **Immutable Query Expressions** — Query expressions are immutable; operators create new objects
2. **Lazy Evaluation** — Queries are not executed until data is fetched
3. **Query Optimization** — Unnecessary attributes are projected out before execution
4. **Semantic Matching** — Joins use lineage-based attribute matching

## Module Overview

| Module | Purpose |
|--------|---------|
| `expression.py` | QueryExpression base class and operators |
| `table.py` | Table class with data manipulation |
| `fetch.py` | Data retrieval implementation |
| `declare.py` | Table definition parsing |
| `heading.py` | Attribute and heading management |
| `blob.py` | Blob serialization |
| `codecs.py` | Type codec system |
| `connection.py` | Database connection management |
| `schemas.py` | Schema binding and activation |

---

## Query System: SQL Transpilation

This section describes how DataJoint translates query expressions to SQL.

### MySQL Clause Evaluation Order

MySQL differs from standard SQL in the sequence of evaluating SELECT statement clauses:

```
Standard SQL: FROM > WHERE > GROUP BY > HAVING > SELECT
MySQL:        FROM > WHERE > SELECT > GROUP BY > HAVING
```

Moving `SELECT` to an earlier phase allows the `GROUP BY` and `HAVING` clauses to use
alias column names created by the `SELECT` clause. The current implementation targets
MySQL where table column aliases can be used in `HAVING`.

### QueryExpression

`QueryExpression` is the main object representing a distinct `SELECT` statement.
It implements operators `&`, `*`, and `proj` — restriction, join, and projection.

- Property `heading` describes all attributes
- Operator `proj` creates a new heading
- Property `restriction` contains the `AndList` of conditions
- Operator `&` creates a new restriction appending the new condition
- Property `support` represents the `FROM` clause (list of QueryExpression objects or table names)
- The join operator `*` adds new elements to `support`

From the user's perspective, `QueryExpression` objects are **immutable**: once created they
cannot be modified. All operators derive new objects.

### Subqueries

Projections, restrictions, and joins do not necessarily trigger new subqueries: the
resulting `QueryExpression` object simply merges the properties of its inputs into
self: `heading`, `restriction`, and `support`.

The input object is treated as a subquery in the following cases:

1. A restriction is applied that uses alias attributes in the heading
2. A projection uses an alias attribute to create a new alias attribute
3. A join is performed on an alias attribute
4. An Aggregation is used as a restriction

Errors arise if:

1. A restriction or projection attempts to use attributes not in the current heading
2. Attempting to join on attributes that are not join-compatible
3. Attempting to restrict by a non-join-compatible expression

### Join Compatibility

The join is always natural (i.e., *equijoin* on namesake attributes).

**Version 0.13+:** Two query expressions are considered join-compatible if their namesake
attributes are either in the primary key or in a foreign key in both input expressions.

**Future versions:** Compatibility will be further restricted to require that namesake
attributes ultimately derive from the same primary key attribute by being passed down
through foreign keys.

The same join compatibility rules apply when restricting one query expression with another.

### Join Mechanics

Any restriction applied to the inputs of a join can be applied to its output.
Therefore, inputs that are not turned into subqueries donate their supports,
restrictions, and projections to the join itself.

### Table

`Table` is a subclass of `QueryExpression` implementing table manipulation methods:
`insert`, `insert1`, `delete`, `update1`, and `drop`.

The restriction operator `&` applied to a `Table` preserves its class identity so that
the result remains of type `Table`. However, `proj` converts the result into a
`QueryExpression` object.

### Aggregation

`Aggregation` is a subclass of `QueryExpression`. Its main input is the *aggregating*
query expression and it takes an additional second input — the *aggregated* query expression.

The SQL equivalent of aggregation is:

1. The `NATURAL LEFT JOIN` of the two inputs
2. Followed by a `GROUP BY` on the primary key arguments of the first input
3. Followed by a projection

The projection allows only calculated attributes using aggregating functions
(`SUM`, `AVG`, `COUNT`) applied to the aggregated (second) input's attributes.

`Aggregation` supports all the same operators as `QueryExpression` except:

1. `restriction` turns into a `HAVING` clause instead of `WHERE`
2. In joins, aggregation always turns into a subquery

### Union

`Union` is a subclass of `QueryExpression` resulting from the `+` operator on two
`QueryExpression` objects. Its `support` property contains the list of expressions
to unify (at least two).

The `Union` operator performs an `OUTER JOIN` of its inputs provided that the inputs
have the same primary key and no secondary attributes in common.

Union treats all its inputs as subqueries except for unrestricted Union objects.

### Universal Sets (`dj.U`)

`dj.U` is a special operand in query expressions that allows performing special
operations. By itself, it can never form a query and is not a subclass of
`QueryExpression`. Other query expressions are modified through participation in
operations with `dj.U`.

### Query Backprojection

Once a QueryExpression is used in a `fetch` operation or becomes a subquery in another
query, it can project out all unnecessary attributes from its own inputs, recursively.
This is implemented by the `finalize` method.

This simplification produces much leaner queries resulting in improved query
performance, especially on complex queries with blob data, compensating for MySQL's
deficiencies in query optimization.

---

## Contributing

See the [Developer Guide](README.md#developer-guide) in README.md for development setup instructions.
