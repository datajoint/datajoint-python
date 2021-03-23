MySQL appears to differ from standard SQL by the sequence of evaluating the clauses of the SELECT statement.

```
Standard SQL: FROM > WHERE > GROUP BY > HAVING > SELECT
MySQL:		FROM > WHERE > SELECT > GROUP BY > HAVING
```

> TODO:  verify with latest SQL standards and postgres / CockroachDB implementations and whether this order can be configured

Moving `SELECT` to an earlier phase allows the `GROUP BY` and `HAVING` clauses to use alias column names created by the `SELECT` clause.
The current implementation targets the MySQL implementation where table column aliases can be used in `HAVING`.
If postgres or CockroachDB cannot be coerced to work this way, restrictions of aggregations will have to be updated accordingly.

## QueryExpression
`QueryExpression` is the main object representing a distinct `SELECT` statement.
It implements operators `&`, `*`, and `proj`  — restriction, join, and projection.

Property `heading` describes all attributes.

Operator `proj` creates a new heading.

Property `restriction` contains the `AndList` of conditions. Operator `&` creates a new restriction appending the new condition to the input's restriction.

Property `support` represents the `FROM` clause and contains a list of either `QueryExpression` objects or table names in the case of base queries.
The joint operator `*` adds new elements to the `support` attribute.

At least one element must be present in `support`. Multiple elements in `support` indicate a join.

From the user's perspective `QueryExpression` objects are immutable: once created they cannot be modified. All operators derive new objects.

### Alias attributes
`proj` can create an alias attribute by renaming an existing attribute or calculating a new attribute.
Alias attributes are the primary reason why subqueries are sometimes required.

### Subqueries
Projections, restrictions, and joins do not necessarily trigger new subqueries: the resulting `QueryExpression` object simply merges the properties of its inputs into self: `heading`, `restriction`, and `support`.

The input object is treated as a subquery in the following cases:
1. A restriction is applied that uses alias attributes in the heading
1. A projection uses an alias attribute to create a new alias attribute.
1. A join is performed on an alias attribute.
1. An Aggregation is used a restriction. 

An error arises if
1. If a restriction or a projection attempts to use attributes not in the current heading.
2. If attempting to join on attributes that are not join-compatible
3. If attempting to restrict by a non-join-compatible expression

A subquery is created by creating a new `QueryExpression` object (or a subclass object) with its `support` pointing to the input object.

### Join compatibility
The join is always natural (i.e. *equijoin* on the namesake attributes).

**Before version 0.13:** As of version `0.12.*` and earlier, two query expressions were considered join-compatible if their namesake attributes were the primary key of at least one of the input expressions. This rule was easiest to implement but does not provide best semantics.

**Version 0.13:** In version `0.13.*`, two query expressions are considered join-compatible if their namesake attributes are either in the primary key or in a foreign key in both input expressions.

 **Future (potentially version 0.14+):**
 This compatibility requirement will be further restricted to require that the namesake attributes ultimately derive from the same primary key attribute by being passed down through foreign keys.

The same join compatibility rules apply when restricting one query expression with another.

### Join mechanics
Any restriction applied to the inputs of a join can be applied to its output.
Therefore, those inputs that are not turned into queries donate their supports, restrictions, and projections to the join itself.

## Table
`Table` is a subclass of `QueryExpression` implementing table manipulation methods such as `insert`, `insert1`, `delete`, `update1`, and `drop`.

The restriction operator `&` applied to a `Table` preserves its class identity so that the result remains of type `Table`.
However, `proj` converts the result into a `QueryExpression` object. This may produce a base query that is not an instance of Table.

## Aggregation
`Aggregation` is a subclass of `QueryExpression`.
Its main input is the *aggregating* query expression and it takes an additional second input — the *aggregated* query expression.

The SQL equivalent of aggregation is
1. the NATURAL LEFT JOIN of the two inputs.
1. followed by a GROUP BY on the primary key arguments of the first input
1. followed by a projection.

The projection works the same as `.proj` with respect to the first input.
With respect to the second input, the projection part of aggregation allows only calculated attributes that use aggregating functions (*eg* `SUM`, `AVG`, `COUNT`)  applied to the attributes of the aggregated (second) input and non-aggregating functions on the attribute of the aggregating (first) input.

`Aggregation` supports all the same operators as `QueryExpression` except:
1. `restriction` turns into a `HAVING` clause instead of a `WHERE` clause. This allows applying any valid restriction without making a subquery (at least for MySQL). Therefore, restricting an `Aggregation` object never results in a subquery.
2. In joins, aggregation always turns into a subquery.

All other rules for subqueries remain the same as for `QueryExpression`

## Union
`Union` is a subclass of `QueryExpression`.
A `Union` object results from the `+` operator on two `QueryExpression` objects.
Its `support` property contains the list of expressions (at least two) to unify.
Thus the `+` operator on unions simply merges their supports, making a bigger union.

The `Union` operator performs an OUTER JOIN of its inputs provided that the inputs have the same primary key and no secondary attributes in common.  

Union treats all its inputs as subqueries except for unrestricted Union objects.

## Universal Sets `dj.U`
`dj.U` is a special operand in query expressions that allows performing special operations.  By itself, it can never form a query and is not a subclass of `QueryExpression`. Other query expressions are modified through participation in operations with `dj.U`.

### Aggegating by `dj.U`

### Resttricting a `dj.U` object with a `QueryExpression` object

### Joining a `dj.U` object

## Query "Backprojection"
Once a QueryExpression is used in a `fetch` operation or becomes a subquery in another query, it can project out all unnecessary attributes from its own inputs, recursively.
This is implemented by the `finalize` method.
This simplification produces much leaner queries resulting in improved query performance in version 0.13, especially on complex queries with blob data, compensating for MySQL's deficiencies in query optimization.
