# Architecture

Internal design documentation for DataJoint developers.

## Query System

- [SQL Transpilation](transpilation.md) — How DataJoint translates query expressions to SQL

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

## Contributing

See the [Contributing Guide](../develop.md) for development setup instructions.
