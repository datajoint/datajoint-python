# Concepts

DataJoint is a framework for scientific workflow management based on relational principles.
For comprehensive coverage of the underlying theory, see the [DataJoint Book](https://datajoint.github.io/datajoint-book).

## Core Ideas

**Tables as Entity Sets**
: All data are represented as tables where each row is an entity with the same set of attributes. A primary key uniquely identifies each entity.

**Data Tiers**
: Tables are categorized by how their data originates:

| Tier | Python Class | Data Origin |
|------|--------------|-------------|
| Lookup | `dj.Lookup` | Predefined contents (parameters, options) |
| Manual | `dj.Manual` | External entry (user input, ingestion scripts) |
| Imported | `dj.Imported` | Auto-populated from external sources |
| Computed | `dj.Computed` | Auto-populated from upstream tables |

**Dependencies**
: Foreign keys define relationships between tables, enabling referential integrity and automatic cascading deletes.

**Schemas**
: Tables are grouped into schemas (database namespaces). Each schema maps to a Python module.

## Learn More

- [DataJoint Book: Concepts](https://datajoint.github.io/datajoint-book) — Relational model, data integrity, pipelines
- [DataJoint Book: Design](https://datajoint.github.io/datajoint-book) — Schema design principles, normalization
- [Terminology](terminology.md) — Quick reference for DataJoint terms
