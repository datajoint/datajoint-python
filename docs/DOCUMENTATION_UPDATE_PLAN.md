# DataJoint Python Documentation Update Plan

This plan outlines updates to the DataJoint Python documentation, focusing on **practical API usage and Python-specific implementation details**. Conceptual and theoretical content is kept minimal with links to the [DataJoint Book](https://datajoint.github.io/datajoint-book) for deeper coverage.

## Goals

1. **Focus on API & Implementation** - Python-specific details, method signatures, code examples
2. **Keep Theory Minimal** - Link to DataJoint Book for concepts; don't duplicate
3. **Document New Features** - `object` type, pydantic-settings, staged inserts, jobs
4. **Improve Navigation** - Clearer structure aligned with Book terminology

---

## Proposed Navigation Structure

### 1. Getting Started
Practical setup and first steps.

| Current | Proposed | Changes |
|---------|----------|---------|
| `index.md` | `index.md` | Keep concise, link to Book for concepts |
| `quick-start.md` | `quick-start.md` | Focus on working code examples |
| `client/install.md` | `getting-started/install.md` | Move, keep practical |
| `client/credentials.md` | `getting-started/connect.md` | Rename, connection setup |
| `client/settings.md` | `getting-started/settings.md` | Move, keep detailed API docs |

### 2. Concepts (MINIMAL)
Brief overview with links to Book for theory.

| Current | Proposed | Changes |
|---------|----------|---------|
| `concepts/principles.md` | `concepts/index.md` | Consolidate to single overview page |
| `concepts/data-model.md` | — | Remove, link to Book |
| `concepts/data-pipelines.md` | — | Remove, link to Book |
| `concepts/teamwork.md` | — | Remove, link to Book |
| `concepts/terminology.md` | `concepts/terminology.md` | Keep as quick reference |

### 3. Schema Design (API-FOCUSED)
How to define schemas and tables in Python.

| Current | Proposed | Changes |
|---------|----------|---------|
| `design/schema.md` | `design/schema.md` | Keep, focus on `dj.Schema` API |
| `design/tables/tiers.md` | `design/tiers.md` | Keep, document Python classes |
| `design/tables/declare.md` | `design/declaration.md` | Keep, syntax reference |
| `design/tables/primary.md` | `design/primary-key.md` | Keep |
| `design/tables/attributes.md` | `design/attributes.md` | Keep, data type reference |
| `design/tables/dependencies.md` | `design/foreign-keys.md` | Rename |
| `design/tables/indexes.md` | `design/indexes.md` | Keep |
| `design/tables/lookup.md` | `design/lookup.md` | Keep |
| `design/tables/manual.md` | `design/manual.md` | Keep |
| `design/tables/master-part.md` | `design/master-part.md` | Keep |
| `design/diagrams.md` | `design/diagrams.md` | Keep, `dj.Diagram` API |
| `design/alter.md` | `design/alter.md` | Keep |
| `design/drop.md` | `design/drop.md` | Keep |
| `design/recall.md` | `design/recall.md` | Keep |
| `design/normalization.md` | — | Remove, link to Book |
| `design/integrity.md` | — | Remove, link to Book |

### 4. Data Types (API-FOCUSED)
Python-specific data type handling.

| Current | Proposed | Changes |
|---------|----------|---------|
| `design/tables/blobs.md` | `datatypes/blob.md` | Move |
| `design/tables/attach.md` | `datatypes/attach.md` | Move |
| `design/tables/filepath.md` | `datatypes/filepath.md` | Move |
| `design/tables/object.md` | `datatypes/object.md` | Move (NEW feature) |
| `design/tables/customtype.md` | `datatypes/adapters.md` | Move, rename |

### 5. Data Operations (API-FOCUSED)
CRUD operations and computations.

| Current | Proposed | Changes |
|---------|----------|---------|
| `manipulation/index.md` | `operations/index.md` | Rename |
| `manipulation/insert.md` | `operations/insert.md` | Add staged insert docs |
| `manipulation/delete.md` | `operations/delete.md` | Keep |
| `manipulation/update.md` | `operations/update.md` | Keep |
| `manipulation/transactions.md` | `operations/transactions.md` | Keep |
| `compute/make.md` | `operations/make.md` | Move |
| `compute/populate.md` | `operations/populate.md` | Move |
| `compute/key-source.md` | `operations/key-source.md` | Move |
| `compute/distributed.md` | `operations/distributed.md` | Move |
| — | `operations/jobs.md` | NEW: Job reservation API |

### 6. Queries (API-FOCUSED)
Query operators and fetch methods.

| Current | Proposed | Changes |
|---------|----------|---------|
| `query/principles.md` | `queries/index.md` | Brief intro, link to Book |
| `query/fetch.md` | `queries/fetch.md` | Full fetch API reference |
| `query/operators.md` | `queries/operators.md` | Operator overview |
| `query/restrict.md` | `queries/restrict.md` | Keep |
| `query/project.md` | `queries/project.md` | Keep |
| `query/join.md` | `queries/join.md` | Keep |
| `query/union.md` | `queries/union.md` | Keep |
| `query/aggregation.md` | `queries/aggr.md` | Rename |
| `query/universals.md` | `queries/universals.md` | Keep |
| `query/iteration.md` | `queries/iteration.md` | Keep |
| `query/query-caching.md` | `queries/caching.md` | Rename |
| `query/example-schema.md` | `queries/example-schema.md` | Keep |

### 7. Administration
Database and storage administration.

| Current | Proposed | Changes |
|---------|----------|---------|
| `sysadmin/database-admin.md` | `admin/database.md` | Move |
| `sysadmin/bulk-storage.md` | `admin/storage.md` | Move |
| `sysadmin/external-store.md` | `admin/external-store.md` | Move |

### 8. Reference

| Current | Proposed | Changes |
|---------|----------|---------|
| `api/` | `api/` | Keep auto-generated |
| `internal/transpilation.md` | `reference/transpilation.md` | Move |
| `faq.md` | `reference/faq.md` | Move |
| `develop.md` | `reference/develop.md` | Move |
| `citation.md` | `reference/citation.md` | Move |
| `changelog.md` | `reference/changelog.md` | Move |
| `publish-data.md` | `reference/publish-data.md` | Move |

---

## Content Guidelines

### Keep Minimal (Link to Book)
- Relational model theory
- Data normalization theory
- Entity-relationship concepts
- Data integrity theory
- Pipeline design principles

### Document Thoroughly (Python-Specific)
- `dj.Schema` class and decorator usage
- Table class hierarchy (`Manual`, `Lookup`, `Imported`, `Computed`, `Part`)
- Definition syntax and all data types
- `dj.config` settings API (pydantic-settings)
- Insert/delete/update method signatures
- `populate()` and `make()` method patterns
- All query operators with Python syntax
- `fetch()` method parameters and formats
- `object` type and `ObjectRef` API
- Job reservation system
- Staged insert API

---

## Priority Updates

### High Priority (New Features)
1. `operations/jobs.md` - Document job reservation system
2. `datatypes/object.md` - Verify completeness of object type docs
3. `operations/insert.md` - Add staged insert documentation
4. `getting-started/settings.md` - Verify pydantic-settings docs

### Medium Priority (Reorganization)
1. Update `mkdocs.yaml` navigation
2. Move files to new locations
3. Update internal links
4. Consolidate concepts to single page with Book links

### Lower Priority (Polish)
1. Add more code examples throughout
2. Ensure all method signatures documented
3. Add troubleshooting sections

---

## Files to Create

New files needed:
- `docs/src/concepts/index.md` (consolidated concepts overview)
- `docs/src/operations/jobs.md` (job reservation API)

Files to remove/consolidate:
- `docs/src/concepts/data-model.md` → link to Book
- `docs/src/concepts/data-pipelines.md` → link to Book
- `docs/src/concepts/teamwork.md` → link to Book
- `docs/src/design/normalization.md` → link to Book
- `docs/src/design/integrity.md` → link to Book

---

## Notes

- Every page should have working Python code examples
- Link to DataJoint Book for conceptual depth
- Focus on "how to do X in Python" rather than "what is X"
- Include method signatures and parameter documentation
- Use admonitions sparingly for critical warnings only
