# DataJoint Python Documentation Update Plan

This plan outlines the comprehensive update to the DataJoint Python documentation, aligning it with the [DataJoint Book](https://datajoint.github.io/datajoint-book) structure while adding Python-specific API details.

## Goals

1. **Align with DataJoint Book** - Follow the same conceptual structure and terminology
2. **Add API Details** - Include Python-specific implementation details, method signatures, and code examples
3. **Document New Features** - Cover new features like `object` type, pydantic-settings configuration, staged inserts
4. **Improve Navigation** - Create a clearer, more logical navigation structure

---

## Proposed Navigation Structure

### 1. Introduction (NEW/ENHANCED)
Aligns with Book: Introduction section

| Current | Proposed | Changes |
|---------|----------|---------|
| `index.md` | `index.md` | Add purpose statement, executive summary |
| `quick-start.md` | `quick-start.md` | Expand with prerequisites, environment setup |
| — | `intro/prerequisites.md` | NEW: Python version, dependencies, database requirements |
| — | `intro/environment.md` | NEW: Development environment setup (IDE, Jupyter, Docker) |
| `client/install.md` | `intro/install.md` | Move and expand installation guide |
| `client/credentials.md` | `intro/connection.md` | Merge credentials into connection setup |

### 2. Concepts (ENHANCED)
Aligns with Book: Concepts section

| Current | Proposed | Changes |
|---------|----------|---------|
| `concepts/principles.md` | `concepts/principles.md` | Expand with complete theory |
| `concepts/data-model.md` | `concepts/relational-model.md` | Rename, align with Book terminology |
| — | `concepts/databases.md` | NEW: What databases are, why use them |
| — | `concepts/data-integrity.md` | NEW: Entity, referential, group integrity |
| `concepts/data-pipelines.md` | `concepts/pipelines.md` | Expand pipeline concepts |
| `concepts/teamwork.md` | `concepts/teamwork.md` | Keep, enhance collaboration aspects |
| `concepts/terminology.md` | `concepts/terminology.md` | Update with Book terminology |

### 3. Configuration (REORGANIZED)
Combines: Client Configuration + System Administration

| Current | Proposed | Changes |
|---------|----------|---------|
| `client/settings.md` | `config/settings.md` | Keep new pydantic-settings docs |
| `client/stores.md` | `config/stores.md` | External store configuration |
| `sysadmin/database-admin.md` | `config/database-admin.md` | Move to config section |
| `sysadmin/bulk-storage.md` | `config/storage-backends.md` | Rename, enhance with fsspec |
| `sysadmin/external-store.md` | `config/external-store.md` | Keep, enhance |
| — | `config/object-storage.md` | NEW: Object storage configuration |

### 4. Design (ENHANCED)
Aligns with Book: Design section

| Current | Proposed | Changes |
|---------|----------|---------|
| `design/schema.md` | `design/schema.md` | Keep, add API details |
| **Tables subsection** | | |
| `design/tables/tiers.md` | `design/tables/tiers.md` | Expand tier explanations |
| `design/tables/declare.md` | `design/tables/declare.md` | Add more syntax examples |
| `design/tables/primary.md` | `design/tables/primary-key.md` | Rename for consistency |
| `design/tables/attributes.md` | `design/tables/attributes.md` | Expand data types, defaults |
| `design/tables/lookup.md` | `design/tables/lookup.md` | Add use cases |
| `design/tables/manual.md` | `design/tables/manual.md` | Keep |
| — | `design/tables/imported.md` | NEW: Document Imported tables |
| — | `design/tables/computed.md` | NEW: Document Computed tables |
| `design/tables/dependencies.md` | `design/tables/foreign-keys.md` | Rename to match Book |
| `design/tables/indexes.md` | `design/tables/indexes.md` | Keep |
| `design/tables/master-part.md` | `design/tables/master-part.md` | Keep |
| **Data Types subsection** | | |
| `design/tables/blobs.md` | `design/datatypes/blob.md` | Move to datatypes |
| `design/tables/attach.md` | `design/datatypes/attach.md` | Move to datatypes |
| `design/tables/filepath.md` | `design/datatypes/filepath.md` | Move to datatypes |
| `design/tables/object.md` | `design/datatypes/object.md` | Move to datatypes (NEW feature) |
| `design/tables/customtype.md` | `design/datatypes/adapters.md` | Move, rename to match Book |
| **Other Design** | | |
| `design/diagrams.md` | `design/diagrams.md` | Keep, add API details |
| `design/normalization.md` | `design/normalization.md` | Expand with examples |
| `design/integrity.md` | `design/integrity.md` | Expand integrity concepts |
| `design/alter.md` | `design/alter.md` | Keep |
| `design/recall.md` | `design/recall.md` | Keep |
| `design/drop.md` | `design/drop.md` | Keep |

### 5. Operations (ENHANCED)
Aligns with Book: Operations section

| Current | Proposed | Changes |
|---------|----------|---------|
| `manipulation/index.md` | `operations/index.md` | Rename section |
| `manipulation/insert.md` | `operations/insert.md` | Expand with staged insert |
| `manipulation/delete.md` | `operations/delete.md` | Add cascade examples |
| `manipulation/update.md` | `operations/update.md` | Keep |
| `manipulation/transactions.md` | `operations/transactions.md` | Keep |
| **Computations** | | |
| `compute/make.md` | `operations/make.md` | Move to operations |
| `compute/populate.md` | `operations/populate.md` | Move to operations |
| `compute/key-source.md` | `operations/key-source.md` | Move to operations |
| `compute/distributed.md` | `operations/distributed.md` | Move to operations |
| — | `operations/jobs.md` | NEW: Job management and reservations |

### 6. Queries (ENHANCED)
Aligns with Book: Queries section

| Current | Proposed | Changes |
|---------|----------|---------|
| `query/principles.md` | `queries/index.md` | Rename to index |
| `query/fetch.md` | `queries/fetch.md` | Expand fetch options |
| `query/operators.md` | `queries/operators.md` | Overview of all operators |
| `query/restrict.md` | `queries/restriction.md` | Rename to match Book |
| `query/project.md` | `queries/projection.md` | Rename to match Book |
| `query/join.md` | `queries/join.md` | Keep |
| `query/union.md` | `queries/union.md` | Keep |
| `query/aggregation.md` | `queries/aggregation.md` | Keep |
| `query/universals.md` | `queries/universal-sets.md` | Keep |
| `query/iteration.md` | `queries/iteration.md` | Keep |
| `query/query-caching.md` | `queries/caching.md` | Keep |
| `query/example-schema.md` | `examples/query-examples.md` | Move to examples |

### 7. Examples (NEW SECTION)
Aligns with Book: Examples section

| Proposed | Description |
|----------|-------------|
| `examples/index.md` | Examples overview |
| `examples/university.md` | University schema example (adapt from Book) |
| `examples/query-examples.md` | Query examples (moved from query section) |
| `tutorials/json.ipynb` | Keep existing tutorial |
| `tutorials/dj-top.ipynb` | Keep existing tutorial |

### 8. Special Topics (NEW SECTION)
Aligns with Book: Special Topics section

| Proposed | Description |
|----------|-------------|
| `topics/uuid.md` | UUID primary keys |
| `topics/caching.md` | Query and result caching |
| `topics/adapters.md` | Adapted attribute types (moved) |
| `topics/migrations.md` | Schema migrations |

### 9. Reference (ENHANCED)

| Current | Proposed | Changes |
|---------|----------|---------|
| `internal/transpilation.md` | `reference/transpilation.md` | Move to reference |
| `api/` | `api/` | Keep auto-generated API docs |
| `faq.md` | `reference/faq.md` | Move to reference |
| `develop.md` | `reference/develop.md` | Move to reference |
| `citation.md` | `reference/citation.md` | Move to reference |
| `changelog.md` | `reference/changelog.md` | Move to reference |

---

## Content Updates by Section

### 1. Introduction Updates

**index.md**
- [ ] Add DataJoint purpose statement (from Book)
- [ ] Add executive summary of capabilities
- [ ] Update "Getting Started" links to new structure
- [ ] Keep pipeline example image

**quick-start.md**
- [ ] Add prerequisites section
- [ ] Expand connection setup with all methods
- [ ] Add troubleshooting tips
- [ ] Add links to full documentation sections

**NEW: intro/prerequisites.md**
- [ ] Python version requirements (3.10+)
- [ ] Required packages (automatically installed)
- [ ] Optional packages (graphviz, pandas)
- [ ] Database requirements (MySQL 8.0+, MariaDB)

**NEW: intro/environment.md**
- [ ] Development environment options
- [ ] Docker Compose setup
- [ ] GitHub Codespaces
- [ ] Local development setup

### 2. Concepts Updates

**concepts/principles.md**
- [ ] Complete the incomplete sections (Object Serialization, Diagramming, etc.)
- [ ] Add examples for each principle
- [ ] Link to implementation details

**concepts/relational-model.md** (renamed from data-model.md)
- [ ] Align terminology with Book
- [ ] Add relational algebra basics
- [ ] Explain entity-relationship model

**NEW: concepts/data-integrity.md**
- [ ] Entity integrity explanation
- [ ] Referential integrity (foreign keys)
- [ ] Group integrity (master-part)
- [ ] How DataJoint enforces each

### 3. Configuration Updates

**config/settings.md**
- [ ] Already updated with pydantic-settings - verify completeness
- [ ] Add migration guide from old config system

**NEW: config/object-storage.md**
- [ ] Object storage setup for `object` type
- [ ] S3, GCS, Azure, local backends
- [ ] fsspec configuration
- [ ] Credential management

### 4. Design Updates

**design/tables/tiers.md**
- [ ] Add tier selection decision tree
- [ ] Include practical examples for each tier
- [ ] Document tier-specific behaviors

**NEW: design/tables/imported.md**
- [ ] Document Imported table class
- [ ] External data source integration
- [ ] Make method requirements

**NEW: design/tables/computed.md**
- [ ] Document Computed table class
- [ ] Make method requirements
- [ ] Key source configuration

**design/datatypes/object.md**
- [ ] Already documented - verify completeness
- [ ] Add migration guide from attach/filepath

### 5. Operations Updates

**operations/insert.md**
- [ ] Document staged insert feature
- [ ] Add batch insert best practices
- [ ] Error handling examples

**NEW: operations/jobs.md**
- [ ] Job table functionality
- [ ] Job reservation system
- [ ] Error tracking
- [ ] Distributed computing coordination

### 6. Queries Updates

**queries/fetch.md**
- [ ] Document all fetch parameters
- [ ] Add format options (array, frame, dict)
- [ ] Performance considerations

**queries/restriction.md**
- [ ] Complete operator syntax
- [ ] Add AND/OR combinations
- [ ] NOT operator usage

### 7. Examples Section

**examples/university.md**
- [ ] Adapt University example from Book
- [ ] Include complete working code
- [ ] Show all CRUD operations
- [ ] Demonstrate queries

---

## Implementation Order

### Phase 1: Structure and Navigation
1. Update `mkdocs.yaml` with new navigation structure
2. Create new directories and placeholder files
3. Move existing files to new locations
4. Update internal links

### Phase 2: Core Content
1. Update Introduction section
2. Enhance Concepts section
3. Update Configuration section
4. Complete Design section

### Phase 3: Operations and Queries
1. Enhance Operations section
2. Improve Queries section
3. Add Examples section

### Phase 4: Polish
1. Add Special Topics
2. Update Reference section
3. Verify all links work
4. Review for consistency

---

## Files to Create

New files needed:
- `docs/src/intro/prerequisites.md`
- `docs/src/intro/environment.md`
- `docs/src/concepts/databases.md`
- `docs/src/concepts/data-integrity.md`
- `docs/src/design/tables/imported.md`
- `docs/src/design/tables/computed.md`
- `docs/src/config/object-storage.md`
- `docs/src/operations/jobs.md`
- `docs/src/examples/index.md`
- `docs/src/examples/university.md`
- `docs/src/topics/uuid.md`
- `docs/src/topics/migrations.md`

---

## Notes

- Keep Python-specific API details that differ from the generic Book
- Maintain existing good content, enhance where needed
- All code examples should be tested and working
- Use admonitions for tips, warnings, and notes
- Include cross-references between related topics
