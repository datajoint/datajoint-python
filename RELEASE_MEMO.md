# DataJoint Release Memo

## Branch Structure

| Branch | Purpose | Version |
|--------|---------|---------|
| `master` | Main development | 2.1.x |
| `maint/2.0` | Maintenance releases | 2.0.x |

For 2.0.x bugfixes:
1. Commit to `maint/2.0`
2. Tag and release as v2.0.x
3. Cherry-pick to master if applicable

---

## Writing Release Notes

Good release notes help users understand what changed and whether they need to take action.

### Categories

Organize changes into these categories (in order):

| Category | When to Use | Example |
|----------|-------------|---------|
| **BREAKING** | Changes that require user action | API changes, removed features |
| **Added** | New features | New methods, new options |
| **Changed** | Behavior changes (non-breaking) | Performance improvements, defaults |
| **Deprecated** | Features marked for removal | Old syntax warnings |
| **Fixed** | Bug fixes | Error corrections |
| **Security** | Security patches | Vulnerability fixes |

### Format

```markdown
## What's Changed

### BREAKING CHANGES
- **`fetch()` removed** — Use `to_dicts()`, `to_pandas()`, or `to_arrays()` instead (#123)

### Added
- New `to_polars()` method for Polars DataFrame output (#456)
- Support for custom codecs via `@codec` decorator (#789)

### Changed
- Improved query performance for complex joins (2-3x faster)
- Default connection timeout increased to 30s

### Fixed
- Fixed incorrect NULL handling in aggregations (#234)

### Full Changelog
https://github.com/datajoint/datajoint-python/compare/v2.0.0...v2.1.0
```

### Guidelines

1. **Lead with breaking changes** — Users need to see these first
2. **Explain the "why"** — Not just what changed, but why it matters
3. **Link to PRs/issues** — For users who want details
4. **Use imperative mood** — "Add feature" not "Added feature"
5. **Be concise** — One line per change, details in PR

### PR Labels

The release drafter uses PR labels to categorize changes:

| Label | Category |
|-------|----------|
| `breaking` | BREAKING CHANGES |
| `enhancement` | Added |
| `bug` | Fixed |
| `documentation` | (usually excluded) |

Ensure PRs have appropriate labels before merging.

---

## PyPI Release Process

### Steps

1. **Add labels to merged PRs** for release-drafter categorization
2. **Run "Manual Draft Release" workflow** on GitHub Actions
3. **Edit the draft release**:
   - Set release name to `Release X.Y.Z`
   - Set tag to `vX.Y.Z`
   - Review and edit release notes
4. **Publish the release**
5. Automation will:
   - Update `version.py` to `X.Y.Z`
   - Build and publish to PyPI
   - Create PR to merge version update back to master

### Version Note

The release drafter computes version from the previous tag. You may need to **manually edit** the release name for major version changes.

The regex in `post_draft_release_published.yaml` extracts version from the release name:
```bash
VERSION=$(echo "${{ github.event.release.name }}" | grep -oP '\d+\.\d+\.\d+')
```

---

## Conda-Forge Release Process

DataJoint has a [conda-forge feedstock](https://github.com/conda-forge/datajoint-feedstock).

### How Conda-Forge Updates Work

Conda-forge has **automated bots** that detect new PyPI releases and create PRs automatically:

1. **You publish to PyPI** (via the GitHub release workflow)
2. **regro-cf-autotick-bot** detects the new version within ~24 hours
3. **Bot creates a PR** to the feedstock with updated version and hash
4. **Maintainers review and merge**
5. **Package builds automatically** for all platforms

### Manual Update (if bot doesn't trigger)

If the bot doesn't create a PR, manually update the feedstock:

1. **Fork** [conda-forge/datajoint-feedstock](https://github.com/conda-forge/datajoint-feedstock)

2. **Edit `recipe/meta.yaml`**:
   ```yaml
   {% set version = "2.1.0" %}

   package:
     name: datajoint
     version: {{ version }}

   source:
     url: https://pypi.io/packages/source/d/datajoint/datajoint-{{ version }}.tar.gz
     sha256: <NEW_SHA256_HASH>

   build:
     number: 0  # Reset to 0 for new version
   ```

3. **Get the SHA256 hash**:
   ```bash
   curl -sL https://pypi.org/pypi/datajoint/2.1.0/json | jq -r '.urls[] | select(.packagetype=="sdist") | .digests.sha256'
   ```

4. **Check dependencies** match `pyproject.toml`:
   ```yaml
   requirements:
     host:
       - python {{ python_min }}
       - pip
       - setuptools >=62.0
     run:
       - python >={{ python_min }}
       - numpy
       - pandas
       - pymysql >=1.0
       - minio
       - packaging
       # ... etc
   ```

5. **Submit PR** to the feedstock

### Verification

After release:
```bash
conda search datajoint -c conda-forge
```

---

## Documentation Release Process

Documentation is hosted at [docs.datajoint.com](https://docs.datajoint.com) and built from [datajoint-docs](https://github.com/datajoint/datajoint-docs).

### How Documentation Builds Work

The documentation build:
1. Checks out `datajoint-python` from the `master` branch
2. Uses mkdocstrings to generate API docs from source docstrings
3. Builds static site with MkDocs
4. Deploys to `gh-pages` branch

### Triggering a Documentation Build

Documentation rebuilds automatically when:
- Changes are pushed to `datajoint-docs` main branch

To manually trigger a rebuild (e.g., after updating docstrings in datajoint-python):
```bash
gh workflow run development.yml --repo datajoint/datajoint-docs
```

Or use the "Run workflow" button in GitHub Actions.

### Updating Documentation

1. **For docstring changes**: Update docstrings in `datajoint-python`, then trigger a docs rebuild
2. **For content changes**: Edit files in `datajoint-docs/src/`, push to main
3. **Docstring style**: Use NumPy-style docstrings (see CONTRIBUTING.md)

### Verification

After build completes:
- Check [docs.datajoint.com](https://docs.datajoint.com)
- Verify API reference pages show updated content

---

## Maintainers

- @datajointbot
- @dimitri-yatsenko
- @ttngu207

## Links

- [datajoint-python on GitHub](https://github.com/datajoint/datajoint-python)
- [datajoint-docs on GitHub](https://github.com/datajoint/datajoint-docs)
- [datajoint-feedstock on GitHub](https://github.com/conda-forge/datajoint-feedstock)
- [datajoint on Anaconda.org](https://anaconda.org/conda-forge/datajoint)
- [datajoint on PyPI](https://pypi.org/project/datajoint/)
- [docs.datajoint.com](https://docs.datajoint.com)
