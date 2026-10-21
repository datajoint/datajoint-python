# DataJoint Release Memo

## Branch Structure

| Branch | Purpose | Version |
|--------|---------|---------|
| `master` | Main development | 2.3.x |

---

## Releases Are Annotated Tags

A release is an annotated tag `vX.Y.Z` whose message is the release notes. The tag is the
record of the release: `git show vX.Y.Z` or `git tag -n99 vX.Y.Z` prints its notes, and the
GitHub release mirrors them. There is no changelog file.

### Cutting a Release

1. **Collect the changes** merged since the last release:

   ```bash
   gh pr list --state merged --base master --search "merged:>YYYY-MM-DD" --json number,title
   ```

2. **Write `notes.md`.** The first line is one sentence that says what the release is about.
   Then, after a blank line, the changes grouped by area (queries, diagrams, jobs, storage, the
   backends, packaging, ...), each naming its pull request. Put anything users must act on
   first.

   ```markdown
   Deletes remove only the rows they reach, and jobs record when they finished.

   ## Deletes
   - `(T & dj.Top(n)).delete()` deletes the rows the `Top` selects, not every matching row (#1234)

   ## Jobs
   - `completed_time` is recorded to the millisecond (#1235)
   ```

3. **Tag the release commit on `master` and push the tag:**

   ```bash
   git tag -a vX.Y.Z --cleanup=verbatim -F notes.md && git push origin vX.Y.Z
   ```

   `--cleanup=verbatim` keeps lines that start with `#`, such as Markdown headings, which git
   would otherwise drop as comments.

4. **The [Release workflow](.github/workflows/release.yaml) does the rest:**
   - checks that the tag is annotated and on `master`;
   - runs the lint and test workflows;
   - builds the package at the tag;
   - publishes it to PyPI;
   - creates the GitHub release, with the tag's message as its notes and the wheel and sdist
     attached;
   - posts to Slack.

If a step fails after the tag is pushed, fix the cause and run the workflow again from the tag:
Actions -> Release -> Run workflow -> "Use workflow from" the tag, with **testpypi** unchecked.
To publish a tag to TestPyPI only, run it the same way with **testpypi** checked. Never move or
delete a pushed tag: the version on PyPI and the GitHub release are tied to it.

Pull request titles still state the user-visible change, since they are what the notes are
written from. Labels are not used for release notes.

### Versions

The package version is **derived from the git tag** at build time by
[hatch-vcs](https://github.com/ofek/hatch-vcs) (see `[tool.hatch.version]` in
`pyproject.toml`) — the tag is the single source of truth. There is **no
`version.py` value to bump** and **no follow-up version PR**: pushing the
`vX.Y.Z` tag is what sets the version everywhere (`pip install`, `pip show`, and
`datajoint.__version__`). Do not hand-edit `src/datajoint/version.py`; it only
re-exports the build-generated `_version.py`.

### Releases Before This Process

Every earlier release that has notes was re-tagged as an annotated tag on the same commit,
with those notes as its message: from the former changelog for 0.3.2 through 0.14.3, and from
the GitHub release for later versions. Such a message ends with a `Retroactive:` line, and the
Release workflow never publishes a tag that has one. A few early versions had no notes
anywhere; their tags stay as they were.

---

## Conda-Forge Release Process

DataJoint has a [conda-forge feedstock](https://github.com/conda-forge/datajoint-feedstock).

### How Conda-Forge Updates Work

Conda-forge has **automated bots** that detect new PyPI releases and create PRs automatically:

1. **You publish to PyPI** (by pushing the release tag)
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
       - pymysql >=0.7.2
       - deepdiff
       - pyparsing
       - pandas
       - tqdm
       - networkx
       - pydot
       - fsspec >=2023.1.0
       - pydantic-settings >=2.0.0
       - packaging
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
- @MilagrosMarin
- @ttngu207

## Links

- [datajoint-python on GitHub](https://github.com/datajoint/datajoint-python)
- [datajoint-docs on GitHub](https://github.com/datajoint/datajoint-docs)
- [datajoint-feedstock on GitHub](https://github.com/conda-forge/datajoint-feedstock)
- [datajoint on Anaconda.org](https://anaconda.org/conda-forge/datajoint)
- [datajoint on PyPI](https://pypi.org/project/datajoint/)
- [docs.datajoint.com](https://docs.datajoint.com)
