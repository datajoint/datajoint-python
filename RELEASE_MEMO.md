# DataJoint 2.0 Release Memo

## PyPI Release Process

### Steps

1. **Run "Manual Draft Release" workflow** on GitHub Actions
2. **Edit the draft release**:
   - Change release name to `Release 2.0.0`
   - Change tag to `v2.0.0`
3. **Publish the release**
4. Automation will:
   - Update `version.py` to `2.0.0`
   - Build and publish to PyPI
   - Create PR to merge version update back to master

### Version Note

The release drafter computes version from the previous tag (`v0.14.6`), so it would generate `0.14.7` or `0.15.0`. You must **manually edit** the release name to include `2.0.0`.

The regex on line 42 of `post_draft_release_published.yaml` extracts version from the release name:
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
4. **Maintainers review and merge** (you're listed as a maintainer)
5. **Package builds automatically** for all platforms

### Manual Update (if bot doesn't trigger)

If the bot doesn't create a PR, manually update the feedstock:

1. **Fork** [conda-forge/datajoint-feedstock](https://github.com/conda-forge/datajoint-feedstock)

2. **Edit `recipe/meta.yaml`**:
   ```yaml
   {% set version = "2.0.0" %}

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
   curl -sL https://pypi.org/pypi/datajoint/2.0.0/json | jq -r '.urls[] | select(.packagetype=="sdist") | .digests.sha256'
   ```

4. **Update license** (important for 2.0!):
   ```yaml
   about:
     license: Apache-2.0  # Changed from LGPL-2.1-only
     license_file: LICENSE
   ```

5. **Submit PR** to the feedstock

### Action Items for 2.0 Release

1. **First**: Publish to PyPI via GitHub release (name it "Release 2.0.0")
2. **Wait**: ~24 hours for conda-forge bot to detect
3. **Check**: [datajoint-feedstock PRs](https://github.com/conda-forge/datajoint-feedstock/pulls) for auto-PR
4. **Review**: Ensure license changed from LGPL to Apache-2.0
5. **Merge**: As maintainer, approve and merge the PR

### Timeline

| Step | When |
|------|------|
| PyPI release | Day 0 |
| Bot detects & creates PR | Day 0-1 |
| Review & merge PR | Day 1-2 |
| Conda-forge package available | Day 1-2 |

### Verification

After release:
```bash
conda search datajoint -c conda-forge
# Should show 2.0.0
```

---

## Maintainers

- @datajointbot
- @dimitri-yatsenko
- @drewyangdev
- @guzman-raphael
- @ttngu207

## Links

- [datajoint-feedstock on GitHub](https://github.com/conda-forge/datajoint-feedstock)
- [datajoint on Anaconda.org](https://anaconda.org/conda-forge/datajoint)
- [datajoint on PyPI](https://pypi.org/project/datajoint/)
