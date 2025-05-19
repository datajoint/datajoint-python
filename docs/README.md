# Contribute to DataJoint Documentation

This is the home for DataJoint software documentation as hosted at https://docs.datajoint.com/core/datajoint-python/latest/.

## VSCode Linter Extensions and Settings

The following extensions were used in developing these docs, with the corresponding
settings files:

- Recommended extensions are already specified in `.vscode/extensions.json`, it will ask you to install them when you open the project if you haven't installed them.
- settings in `.vscode/settings.json`
- [MarkdownLinter](https://marketplace.visualstudio.com/items?itemName=DavidAnson.vscode-markdownlint):
  - `.markdownlint.yaml` establishes settings for various
  [linter rules](https://github.com/DavidAnson/markdownlint/blob/main/doc/Rules.md)
  - `.vscode/settings.json` formatting on save to fix linting

- [CSpell](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker): `cspell.json`
has various ignored words.

- [ReWrap](https://marketplace.visualstudio.com/items?itemName=stkb.rewrap): `.vscode/settings.json` allows toggling
automated hard wrapping for files at 88 characters. This can also be keymapped to be
performed on individual paragraphs, see documentation.

## With Virtual Environment

conda
```bash
conda create -n djdocs -y
conda activate djdocs
```
venv
```bash
python -m venv .venv
source .venv/bin/activate
```

Then install the required packages:
```bash
# go to the repo's root directory to generate API docs
# cd ~/datajoint-python/

# install mkdocs related requirements
pip install -r ./docs/pip_requirements.txt
# install datajoint, since API docs are generated from the package
pip install -e .[dev]
```

Run mkdocs at: http://127.0.0.1:8000/
```bash
# go to the repo's root directory to generate API docs
# cd ~/datajoint-python/

# It will automatically reload the docs when changes are made
mkdocs serve --config-file ./docs/mkdocs.yaml
```

## With Docker

> We mostly use Docker to simplify docs deployment

Ensure you have `Docker` and `Docker Compose` installed.

Then run the following:
```bash
# It will automatically reload the docs when changes are made
MODE="LIVE" docker compose up --build
```

Navigate to http://127.0.0.1:8000/ to preview the changes.

This setup supports live-reloading so all that is needed is to save the markdown files
and/or `mkdocs.yaml` file to trigger a reload.

## Mkdocs Warning Explanation

> TL;DR: We need to do it this way for hosting, please keep it as is.

```log
INFO    -  Doc file 'index.md' contains an unrecognized relative link './develop', it was left as is. Did you mean
           'develop.md'?
```

- We use reverse proxy to proxy our docs sites, here is the proxy flow:
  - You hit `datajoint.com/*` on your browser
  - It'll bring you to the reverse proxy server first, that you wouldn't notice
  - when your URL ends with:
    - `/` is the company page
    - `/docs/` is the landing/navigation page hosted by datajoint/datajoint-docs's github pages
    - `/docs/core/datajoint-python/` is the actual docs site hosted by datajoint/datajoint-python's github pages
    - `/docs/elements/element-*/` is the actual docs site hosted by each element's github pages

```log
WARNING -  Doc file 'query/operators.md' contains a link '../../../images/concepts-operators-restriction.png', but
           the target '../../images/concepts-operators-restriction.png' is not found among documentation files.
```
- We use Github Pages to host our docs, the image references needs to follow the mkdocs's build directory structure, under `site/` directory once you run mkdocs.
