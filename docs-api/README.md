# Datajoint API docs

To generate the documentation, run

``` bash
(cd sphinx && make html)
```

Docs will be placed in the `docs/` folder.

## Editing the docs

All source files are in `sphinx/source`.
Docs are currently structured as follows:

```
sphinx/source/conf.py         # Sphinx configuration file
sphinx/source/index.rst       # This is the docs front page
sphinx/source/datajoint.*.rst # Docs for different modules
```