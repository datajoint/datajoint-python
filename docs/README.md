# Docs Contributions

Docs contributors should be aware of the following extensions, with the corresponding
settings files, that were used in developing these docs:

- [MarkdownLinter](https://github.com/DavidAnson/markdownlint):
  - `.markdownlint.yaml` establishes settings for various
  [linter rules](https://github.com/DavidAnson/markdownlint/blob/main/doc/Rules.md)
  - `.vscode/settings.json` formatting on save to fix linting

- [CSpell](https://github.com/streetsidesoftware/vscode-spell-checker): `cspell.json`
has various ignored words.

- [ReWrap](https://github.com/stkb/Rewrap/): `.vscode/settings.json` allows toggling
automated hard wrapping for files at 88 characters. This can also be keymapped to be
performed on individual paragraphs, see documentation.
