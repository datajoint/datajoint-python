# Develop

Included with the codebase is the recommended development environment configured using [DevContainer](https://containers.dev/).

## Launch Environment

Here are some options that provide a great developer experience:

- **Cloud-based IDE**: (*recommended*)
  - Launch using [GitHub Codespaces](https://github.com/features/codespaces) using the option `Create codespace on master` in the codebase repository on your fork.
  - Build time for a 2-Core codespace is **~6m**. This is done infrequently and cached for convenience.
  - Start time for a 2-Core codespace is **~2m**. This will pull the built codespace from cache when you need it.
  - *Tip*: GitHub auto names the codespace but you can rename the codespace so that it is easier to identify later.
- **Local IDE**:
  - Ensure you have [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
  - Ensure you have [Docker](https://docs.docker.com/get-docker/)
  - Ensure you have [VSCode](https://code.visualstudio.com/)
  - Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
  - `git clone` the codebase repository and open it in VSCode
  - Use the `Dev Containers extension` to `Reopen in Container` (More info in the `Getting started` included with the extension)

You will know your environment has finished loading once you see a terminal open related to `Running postStartCommand` with a final message: `Done`.

## Features

Once you've successfully launched the development environment, you'll be able to take advantage of our developer tooling to help improve productivity and quality.

### Syntax Tests

The following will verify that there are no syntax errors.

```
flake8 datajoint --count --select=E9,F63,F7,F82 --show-source --statistics
```

### Integration Tests

The following will verify there are no regression errors by running our test suite of unit and integration tests.

- Entire test suite:
  ```
  pytest -sv --cov-report term-missing --cov=datajoint tests
  ```

- A single functional test:
  ```
  pytest -sv tests/test_connection.py::test_dj_conn
  ```
- A single class test:
  ```
  pytest -sv tests/test_aggr_regressions.py::TestIssue558
  ```

### Style Tests

The following will verify that there are no code styling errors.

```
flake8 --ignore=E203,E722,W503 datajoint --count --max-complexity=62 --max-line-length=127 --statistics
```

The following will ensure the codebase has been formatted with [black](https://black.readthedocs.io/en/stable/).

```
black datajoint --check -v
```

The following will ensure the test suite has been formatted with [black](https://black.readthedocs.io/en/stable/).

```
black tests --check -v
```

### Jupyter

Jupyter notebooks are supported in this environment. This means that when you `import datajoint`, it will use the current state of the source.

Be sure to see the reference documentation if you are new to [running Jupyter notebooks w/ VSCode](https://code.visualstudio.com/docs/datascience/jupyter-notebooks#_create-or-open-a-jupyter-notebook).

### Debugger

[VSCode Debugger](https://code.visualstudio.com/docs/editor/debugging) is a powerful tool that can really accelerate fixes.

Try it as follows:

- Create a python script of your choice
- `import datajoint` (This will use the current state of the source)
- Add breakpoints by adding red dots next to line numbers
- Select the `Run and Debug` tab
- Start by clicking the button `Run and Debug`

### MySQL CLI

It is often useful in development to connect to DataJoint's relational database backend directly using the MySQL CLI.

Connect as follows to the database running within your developer environment:

```
mysql -hfakeservices.datajoint.io -uroot -ppassword
```

### Documentation

Our documentation is built using [MkDocs Material](https://squidfunk.github.io/mkdocs-material/). The easiest way to improve the documentation is by using the `docs/docker-compose.yaml` environment. The source can be modified in `docs/src` using markdown.

The docs environment can be run using 3 modes:

- **LIVE**: (*recommended*) This serves the docs locally. It supports live reloading on saves to `docs/src` files but does not support the docs version dropdown. Useful to see changes live.
  ```
  MODE="LIVE" PACKAGE=datajoint UPSTREAM_REPO=https://github.com/datajoint/datajoint-python.git HOST_UID=$(id -u) docker compose -f docs/docker-compose.yaml up --build
  ```
- **QA**: This serves the docs locally. It supports the docs version dropdown but does not support live reloading. Useful as a final check.
  ```
  MODE="QA" PACKAGE=datajoint UPSTREAM_REPO=https://github.com/datajoint/datajoint-python.git HOST_UID=$(id -u) docker compose -f docs/docker-compose.yaml up --build
  ```
- **BUILD**: This compiles the docs. Most useful for the docs deployment automation. Other modes are more useful to new contributors.
  ```
  MODE="BUILD" PACKAGE=datajoint UPSTREAM_REPO=https://github.com/datajoint/datajoint-python.git HOST_UID=$(id -u) docker compose -f docs/docker-compose.yaml up --build
  ```

When the docs are served locally, use the VSCode `PORTS` tab (next to `TERMINAL`) to manage access to the forwarded ports. Docs are served on port `8080`.
