# Developer Guide

## Table of Contents

- [Contribute to DataJoint Python Documentation](#contribute-to-datajoint-python-documentation)
- [Setup Development Environment](#setup-development-environment)
  - [Prerequisites](#prerequisites)
  - [With Virtual Environment](#with-virtual-environment)
  - [With DevContainer](#with-devcontainer)
  - [Extra Efficiency, Optional But Recommended](#extra-efficiency-optional-but-recommended)
    - [Pre-commit Hooks](#pre-commit-hooks)
    - [Integration Tests](#integration-tests)
    - [VSCode](#vscode)
      - [Jupyter Extension](#jupyter-extension)
      - [Debugger](#debugger)
    - [MySQL CLI](#mysql-cli)

## Contribute to DataJoint Python Documentation

> Contributions to documentations are equivalently important to any code for the community, please help us to resolve any confusions in documentations.

[Here](https://github.com/datajoint/datajoint-python/blob/master/docs/README.md) is the instructions for contributing documentations, or you can find the same instructions at `$PROJECT_DIR/docs/README.md` in the repository.

[Back to top](#table-of-contents)

## Setup Development Environment

> We have [DevContainer](https://containers.dev/) ready for contributors to develop without setting up their environment. If you are familiar with DevContainer, Docker or Github Codespace, this is the recommended development environment for you.
> If you have never used Docker, it might be easier for you to use a virtual environment through `conda/mamba/venv`, it is also very straightforward to set up.

### Prerequisites

- Clone datajoint-python repository

```bash
# If you have your SSH key set up with GitHub, you can clone using SSH
git clone git@github.com:datajoint/datajoint-python.git
# Otherwise, you can clone using HTTPS
git clone https://github.com/datajoint/datajoint-python.git
```
- If you don't use DevContainer, then either install Anaconda/[Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/install)/Mamba, or just use Python's built-in `venv` module without install anything else.

### With Virtual Environment

```bash
# Check if you have Python 3.10 or higher, if not please upgrade
python --version
# Create a virtual environment with venv
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]

# Or create a virtual environment with conda
conda create -n dj python=3.13 # any 3.10+ is fine
conda activate dj
pip install -e .[dev]
```

[Back to top](#table-of-contents)

### With DevContainer

#### Launch Environment

Here are some options that provide a great developer experience:

- **Cloud-based IDE**: (*recommended*)
  - Launch using [GitHub Codespaces](https://github.com/features/codespaces) using the option `Create codespace on master` in the codebase repository on your fork.
  - Build time for a 2-Core codespace is **~6m**. This is done infrequently and cached for convenience.
  - Start time for a 2-Core codespace is **~2m**. This will pull the built codespace from cache when you need it.
  - *Tip*: GitHub auto names the codespace but you can rename the codespace so that it is easier to identify later.
- **Local IDE (VSCode - Dev Containers)**:
  - Ensure you have [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
  - Ensure you have [Docker](https://docs.docker.com/get-docker/)
  - Ensure you have [VSCode](https://code.visualstudio.com/)
  - Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
  - `git clone` the codebase repository and open it in VSCode
  - Use the `Dev Containers extension` to `Reopen in Container` (More info in the `Getting started` included with the extension)
  - You will know your environment has finished loading once you see a terminal open related to `Running postStartCommand` with a final message: `Done. Press any key to close the terminal.`.
- **Local IDE (Docker Compose)**:
  - Ensure you have [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
  - Ensure you have [Docker](https://docs.docker.com/get-docker/)
  - `git clone` the codebase repository and open it in VSCode
  - Issue the following command in the terminal to build and run the Docker container: `HOST_UID=$(id -u) PY_VER=3.11 DJ_VERSION=$(grep -oP '\d+\.\d+\.\d+' src/datajoint/version.py) docker compose --profile test run --rm -it djtest -- sh -c 'pip install -qe ".[dev]" && bash'`
  - Issue the following command in the terminal to stop the Docker compose stack: `docker compose --profile test down`

[Back to top](#table-of-contents)

## Extra Efficiency, Optional But Recommended

### Pre-commit Hooks

We recommend using [pre-commit](https://pre-commit.com/) to automatically run linters and formatters on your code before committing.
To set up pre-commit, run the following command in your terminal:

```bash
pip install pre-commit
pre-commit install
```

You can manually run pre-commit on all files with the following command:

```bash
pre-commit run --all-files
```
This will run all the linters and formatters specified in the `.pre-commit-config.yaml` file. If all check passed, you can commit your code. Otherwise, you need to fix the failed checks and run the command again.

> Pre-commit will automatically run the linters and formatters on all staged files before committing. However, if your code doesn't follow the linters and formatters, the commit will fail.
> Some hooks will automatically fix your problem, and add the fixed files as git's `unstaged` files, you just need to add them(`git add .`) to git's `staged` files and commit again.
> Some hooks will not automatically fix your problem, so you need to check the pre-commit failed log to fix them manually and include the update to your `staged` files and commit again.

If you really don't want to use pre-commit, or if you don't like it, you can uninstall it with the following command:

```bash
pre-commit uninstall
```

But when you issue a pull request, the same linter and formatter check will run against your contribution, you are going to have the same failure as well. So without pre-commit, you need to **manually run these linters and formatters before committing your code**:

- Syntax tests

The following will verify that there are no syntax errors.

```
flake8 datajoint --count --select=E9,F63,F7,F82 --show-source --statistics
```

- Style tests

The following will verify that there are no code styling errors.

```
flake8 --ignore=E203,E722,W503 datajoint --count --max-complexity=62 --max-line-length=127 --statistics
```

The following will ensure the codebase has been formatted with [black](https://black.readthedocs.io/en/stable/).

```
black datajoint --check -v --diff
```

The following will ensure the test suite has been formatted with [black](https://black.readthedocs.io/en/stable/).

```
black tests --check -v --diff
```

[Back to top](#table-of-contents)

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

[Back to top](#table-of-contents)

### VSCode

#### Jupyter Extension

Be sure to go through this documentation if you are new to [Running Jupyter Notebooks with VSCode](https://code.visualstudio.com/docs/datascience/jupyter-notebooks#_create-or-open-a-jupyter-notebook).

#### Debugger

[VSCode Debugger](https://code.visualstudio.com/docs/editor/debugging) is a powerful tool that can really accelerate fixes.

Try it as follows:

- Create a python script of your choice
- `import datajoint` (This will use the current state of the source)
- Add breakpoints by adding red dots next to line numbers
- Select the `Run and Debug` tab
- Start by clicking the button `Run and Debug`

[Back to top](#table-of-contents)

### MySQL CLI

> Installation instruction is in [here](https://dev.mysql.com/doc/mysql-shell/8.0/en/mysql-shell-install.html)

It is often useful in development to connect to DataJoint's relational database backend directly using the MySQL CLI.

Connect as follows to the database running within your developer environment:

```
mysql -hdb -uroot -ppassword
```

[Back to top](#table-of-contents)