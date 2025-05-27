FROM python:3

WORKDIR /main
COPY ./docs/pip_requirements.txt /main/docs/pip_requirements.txt
COPY ./datajoint /main/datajoint/
COPY ./pyproject.toml /main/pyproject.toml

RUN \
    # Install docs dependencies
    pip install --no-cache-dir -r /main/docs/pip_requirements.txt && \
    # Install datajoint
    pip install --no-cache-dir -e /main/

# Install dependencies first and use docker cache
# modify docs content won't cause image rebuild
COPY ./docs/ /main/docs/
