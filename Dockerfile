ARG IMAGE=mambaorg/micromamba:1.5-bookworm-slim
FROM ${IMAGE}

ARG CONDA_BIN=micromamba
ARG PY_VER=3.11
ARG HOST_UID=1000

RUN ${CONDA_BIN} install --no-pin -qq -y -n base -c conda-forge \
        python=${PY_VER} pip setuptools git graphviz pydot && \
    ${CONDA_BIN} clean -qq -afy
ENV PATH="$PATH:/home/mambauser/.local/bin"

COPY --chown=${HOST_UID:-1000}:mambauser ./pyproject.toml ./README.md ./LICENSE /main/
COPY --chown=${HOST_UID:-1000}:mambauser ./src/datajoint /main/src/datajoint

VOLUME /src
WORKDIR /src
USER root
# pymysql[rsa] adds cryptography, which PyMySQL needs to log in without TLS to a
# user with caching_sha2_password, the default authentication plugin of the
# official MySQL images in docker-compose.yaml. The interactive app service needs
# it; djtest also gets it from the test extra (testcontainers[mysql]).
RUN \
    chown -R ${HOST_UID:-1000}:mambauser /main && \
    chown -R ${HOST_UID:-1000}:mambauser /src && \
    ${CONDA_BIN} run -n base pip install -q --no-cache-dir /main 'pymysql[rsa]' && \
    rm -r /main/*
USER ${MAMBA_USER}
