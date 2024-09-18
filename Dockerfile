ARG IMAGE=mambaorg/micromamba:1.5-bookworm-slim
FROM ${IMAGE}

ARG CONDA_BIN=micromamba
ARG PY_VER=3.9
ARG HOST_UID=1000

RUN ${CONDA_BIN} install --no-pin -qq -y -n base -c conda-forge \
        python=${PY_VER} pip setuptools git graphviz pydot && \
    ${CONDA_BIN} clean -qq -afy

COPY --chown=${HOST_UID:-1000}:mambauser ./pyproject.toml ./README.md ./LICENSE.txt /main/
COPY --chown=${HOST_UID:-1000}:mambauser ./datajoint /main/datajoint

VOLUME /src
WORKDIR /src
USER root
RUN \
    chown -R ${HOST_UID:-1000}:mambauser /main && \
    chown -R ${HOST_UID:-1000}:mambauser /src && \
    eval "$(micromamba shell hook --shell bash)" && \
    micromamba activate base && \
    pip install -q --no-cache-dir /main && \
    rm -r /main/*
USER ${MAMBA_USER}
ENV PATH="$PATH:/home/mambauser/.local/bin"
