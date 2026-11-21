ARG IMAGE=mambaorg/micromamba:1.5-bookworm-slim
FROM ${IMAGE}

ARG CONDA_BIN=micromamba
ARG PY_VER=3.11
ARG HOST_UID=1000
# The package version comes from the git tag (hatch-vcs), and the build context
# has no .git directory, so the version is given here. Set it to the release
# being built, e.g. --build-arg DJ_BUILD_VERSION=2.3.7. Only the datajoint
# wheel is built with it; dependencies are installed without it.
ARG DJ_BUILD_VERSION=0.0.0+docker

RUN ${CONDA_BIN} install --no-pin -qq -y -n base -c conda-forge \
        python=${PY_VER} pip setuptools git graphviz pydot && \
    ${CONDA_BIN} clean -qq -afy
ENV PATH="$PATH:/home/mambauser/.local/bin"

COPY --chown=${HOST_UID:-1000}:mambauser ./pyproject.toml ./README.md ./LICENSE /main/
COPY --chown=${HOST_UID:-1000}:mambauser ./src/datajoint /main/src/datajoint

VOLUME /src
WORKDIR /src
USER root
RUN \
    chown -R ${HOST_UID:-1000}:mambauser /main && \
    chown -R ${HOST_UID:-1000}:mambauser /src && \
    SETUPTOOLS_SCM_PRETEND_VERSION=${DJ_BUILD_VERSION} \
        ${CONDA_BIN} run -n base pip wheel -q --no-cache-dir --no-deps -w /tmp/dist /main && \
    ${CONDA_BIN} run -n base pip install -q --no-cache-dir /tmp/dist/datajoint-*.whl && \
    rm -r /main/* /tmp/dist
USER ${MAMBA_USER}
