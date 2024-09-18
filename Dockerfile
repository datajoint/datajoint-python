ARG PY_VER=3.9
ARG IMAGE=jupyter/docker-stacks-foundation
FROM ${IMAGE}
RUN conda install -y -n base -c conda-forge python=${PY_VER} git graphviz pydot && \
    conda clean -afy
USER 1000
VOLUME /src
WORKDIR /tmp
COPY --chown=1000:100 ./pyproject.toml ./README.md ./LICENSE.txt /main/
COPY --chown=1000:100 ./datajoint /main/datajoint
RUN \
    pip install --no-cache-dir /main && \
    rm -r /main/*
WORKDIR /src
