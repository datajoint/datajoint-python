ARG PY_VER=3.9
ARG IMAGE=jupyter/docker-stacks-foundation
FROM ${IMAGE}
USER jovyan
RUN conda install -y -n base -c conda-forge python=${PY_VER} pydot networkx && \
    conda clean -afy
COPY --chown=1000:100 ./pyproject.toml ./README.md ./LICENSE.txt ./datajoint.pub /main/
COPY --chown=1000:100 ./datajoint /main/datajoint
RUN \
    pip install --no-cache-dir /main && \
    rm -r /main/*
