ARG IMAGE=jupyter/docker-stacks-foundation
ARG PY_VER=3.9
ARG DISTRO=debian
FROM ${IMAGE}
RUN conda install -y -n base -c conda-forge python=${PY_VER} && \
    conda clean -afy
COPY --chown=anaconda:anaconda ./setup.py ./datajoint.pub ./requirements.txt /main/
COPY --chown=anaconda:anaconda ./datajoint /main/datajoint
RUN \
    pip install --no-cache-dir /main && \
    rm -r /main/*
