ARG IMAGE=djbase
ARG PY_VER=3.9
ARG DISTRO=debian
FROM datajoint/${IMAGE}:py${PY_VER}-${DISTRO}
COPY --chown=anaconda:anaconda ./setup.py ./datajoint.pub ./requirements.txt /main/
COPY --chown=anaconda:anaconda ./datajoint /main/datajoint
RUN \
    pip install --no-cache-dir /main && \
    rm -r /main/*
