ARG IMAGE
ARG PY_VER
ARG DISTRO
FROM datajoint/${IMAGE}:py${PY_VER}-${DISTRO}
COPY --chown=anaconda:anaconda ./setup.py ./datajoint.pub ./requirements.txt /main/
COPY --chown=anaconda:anaconda ./datajoint /main/datajoint
RUN \
    pip install --no-cache-dir /main && \
    rm -r /main/*
