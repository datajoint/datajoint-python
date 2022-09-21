ARG PY_VER=3.8
ARG DISTRO=alpine
ARG IMAGE=djbase
FROM datajoint/${IMAGE}:py${PY_VER}-${DISTRO}
WORKDIR /main
COPY --chown=anaconda:anaconda ./requirements.txt ./setup.py \
    /main/
COPY --chown=anaconda:anaconda ./datajoint/*.py /main/datajoint/
RUN \
    umask u+rwx,g+rwx,o-rwx && \
    pip install --no-cache-dir . && \
    rm -R ./*
CMD ["python"]