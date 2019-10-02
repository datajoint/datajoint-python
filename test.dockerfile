ARG PY_VER
ARG ALPINE_VER
FROM raphaelguzman/pydev:${PY_VER}-alpine${ALPINE_VER}

ENV PYTHON_USER dja
RUN \
    /startup $(id -u) $(id -g) && \
    pip install --user nose nose-cov ptvsd