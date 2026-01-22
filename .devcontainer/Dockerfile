ARG PY_VER
ARG DISTRO
FROM mcr.microsoft.com/devcontainers/python:${PY_VER}-${DISTRO}
RUN \
    apt update && \
    apt-get install bash-completion graphviz default-mysql-client -y && \
    pip install flake8 black faker ipykernel pytest pytest-cov nose nose-cov datajoint jupyterlab && \
    pip uninstall datajoint -y

USER root
ENV DJ_HOST=db
ENV DJ_USER=root
ENV DJ_PASS=password
ENV S3_ENDPOINT=minio:9000
