# Note: You can use any Debian/Ubuntu based image you want. 
FROM mcr.microsoft.com/devcontainers/python:3.7-bullseye

RUN \
    apt update && \
    apt-get install bash-completion graphviz default-mysql-client -y && \
    pip install flake8 black faker ipykernel pytest pytest-cov nose nose-cov datajoint && \
    pip uninstall datajoint -y

ENV DJ_HOST fakeservices.datajoint.io
ENV DJ_USER root
ENV DJ_PASS simple