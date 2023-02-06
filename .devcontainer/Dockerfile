# Note: You can use any Debian/Ubuntu based image you want. 
FROM mcr.microsoft.com/devcontainers/base:bullseye

RUN \
    apt update && \
    apt-get install bash-completion graphviz -y

ENV DJ_HOST fakeservices.datajoint.io
ENV DJ_USER root
ENV DJ_PASS simple