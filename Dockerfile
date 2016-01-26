FROM ubuntu:14.04

MAINTAINER Fabian Sinz <sinz@bcm.edu>


RUN \
  apt-get update && \
  apt-get install -y -q \
    git

RUN \
  apt-get update && \
  apt-get install -y -q \
    python3 \
    python3-pip \
    python3-numpy \
    python3-scipy \
    python3-matplotlib \
    python3-pandas \
    ipython3 \
    ipython3-notebook \
    mysql-server \
    mysql-client

RUN pip3 install git+https://github.com/datajoint/datajoint-python.git

RUN pip3 install git+https://github.com/datajoint/datajoint-addons.git

