FROM eywalker/pydev

MAINTAINER Fabian Sinz <sinz@bcm.edu>


RUN \
  apt-get update && \
  apt-get install -y -q \
    git

RUN pip install git+https://github.com/datajoint/datajoint-python.git

RUN pip install git+https://github.com/datajoint/datajoint-addons.git

