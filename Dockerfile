FROM eywalker/pydev

MAINTAINER Fabian Sinz <sinz@bcm.edu>

RUN pip install git+https://github.com/datajoint/datajoint-python.git

RUN pip install git+https://github.com/datajoint/datajoint-addons.git

