FROM datajoint/pydev

ADD . /src
RUN pip install /src && \
    rm -rf /src
