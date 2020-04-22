FROM datajoint/pydev

ADD . /src
RUN pip install --user /src && \
    rm -rf /src
