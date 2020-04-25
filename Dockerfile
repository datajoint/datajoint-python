FROM datajoint/pydev

COPY --chown=dja . /tmp/src
RUN pip install --user /tmp/src && \
    rm -rf /tmp/src
