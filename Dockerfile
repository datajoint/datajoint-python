FROM datajoint/djbase

COPY --chown=anaconda:anaconda . /tmp/src
RUN pip install --no-cache-dir /tmp/src && \
    rm -rf /tmp/src
