FROM python:3-alpine

WORKDIR /main
COPY mkdocs.yaml mkdocs.yaml
COPY src/ src/
COPY pip_requirements.txt pip_requirements.txt

ARG BOT_PAT
RUN \
    apk add --no-cache git && \
    pip install --no-cache-dir -r /main/pip_requirements.txt
    #&& \
    #pip install --no-cache git+https://${BOT_PAT}@github.com/datajoint/mkdocs-material-insiders.git@master
