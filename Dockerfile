FROM python:3.7.4-alpine3.10 as base

RUN apk add --no-cache bash git \
    && rm -rf /var/cache/apk/*

WORKDIR /flywheel/v0
COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt \
    && pip3 install --no-deps dicomweb-client

COPY . .

ENTRYPOINT ["/flywheel/v0/run.py"]

FROM base as testing
RUN pip install -r tests/requirements.txt
