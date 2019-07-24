FROM python:3.7.4-alpine3.10

RUN apk add --no-cache bash git

WORKDIR /flywheel/v0
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt \
    && pip3 install --no-deps dicomweb-client

COPY . .

CMD ["/flywheel/v0/run.py"]
