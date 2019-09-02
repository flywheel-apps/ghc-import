FROM python:3.7.4-alpine3.10

RUN apk add --no-cache bash git

WORKDIR /flywheel/v0
COPY Pipfile .
COPY Pipfile.lock .
RUN pip install pipenv \
    && pipenv install --deploy --system \
    && pip install --no-deps dicomweb-client

COPY . .

CMD ["/flywheel/v0/run.py"]
