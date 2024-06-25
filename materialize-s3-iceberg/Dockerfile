FROM python:3.12-slim-bookworm as base

# Python build stage
################################################################################
FROM base as pybuilder

RUN apt-get update && \
    apt install -y --no-install-recommends \
    python3-poetry

RUN python -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv

WORKDIR /opt/iceberg-ctl

COPY materialize-s3-iceberg/iceberg-ctl /opt/iceberg-ctl

RUN poetry install

# Go build stage
################################################################################
FROM --platform=linux/amd64 golang:1.21-bullseye as gobuilder

WORKDIR /builder

COPY go.*                       ./
COPY go                         ./go
COPY filesink                   ./filesink
COPY materialize-boilerplate    ./materialize-boilerplate
COPY materialize-sql            ./materialize-sql
COPY materialize-s3-iceberg     ./materialize-s3-iceberg

RUN go test -v ./filesink/...
RUN go test -v ./materialize-sql/...
RUN go test -v ./materialize-boilerplate/...
RUN go test -v ./materialize-s3-iceberg/...

RUN go build -o ./connector -v ./materialize-s3-iceberg

# Runtime stage
################################################################################

FROM base as runner

COPY --from=pybuilder /opt/venv /opt/venv
COPY --from=pybuilder /opt/iceberg-ctl /opt/iceberg-ctl
COPY --from=gobuilder /builder/connector /opt/materialize-s3-iceberg

ENV GOMEMLIMIT=900MiB
ENV PYTHON_PATH=/opt/venv/bin/python

LABEL FLOW_RUNTIME_PROTOCOL=materialize

WORKDIR /opt

ENTRYPOINT ["/opt/materialize-s3-iceberg"]
