ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO} AS builder

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
RUN go test -short -v ./materialize-s3-iceberg/...

RUN go build -o ./connector -v ./materialize-s3-iceberg/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     ca-certificates \
     && rm -rf /var/lib/apt/lists/*

USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-s3-iceberg

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-s3-iceberg"]
