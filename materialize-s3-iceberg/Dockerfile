ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.25-bookworm AS builder

WORKDIR /builder

COPY go.*                       ./
RUN go mod download

COPY go                         ./go
COPY filesink                   ./filesink
COPY materialize-boilerplate    ./materialize-boilerplate
COPY materialize-sql            ./materialize-sql
COPY materialize-s3-iceberg     ./materialize-s3-iceberg

RUN go test -v ./filesink/...
RUN go test -v ./materialize-sql/...
RUN go test -v ./materialize-boilerplate/...
RUN go test -short -v ./materialize-s3-iceberg/...

RUN go build -o ./connector -v ./materialize-s3-iceberg

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

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-s3-iceberg"]
