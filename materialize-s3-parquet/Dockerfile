ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.21-bullseye as builder

WORKDIR /builder

COPY go.* ./
RUN go mod download

COPY go                         ./go
COPY filesink                   ./filesink
COPY materialize-boilerplate    ./materialize-boilerplate
COPY materialize-s3-parquet    ./materialize-s3-parquet

RUN go test -v ./filesink/...
RUN go test -v ./materialize-boilerplate/...
RUN go test -v ./materialize-s3-parquet/...

RUN go build -o ./connector -v ./materialize-s3-parquet

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector /connector/materialize-s3-parquet

USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-s3-parquet"]
