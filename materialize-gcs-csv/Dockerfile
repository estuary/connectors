ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                         ./go
COPY filesink                   ./filesink
COPY materialize-boilerplate    ./materialize-boilerplate
COPY materialize-gcs-csv    ./materialize-gcs-csv

RUN go test -v ./filesink/...
RUN go test -v ./materialize-boilerplate/...
RUN go test -short -v ./materialize-gcs-csv/...

RUN go build -o ./connector -v ./materialize-gcs-csv

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector /connector/materialize-gcs-csv

USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-gcs-csv"]
