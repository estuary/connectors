ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO}-nozstd AS builder

WORKDIR /builder

COPY go.* ./

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-spanner     ./materialize-spanner
COPY materialize-sql         ./materialize-sql

# Test and build the connector.
RUN go test -tags nozstd -v      ./materialize-sql/...
RUN go test -tags nozstd -short -v ./materialize-spanner/...
RUN go build -tags nozstd -v -o ./connector ./materialize-spanner/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-spanner

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-spanner"]
