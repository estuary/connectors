ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM golang:1.27-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-sql         ./materialize-sql
COPY materialize-oracle      ./materialize-oracle

RUN go test -tags nozstd -short -v ./materialize-oracle/...
RUN go build -tags nozstd -v -o ./connector ./materialize-oracle/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-oracle

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-oracle"]
