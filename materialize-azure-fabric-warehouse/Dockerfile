ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO_NOZSTD=ghcr.io/estuary/base-image-go:dev-nozstd

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO_NOZSTD} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                                   ./go
COPY materialize-boilerplate              ./materialize-boilerplate
COPY materialize-azure-fabric-warehouse   ./materialize-azure-fabric-warehouse
COPY materialize-sql                      ./materialize-sql

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql/...
RUN go test  -tags nozstd -short -v ./materialize-azure-fabric-warehouse/...
RUN go build -tags nozstd -v -o ./connector ./materialize-azure-fabric-warehouse/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-azure-fabric-warehouse

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-azure-fabric-warehouse"]
