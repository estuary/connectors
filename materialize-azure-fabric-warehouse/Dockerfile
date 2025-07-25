ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.24-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                                   ./go
COPY materialize-boilerplate              ./materialize-boilerplate
COPY materialize-azure-fabric-warehouse   ./materialize-azure-fabric-warehouse
COPY materialize-sql                      ./materialize-sql

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql/...
RUN go test  -tags nozstd -v ./materialize-azure-fabric-warehouse/...
RUN go build -tags nozstd -v -o ./connector ./materialize-azure-fabric-warehouse/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-azure-fabric-warehouse

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-azure-fabric-warehouse"]
