ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.24-bullseye AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-bigquery    ./materialize-bigquery
COPY materialize-sql-v2      ./materialize-sql-v2
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql-v2/...
RUN go test  -tags nozstd -v ./materialize-bigquery/...
RUN go build -tags nozstd -v -o ./connector ./materialize-bigquery/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-bigquery

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-bigquery"]
