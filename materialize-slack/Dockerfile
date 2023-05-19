ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-slack         ./materialize-slack
COPY go/schema-gen             ./go/schema-gen
COPY go/connector-errors       ./go/connector-errors

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-slack/...
RUN go build -tags nozstd -v -o ./connector ./materialize-slack

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/connector"]
