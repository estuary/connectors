ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.24-bullseye AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-kinesis     ./source-kinesis

# Run automated tests. To skip tests which interact with an external database (kinesis, in this
# case), specify `--build-arg TEST_DATABASE=no` in the Docker command line.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
RUN go test -v ./source-kinesis/...

# Build the connector.
RUN go build -o ./connector -v ./source-kinesis/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-kinesis

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-kinesis"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
