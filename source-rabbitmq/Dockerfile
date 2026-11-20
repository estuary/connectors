ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.27-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-rabbitmq    ./source-rabbitmq

# Live RabbitMQ tests require TEST_DATABASE=yes and are skipped in short mode.
RUN go test -short -v ./source-rabbitmq/...

# Build the connector.
RUN go build -o ./connector -v ./source-rabbitmq/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-rabbitmq

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-rabbitmq"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
