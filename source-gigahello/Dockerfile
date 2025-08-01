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
COPY source-gigahello ./source-gigahello

# Run the unit tests.
RUN go test -v ./source-gigahello/...

# Build the connector.
RUN go build -o ./connector -v ./source-gigahello


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-gigahello

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL dev.estuary.usage-rate=0

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-gigahello"]
