ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.25-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate

RUN go install -v ./source-boilerplate/...

# Run tests and build the connector
COPY source-oracle-batch ./source-oracle-batch
RUN go test ./source-oracle-batch/...
RUN go build -o ./connector -v ./source-oracle-batch/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-oracle-batch
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-oracle-batch"]
