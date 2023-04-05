ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.19-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY filesource ./filesource
COPY source-http-file ./source-http-file

# Run the unit tests.
RUN go test -v ./filesource/...
RUN go test -v ./source-http-file/...

# Build the connector.
RUN go build -o ./connector -v ./source-http-file/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-http-file

# Avoid running the connector as root.
USER nonroot:nonroot

COPY --from=ghcr.io/estuary/airbyte-to-flow:eb2f96a /airbyte-to-flow ./
ENTRYPOINT ["/connector/airbyte-to-flow", "--connector-entrypoint", "/connector/source-http-file"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL FLOW_RUNTIME_CODEC=json
