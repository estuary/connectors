ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-alpaca      ./source-alpaca

# Run the unit tests.
RUN go test -v ./source-alpaca/...

# Build the connector.
RUN go build -o ./connector -v ./source-alpaca/...

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v2

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-alpaca

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-alpaca"]
