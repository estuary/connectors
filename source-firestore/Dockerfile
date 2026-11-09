ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-firestore   ./source-firestore

COPY source-firestore ./source-firestore

# Run the unit tests.
RUN go test -v ./source-firestore/...

# Build the connector.
RUN go build -o ./connector -v ./source-firestore/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-firestore

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-firestore"]
