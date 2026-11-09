ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-hello-world ./source-hello-world

# Run the unit tests.
RUN go test -v ./source-hello-world/...

# Build the connector.
RUN go build -o ./connector -v ./source-hello-world


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-hello-world

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL dev.estuary.usage-rate=0

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-hello-world"]
