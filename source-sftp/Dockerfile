ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go           ./go
COPY filesource   ./filesource
COPY source-boilerplate ./source-boilerplate
COPY source-sftp  ./source-sftp

RUN go test -v ./filesource/...
RUN go test -v ./source-sftp/...

# Build the connector.
RUN go build -o ./connector -v ./source-sftp/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-sftp

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-sftp"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL dev.estuary.usage-rate=0.33
