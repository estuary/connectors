ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-kinesis     ./source-kinesis

# Run automated tests. Integration tests that require LocalStack/AWS are gated behind
# `testing.Short()` so they are skipped here; they run via the integration test harness which
# provides LocalStack. To force-run them in the build (e.g. against real AWS), drop `-short`
# and set `--build-arg TEST_DATABASE=yes`.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
RUN go test -short -v ./source-kinesis/...

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
