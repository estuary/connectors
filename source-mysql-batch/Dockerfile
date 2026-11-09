ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate

RUN go install -v ./source-boilerplate/...

# Run tests and build the connector
COPY source-mysql-batch ./source-mysql-batch
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
RUN go test -short -failfast ./source-mysql-batch/...
RUN go build -o ./connector -v ./source-mysql-batch/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-mysql-batch
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-mysql-batch"]
