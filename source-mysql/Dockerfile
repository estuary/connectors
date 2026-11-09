ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM ${BASE_IMAGE_GO} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                  ./go
COPY source-boilerplate  ./source-boilerplate
COPY sqlcapture          ./sqlcapture
COPY source-mysql        ./source-mysql

# Run the unit tests. To skip tests which access a test database, specify `--build-arg TEST_DATABASE=no` in the Docker command line.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
ENV CI_BUILD=yes
RUN go test -short -failfast ./source-mysql/...

# Build the connector.
RUN go build -o ./connector -v ./source-mysql/...


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel
COPY --from=builder /builder/connector ./source-mysql

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-mysql"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
