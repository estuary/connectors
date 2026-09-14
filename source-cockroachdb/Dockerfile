ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM golang:1.25-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY sqlcapture         ./sqlcapture
COPY source-cockroachdb ./source-cockroachdb

ENV PATH="/builder/bin:$PATH"

# The capture/replication tests use the black-box harness, which drives `flowctl` against a live
# CockroachDB and so can't run inside the build. They're gated on ${TEST_DATABASE}; the default of
# "no" runs only the non-database tests (config validation) here, while CI can run the full suite
# via run_tests.sh against the docker-compose database.
ARG TEST_DATABASE=no
ENV TEST_DATABASE=$TEST_DATABASE
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go test -short -failfast ./source-cockroachdb/...

# Build the connector.
RUN --mount=type=cache,id=gomod,target=/go/pkg/mod \
    --mount=type=cache,id=gobuild,target=/root/.cache/go-build \
    go build -o ./connector -v ./source-cockroachdb/...


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifacts from the builder.
COPY --from=builder /builder/connector ./source-cockroachdb
COPY --from=builder /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-cockroachdb"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
