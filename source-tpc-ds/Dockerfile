ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
# Patched TPC-DS generator from https://github.com/estuary/tpcds-kit, pinned by commit.
ARG DSDGEN_IMAGE=ghcr.io/estuary/dsdgen:e73c527

FROM ${DSDGEN_IMAGE} AS dsdgen

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
COPY source-tpc-ds      ./source-tpc-ds

# The tests drive the real generator.
COPY --from=dsdgen /dsdgen /tpcds.idx /usr/local/bin/

# Run the unit tests.
RUN go test -v ./source-tpc-ds/...

# Build the connector.
RUN go build -o ./connector -v ./source-tpc-ds


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# The generator and its distributions file sit next to the connector, on PATH.
COPY --from=dsdgen /dsdgen /tpcds.idx ./

COPY --from=builder /builder/connector ./source-tpc-ds

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL dev.estuary.usage-rate=0

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-tpc-ds"]
