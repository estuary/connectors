ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM golang:1.27-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go           ./go
COPY filesource   ./filesource
COPY source-boilerplate ./source-boilerplate
COPY source-smb   ./source-smb

# Run automated tests. To skip tests which interact with an external database,
# specify `--build-arg TEST_DATABASE=no` in the Docker command line.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
RUN go test -v ./go/smb/...
RUN go test -v ./filesource/...
RUN go test -v ./source-smb/...

# Build the connector.
RUN go build -o ./connector -v ./source-smb/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-smb

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-smb"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL dev.estuary.usage-rate=0.33
