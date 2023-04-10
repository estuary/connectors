ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.19-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy the helper packages we depend on and fetch dependencies.
COPY go-network-tunnel  ./go-network-tunnel
COPY go-schema-gen      ./go-schema-gen
COPY source-boilerplate ./source-boilerplate
COPY sqlcapture         ./sqlcapture
RUN go get -v ./go-network-tunnel ./go-schema-gen ./source-boilerplate ./sqlcapture

# Copy the connector code and fetch dependencies.
COPY source-mysql ./source-mysql
RUN go get -v ./source-mysql

# Run the unit tests. To skip tests which access a test database, specify `--build-arg TEST_DATABASE=no` in the Docker command line.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
ENV CI_BUILD=yes
RUN go test -v ./source-mysql/...

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

# Ask the Go runtime to keep heap size below 900MiB. Our real memory limit is
# 1GiB so this is 10% headroom as recommended in https://go.dev/doc/gc-guide
ENV GOMEMLIMIT=900MiB

ENTRYPOINT ["/connector/source-mysql"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
