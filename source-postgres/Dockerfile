ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.17-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY source-postgres        ./source-postgres
COPY sqlcapture             ./sqlcapture
COPY go-schema-gen          ./go-schema-gen
COPY go-network-tunnel       ./go-network-tunnel

ENV PATH="/builder/bin:$PATH"
# Run the unit tests.
RUN go test -short -v ./source-postgres/...

# Build the connector.
RUN go build -o ./connector -v ./source-postgres/...


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifacts from the builder.
COPY --from=builder /builder/connector ./
COPY --from=builder /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

# Avoid running the connector as root.
USER nonroot:nonroot

COPY --from=ghcr.io/estuary/airbyte-to-flow:dev /airbyte-to-flow ./
ENTRYPOINT ["/connector/airbyte-to-flow", "--connector-entrypoint", "/connector/connector"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
