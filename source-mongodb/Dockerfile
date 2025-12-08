# Rust Build Stage
################################################################################
FROM rust:1-slim-bookworm AS rust-builder

WORKDIR /builder

COPY bson-transcoder ./bson-transcoder

RUN cargo build --release --manifest-path bson-transcoder/Cargo.toml

# Go Build Stage
################################################################################
FROM golang:1.25-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-mongodb     ./source-mongodb

# Run the unit tests.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
COPY --from=rust-builder /builder/bson-transcoder/target/release/bson-transcoder /usr/bin/bson-transcoder
RUN go test -short -v ./source-mongodb/...

# Build the connector.
RUN go build -o ./connector -v ./source-mongodb/...

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-mongodb
COPY --from=rust-builder /builder/bson-transcoder/target/release/bson-transcoder /usr/bin/bson-transcoder
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-mongodb"]
