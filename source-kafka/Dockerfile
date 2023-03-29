ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM rust:1.54-slim-buster as builder

RUN rustup component add clippy

RUN apt-get update \
 && apt-get install -y ca-certificates pkg-config cmake g++ libssl-dev libsasl2-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /connector

COPY source-kafka/Cargo.* /connector/

# Avoid having to install/build all dependencies by copying the Cargo files and
# making a dummy src/main.rs and empty lib.rs files.
RUN mkdir src \
 && echo "fn main() {}" > src/main.rs \
 && touch src/lib.rs \
 && cargo test --locked \
 && cargo build --release --locked \
 && rm -r src

COPY source-kafka/src ./src

# This touch prevents Docker from using a cached empty main.rs file.
RUN touch src/main.rs \
 && touch src/lib.rs \
 && cargo test --locked --offline \
 && cargo clippy --locked --offline \
 && cargo install --path . --locked --offline


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Copy in the shared library files we linked against.
COPY --from=builder /usr/lib/x86_64-linux-gnu/libdb-5.3.so* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/sasl2/libsasldb.so* /usr/lib/x86_64-linux-gnu/sasl2/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsasl2.so.2* /usr/lib/x86_64-linux-gnu/

# Copy in the connector artifact.
COPY --from=builder /usr/local/cargo/bin/source-kafka ./source-kafka

# Avoid running the connector as root.
USER nonroot:nonroot

COPY --from=ghcr.io/estuary/airbyte-to-flow:1fb6c12 /airbyte-to-flow ./
ENTRYPOINT ["/connector/airbyte-to-flow", "--connector-entrypoint", "/connector/source-kafka"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL FLOW_RUNTIME_CODEC=json
