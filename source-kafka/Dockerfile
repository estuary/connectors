ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.24-bullseye AS builder

RUN apt-get update \
    && apt-get install -y curl ca-certificates pkg-config cmake g++ libssl-dev libsasl2-dev libclang-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --default-toolchain 1.89
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup component add clippy

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
    && cargo test --release --locked --offline \
    && cargo clippy --locked --offline --no-deps \
    && cargo build --release --locked --offline

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
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssl.so.1* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcrypto.so.1* /usr/lib/x86_64-linux-gnu/

# Copy in the connector artifact.
COPY --from=builder /connector/target/release/source-kafka ./

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-kafka"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL FLOW_RUNTIME_CODEC=json
LABEL CONNECTOR_PROTOCOL=flow-capture
