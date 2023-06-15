ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM rust:1.69-slim-buster as builder

RUN rustup component add clippy

RUN apt-get update \
    && apt-get install -y ca-certificates pkg-config cmake g++ libssl-dev libsasl2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /connector

ENV CARGO_HOME /connector/cargo
ENV CARGO_TARGET_DIR /connector/target

RUN mkdir /connector/src/
RUN touch /connector/src/lib.rs
RUN echo "fn main() {}" > /connector/src/main.rs

COPY source-kafka/Cargo.* /connector/

RUN cargo build --release \
    && cargo test --release

RUN rm -r /connector/src

COPY source-kafka/src /connector/src

RUN touch /connector/src/main.rs \
    && touch /connector/src/lib.rs \
    && cargo build --offline --release \
    && cargo test --release \
    && cargo clippy --release

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
COPY --from=builder /connector/target/release/source-kafka ./source-kafka

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-kafka"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
LABEL FLOW_RUNTIME_CODEC=json
