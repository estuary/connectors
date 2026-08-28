ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM golang:1.25-trixie AS builder

RUN apt-get update \
    && apt-get install -y curl ca-certificates pkg-config cmake g++ protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --default-toolchain 1.91
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup component add clippy

WORKDIR /connector

COPY source-http-ingest/Cargo.* /connector/

# Avoid having to install/build all dependencies by copying the Cargo files and
# making a dummy src/main.rs and empty lib.rs files.
#
# Keep these the same invocations, in the same profile, as the build below:
# cargo keys artifacts on both, so priming anything else just compiles every
# dependency twice. Release is the cheaper profile to share -- `cargo test
# --release` reuses all but the dev-dependencies of the release build, and
# clippy then compiles nothing.
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && touch src/lib.rs \
    && cargo test --release --locked --no-run \
    && cargo clippy --release --locked --no-deps \
    && cargo build --release --locked \
    && rm -r src

COPY source-http-ingest/src ./src

# This touch prevents Docker from using a cached empty main.rs file.
RUN touch src/main.rs \
    && touch src/lib.rs \
    && cargo test --release --locked --offline \
    && cargo clippy --release --locked --offline --no-deps \
    && cargo build --release --locked --offline


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Copy in the shared library files we linked against.

# Copy in the connector artifact.
COPY --from=builder /connector/target/release/source-http-ingest ./

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-http-ingest"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL FLOW_RUNTIME_CODEC=json
LABEL CONNECTOR_PROTOCOL=flow-capture

EXPOSE 8080
LABEL dev.estuary.port-public.8080=true

