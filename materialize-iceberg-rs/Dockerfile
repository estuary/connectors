ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM debian:trixie-slim AS builder

RUN apt-get update \
    && apt-get install -y curl ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y --default-toolchain 1.97
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup component add clippy

WORKDIR /connector

COPY materialize-iceberg-rs/Cargo.* /connector/

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

COPY materialize-iceberg-rs/src ./src

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

# Copy in the connector artifact.
COPY --from=builder /connector/target/release/materialize-iceberg-rs ./

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/materialize-iceberg-rs"]

LABEL FLOW_RUNTIME_PROTOCOL=materialize

EXPOSE 8080
LABEL dev.estuary.port-public.8080=true
