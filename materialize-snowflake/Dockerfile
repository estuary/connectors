ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.25-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-snowflake   ./materialize-snowflake
COPY materialize-sql         ./materialize-sql
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql/...
RUN go test  -tags nozstd -short -v ./materialize-snowflake/...
RUN go build -tags nozstd -v -o ./connector ./materialize-snowflake/...

# Python Sidecar Stage
################################################################################
# The Snowpipe Streaming sidecar venv is built on the same base image as the
# runtime stage so its interpreter and site-packages exactly match the python3
# installed there.
FROM ${BASE_IMAGE} AS pybuilder

USER root
RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     python3 \
     python3-venv \
     && rm -rf /var/lib/apt/lists/*

COPY materialize-snowflake/sidecar /sidecar
RUN python3 -m venv /opt/venv \
     && /opt/venv/bin/pip install --no-cache-dir /sidecar

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     ca-certificates \
     curl \
     unzip \
     python3 \
     && rm -rf /var/lib/apt/lists/*

# The Snowpipe Streaming (high-performance) python SDK sidecar.
COPY --from=pybuilder /opt/venv /opt/venv

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-snowflake

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-snowflake"]
