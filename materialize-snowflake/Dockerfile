ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO_NOZSTD=ghcr.io/estuary/base-image-go:dev-nozstd

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO_NOZSTD} AS builder

WORKDIR /builder

COPY go.* ./

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-snowflake   ./materialize-snowflake
COPY materialize-sql         ./materialize-sql

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql/...
RUN go test  -tags nozstd -short -v ./materialize-snowflake/...
RUN go build -tags nozstd -v -o ./connector ./materialize-snowflake/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     ca-certificates \
     curl \
     unzip \
     && rm -rf /var/lib/apt/lists/*

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-snowflake

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-snowflake"]
