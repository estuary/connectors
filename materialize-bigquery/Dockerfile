ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.19-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Download recent flow binaries for usage by tests.
RUN curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz" | tar -zx -C /usr/local/bin/

# Build the connector projects we depend on.
COPY go-schema-gen           ./go-schema-gen
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-bigquery    ./materialize-bigquery
COPY materialize-sql         ./materialize-sql
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-bigquery/...
RUN go build -tags nozstd -v -o ./connector ./materialize-bigquery/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-bigquery

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-bigquery"]
