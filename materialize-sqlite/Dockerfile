ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.19-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Download recent flow binaries for usage by tests.
RUN curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz"| tar -zx -C /usr/local/bin/

# Build the connector projects we depend on.
COPY go-schema-gen           ./go-schema-gen
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-sqlite      ./materialize-sqlite
COPY materialize-sql         ./materialize-sql

# Test and build the connector.
RUN go test  -tags nozstd,nodb -v ./materialize-sqlite/...
RUN go build -tags nozstd -v -o ./connector ./materialize-sqlite/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

RUN DEBIAN_FRONTEND=noninteractive apt update && apt install sqlite3 -y --no-install-recommends

COPY --from=builder /builder/connector ./materialize-sqlite

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-sqlite"]
