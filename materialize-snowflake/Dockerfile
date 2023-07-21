ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.20-bullseye as builder

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
RUN go test  -tags nozstd -v ./materialize-snowflake/...
RUN go build -tags nozstd -v -o ./connector ./materialize-snowflake/...

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

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-snowflake"]
