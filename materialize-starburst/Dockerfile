ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.24-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-starburst   ./materialize-starburst
COPY materialize-sql         ./materialize-sql
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test -tags nozstd -v      ./materialize-sql/...
RUN go test -tags nozstd,nodb -v ./materialize-starburst/...
RUN go build -tags nozstd -v -o ./connector ./materialize-starburst/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./materialize-starburst
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-starburst"]
