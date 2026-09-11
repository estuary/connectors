ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.25-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY go                        ./go
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-slack         ./materialize-slack

# Test and build the connector.
RUN go test  -tags nozstd -short -v ./materialize-slack/...
RUN go build -tags nozstd -v -o ./connector ./materialize-slack

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-slack

# Avoid running the connector as root.
USER nonroot:nonroot

# The build this image carries, for the transaction health log line. CI
# passes "<VERSION>-<short sha>"; a local build reports "dev".
ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-slack"]
