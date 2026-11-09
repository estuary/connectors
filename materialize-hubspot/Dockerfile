ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO=ghcr.io/estuary/base-image-go:dev

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO}-nozstd AS builder

WORKDIR /builder

COPY go.* ./

COPY go                        ./go
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-hubspot       ./materialize-hubspot

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-boilerplate/...
RUN go test  -tags nozstd -v -short ./materialize-hubspot/...
RUN go build -tags nozstd -v -o ./connector ./materialize-hubspot/cmd/connector
RUN go build -tags nozstd -v -o ./hubspot-cli ./materialize-hubspot/cmd/hubspot-cli

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector/materialize-hubspot

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-hubspot"]
