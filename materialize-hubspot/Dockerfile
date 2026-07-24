ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.25-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

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

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-hubspot"]
