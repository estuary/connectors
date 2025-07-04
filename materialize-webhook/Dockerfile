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
COPY materialize-webhook     ./materialize-webhook

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-webhook/...
RUN go build -tags nozstd -v -o ./connector ./materialize-webhook/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-webhook

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-webhook"]
