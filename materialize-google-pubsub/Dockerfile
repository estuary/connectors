ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.20-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                        ./go
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-google-pubsub ./materialize-google-pubsub

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-google-pubsub/...
RUN go build -tags nozstd -v -o ./connector ./materialize-google-pubsub/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector/materialize-google-pubsub

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-google-pubsub"]
