ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:alpine as builder

WORKDIR /builder

# Install azurite
RUN apk update
RUN apk add --update npm
RUN npm install -g azurite

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN go mod download

COPY go         ./go
COPY filesource ./filesource
COPY source-boilerplate ./source-boilerplate
COPY source-azure-blob-storage  ./source-azure-blob-storage

# Run the unit tests.
RUN azurite-blob --blobHost 127.0.0.1 --blobPort 10000 --location /tester/source-azure-blob-storage/.azurite &
RUN go test -v ./filesource/...
RUN go test -v ./source-azure-blob-storage/...

# Build the connector.
RUN go build -o ./connector -v ./source-azure-blob-storage/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-azure-blob-storage

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-azure-blob-storage"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture
