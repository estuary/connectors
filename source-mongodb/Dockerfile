# Build Stage
################################################################################
FROM golang:1.20-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY source-mongodb     ./source-mongodb

# Run the unit tests.
RUN go test -v ./source-mongodb/...

# Build the connector.
RUN go build -o ./connector -v ./source-mongodb/...

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-mongodb

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-mongodb"]
