ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.21-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go         ./go
COPY filesource ./filesource
COPY source-boilerplate ./source-boilerplate
COPY source-google-drive ./source-google-drive

# Run the unit tests.
RUN go test -v ./filesource/...
RUN go test -v ./source-google-drive/...

# Build the connector.
RUN go build -o ./connector -v ./source-google-drive/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-google-drive

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-google-drive"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
