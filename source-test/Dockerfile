ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM golang:1.19-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY go-schema-gen ./go-schema-gen
COPY source-boilerplate ./source-boilerplate
COPY source-test ./source-test

# Run the unit tests.
RUN go test -v ./source-test/...

# Build the connector.
RUN go build -o ./connector -v ./source-test/...


# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-test

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-test"]

LABEL FLOW_RUNTIME_PROTOCOL=capture
ENV FLOW_RUNTIME_CODEC=json