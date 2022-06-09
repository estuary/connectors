# Build Stage
################################################################################
FROM golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy in a recent `flowctl` for usage by tests.
COPY --from=ghcr.io/estuary/flow:dev /usr/local/bin/flowctl /usr/local/bin/flowctl

# Build the connector projects we depend on.
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-google-sheets ./materialize-google-sheets
COPY testsupport               ./testsupport
COPY go-schema-gen             ./go-schema-gen

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-google-sheets/...
RUN go build -tags nozstd -v -o ./connector ./materialize-google-sheets

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/connector"]
