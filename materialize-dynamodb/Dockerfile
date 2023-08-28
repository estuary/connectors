ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.21-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

COPY go                        ./go
COPY materialize-boilerplate   ./materialize-boilerplate
COPY materialize-dynamodb      ./materialize-dynamodb

# Run automated tests. To skip tests which interact with an external database,
# specify `--build-arg TEST_DATABASE=no` in the Docker command line.
ARG TEST_DATABASE=yes
ENV TEST_DATABASE=$TEST_DATABASE
RUN go test -tags nozstd -v ./materialize-boilerplate/...
RUN go test -tags nozstd -v ./materialize-dynamodb/...

# Build the connector.
RUN go build -tags nozstd -v -o ./connector ./materialize-dynamodb

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector/materialize-dynamodb

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-dynamodb"]
