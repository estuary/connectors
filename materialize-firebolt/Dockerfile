ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.21-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy schema-builder binary.
COPY flow-bin/flow-schemalate ./

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-firebolt    ./materialize-firebolt

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-firebolt/...
RUN go build -tags nozstd -v -o ./connector ./materialize-firebolt

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector/materialize-firebolt
COPY --from=builder /builder/flow-schemalate /connector/
COPY --from=builder /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-firebolt"]
