# Build Stage
################################################################################
FROM golang:1.19-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY schema_inference ./schema_inference
COPY source-boilerplate ./source-boilerplate
COPY go-schema-gen  ./go-schema-gen
RUN go get -v ./source-boilerplate ./schema_inference ./go-schema-gen

COPY source-firestore ./source-firestore

# Run the unit tests.
RUN go test -v ./source-firestore/...

# Build the connector.
RUN go build -o ./connector -v ./source-firestore/...

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Grab the statically-built parser cli.
COPY flow-bin/flow-parser ./
# Grab the statically-built schema-inference cli.
COPY flow-bin/flow-schema-inference ./

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-firestore

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-firestore"]
