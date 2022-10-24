# Build Stage
################################################################################
FROM golang:1.17-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy over helper packages that the connector uses and then build them and
# their transitive dependencies to aid in layer caching.
COPY go-schema-gen      ./go-schema-gen
COPY source-boilerplate ./source-boilerplate
RUN go get -v ./go-schema-gen ./source-boilerplate

# Copy and build and test the connector (there are currently no tests)
COPY source-nativehello ./source-nativehello
RUN go build -o ./connector -v ./source-nativehello
# RUN go test -v ./source-nativehello/...


# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=busybox:latest /bin/sh /bin/sh

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

COPY --from=ghcr.io/estuary/airbyte-to-flow:dev /airbyte-to-flow ./airbyte-to-flow
ENTRYPOINT ["/connector/airbyte-to-flow", "--connector-entrypoint", "/connector/connector"]
