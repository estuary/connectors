ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2
ARG BASE_IMAGE_GO_NOZSTD=ghcr.io/estuary/base-image-go:dev-nozstd

# Build Stage
################################################################################
FROM --platform=linux/amd64 ${BASE_IMAGE_GO_NOZSTD} AS builder

WORKDIR /builder

COPY go.* ./

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-sns         ./materialize-sns

# Test and build the connector.
RUN go test  -tags nozstd -short -v ./materialize-sns/...
RUN go build -tags nozstd -v -o ./connector ./materialize-sns/cmd/connector

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector/materialize-sns

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-sns"]
