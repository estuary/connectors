ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.27-trixie AS builder

WORKDIR /builder

COPY go.* ./
RUN go mod download

COPY go                         ./go
COPY filesink                   ./filesink
COPY materialize-boilerplate    ./materialize-boilerplate
COPY materialize-smb            ./materialize-smb

RUN go test -v ./filesink/...
RUN go test -v ./materialize-boilerplate/...
RUN go test -short -v ./materialize-smb/...

RUN go build -o ./connector -v ./materialize-smb

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector /connector/materialize-smb

USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-smb"]
