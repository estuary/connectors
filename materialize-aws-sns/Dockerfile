ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

FROM --platform=linux/amd64 golang:1.21-bullseye as builder

WORKDIR /builder

# Download & cache dependencies.
COPY go.* ./
RUN go mod download

# Copy in the latest sources and build.
COPY ./ ./
RUN go build -o ./connector -v ./materialize-aws-sns/cmd/connector

FROM ${BASE_IMAGE}

LABEL FLOW_RUNTIME_PROTOCOL=materialize

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector /connector

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector"]

