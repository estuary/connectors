# Build Stage
################################################################################
FROM golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-rockset     ./materialize-rockset

# Test and build the connector.
# RUN go test  -tags nozstd -v ./materialize-rockset/...
RUN go build -tags nozstd -v -o ./connector ./materialize-rockset/cmd/connector/

# Runtime Stage
################################################################################
FROM gcr.io/distroless/base-debian11

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/connector"]
