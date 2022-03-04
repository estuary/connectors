# Build Stage
################################################################################
FROM golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download
RUN go install github.com/mattn/go-sqlite3

# Download recent flow binaries for usage by tests.
RUN curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz"| tar -zx -C /usr/local/bin/

# Build the connector projects we depend on.
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-postgres    ./materialize-postgres
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-postgres/...
RUN go build -tags nozstd -v -o ./connector ./materialize-postgres/...

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
