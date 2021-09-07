# Build Stage
################################################################################
FROM golang:1.17-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY parser/*.go ./parser/
COPY filesource ./filesource
COPY source-gcs ./source-gcs

# Run the unit tests.
RUN go test -v ./source-gcs/...

# Build the connector.
RUN go build -o ./connector -v ./source-gcs/...


# Runtime Stage
################################################################################
FROM gcr.io/distroless/base-debian10

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Grab the statically-built parser cli.
COPY parser/target/x86_64-unknown-linux-musl/release/parser ./parser

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/connector"]
