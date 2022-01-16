# Build Stage
################################################################################
FROM golang:1.17-buster as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Build the connector projects we depend on.
COPY source-postgres ./source-postgres
COPY sqlcapture ./sqlcapture
COPY network-proxy-service ./network-proxy-service

# Copy network-proxy-service binary.
COPY network-proxy-service/target/x86_64-unknown-linux-musl/release/network-proxy-service ./bin/network-proxy-service

ENV PATH="/builder/bin:$PATH"
# Run the unit tests.
RUN go test -v ./network-proxy-service/...
RUN go test -v ./source-postgres/...

# Build the connector.
RUN go build -o ./connector -v ./source-postgres/...


# Runtime Stage
################################################################################
FROM gcr.io/distroless/base-debian10

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifacts from the builder.
COPY --from=builder /builder/connector ./connector
COPY --from=builder /builder/bin/network-proxy-service ./network-proxy-service
COPY --from=builder /lib/x86_64-linux-gnu/libgcc_s.so.1 /lib/x86_64-linux-gnu/

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/connector"]
