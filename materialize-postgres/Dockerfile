# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Download recent flow binaries for usage by tests.
RUN curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz"| tar -zx -C /usr/local/bin/

# Build the connector projects we depend on.
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-postgres    ./materialize-postgres
COPY testsupport             ./testsupport

# Test and build the connector.
#RUN go test  -tags nozstd -v ./materialize-postgres/...
RUN go build -tags nozstd -v -o ./connector ./materialize-postgres/...

# Runtime Stage
################################################################################
FROM --platform=linux/amd64 debian:stable-slim as debian

RUN apt-get update && apt-get install -y openssh-client

WORKDIR /connector
ENV PATH="/connector:$PATH"

COPY --from=builder /builder/connector ./connector

# Avoid running the connector as root.
COPY --from=gcr.io/distroless/cc-debian11 /etc/passwd /etc/passwd
COPY --from=gcr.io/distroless/cc-debian11 /etc/group /etc/group
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/connector"]
