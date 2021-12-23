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
COPY ssh-forwarding-service ./ssh-forwarding-service

# Copy ssh-forwarding-service binary.
COPY ssh-forwarding-service/target/x86_64-unknown-linux-musl/release/ssh-forwarding-service ./bin/ssh-forwarding-service

ENV PATH="/builder/bin:$PATH"
# Run the unit tests.
RUN go test -v ./ssh-forwarding-service/...
RUN go test -v ./source-postgres/...


# Build the connector.
RUN go build -o ./connector -v ./source-postgres/...

# Runtime Stage
################################################################################
FROM debian:bullseye-slim
# Install openssh-client
RUN apt update && apt install -y openssh-client

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifacts from the builder.
COPY --from=builder /builder/connector ./connector
COPY --from=builder /builder/bin/ssh-forwarding-service ./ssh-forwarding-service

# Create a non-privileged "nonroot" user.
RUN useradd nonroot --create-home --shell /usr/sbin/nologin
# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/connector"]
