ARG BASE_IMAGE=ghcr.io/estuary/base-image:v1

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.24-bookworm AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-sqlite      ./materialize-sqlite
COPY materialize-sql         ./materialize-sql

# Test and build the connector.
RUN go test  -tags nozstd,nodb -v ./materialize-sqlite/...
RUN go build -tags nozstd -v -o ./connector ./materialize-sqlite/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# install sqlite3 to allow debugging / troubleshooting by exec-ing into the
# container
RUN DEBIAN_FRONTEND=noninteractive apt update && apt install sqlite3 -y --no-install-recommends

# install datasette, taken from https://github.com/simonw/datasette/blob/main/Dockerfile
ENV DATASETTE_VERSION="0.64.2"

RUN apt-get update && \
  apt-get install -y --no-install-recommends libsqlite3-mod-spatialite \
  python3-pip && \
  apt clean && \
  rm -rf /var/lib/apt && \
  rm -rf /var/lib/dpkg/info/*

RUN pip install https://github.com/simonw/datasette/archive/refs/tags/${DATASETTE_VERSION}.zip --break-system-packages && \
  find /usr/local/lib -name '__pycache__' | xargs rm -r && \
  rm -rf /root/.cache/pip

EXPOSE 8001

COPY --from=builder /builder/connector ./materialize-sqlite
COPY --from=builder /builder/materialize-sqlite/init.sh ./init.sh

# Avoid running the connector as root.
USER nonroot:nonroot

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/init.sh", "/connector/materialize-sqlite"]
