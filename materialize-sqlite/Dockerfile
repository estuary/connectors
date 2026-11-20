ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.27-trixie AS builder

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
RUN go test  -tags nozstd -short -v ./materialize-sqlite/...
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

# pip is only needed to install datasette, so it's purged in the same layer,
# along with the python3-wheel and python3-packaging it pulls in. They lag
# their security fixes, and nothing at runtime uses them. python3 is installed
# explicitly so the purge's autoremove keeps it. The purge has to run before
# /var/lib/dpkg/info is deleted, since apt needs it.
RUN apt-get update && \
  apt-get install -y --no-install-recommends libsqlite3-mod-spatialite \
  python3 python3-pip && \
  pip install https://github.com/simonw/datasette/archive/refs/tags/${DATASETTE_VERSION}.zip --break-system-packages && \
  DEBIAN_FRONTEND=noninteractive apt-get purge -y --auto-remove python3-pip && \
  find /usr/local/lib -name '__pycache__' | xargs rm -r && \
  rm -rf /root/.cache/pip && \
  apt clean && \
  rm -rf /var/lib/apt && \
  rm -rf /var/lib/dpkg/info/*

EXPOSE 8001

COPY --from=builder /builder/connector ./materialize-sqlite
COPY --from=builder /builder/materialize-sqlite/init.sh ./init.sh

# Avoid running the connector as root.
USER nonroot:nonroot

ARG CONNECTOR_VERSION=dev
ENV CONNECTOR_VERSION="${CONNECTOR_VERSION}"

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/init.sh", "/connector/materialize-sqlite"]
