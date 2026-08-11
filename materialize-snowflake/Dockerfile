ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM --platform=linux/amd64 golang:1.25-trixie AS builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Copy in the latest flowctl-go for usage by tests.
COPY flow-bin/flowctl-go /usr/local/bin/flowctl-go

COPY go                      ./go
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-snowflake   ./materialize-snowflake
COPY materialize-sql         ./materialize-sql
COPY testsupport             ./testsupport

# The supervisor tests drive a stdlib-only python fake sidecar, so python3 is a
# real dependency of this stage rather than one inherited incidentally from the
# base image. Without it those tests skip instead of failing, so the build would
# stay green while silently covering less.
RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     python3 \
     && rm -rf /var/lib/apt/lists/*

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-sql/...
RUN go test  -tags nozstd -short -v ./materialize-snowflake/...
RUN go build -tags nozstd -v -o ./connector ./materialize-snowflake/cmd/connector

# Python Sidecar Stage
################################################################################
# The Snowpipe Streaming sidecar venv is built on the same base image as the
# runtime stage so its interpreter and site-packages exactly match the python3
# installed there.
FROM ${BASE_IMAGE} AS pybuilder

USER root
RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     python3 \
     python3-venv \
     && rm -rf /var/lib/apt/lists/*

COPY materialize-snowflake/sidecar /sidecar
RUN python3 -m venv /opt/venv \
     && /opt/venv/bin/pip install --no-cache-dir /sidecar

# Test the sidecar, as the builder stage does the connector. A throwaway venv
# holds pytest, keeping the test dependencies out of the venv the image ships,
# and reaches the SDK through the shipped venv's site-packages: most of the suite
# stubs the SDK, but the guard on the append path's use of msgspec.Raw has to run
# against the installed one. SNOWPIPE_SDK_GUARD makes an SDK it cannot import a
# failure there rather than a skip, so the guard cannot be passed over silently.
RUN python3 -m venv /tmp/testvenv \
     && /tmp/testvenv/bin/pip install --no-cache-dir pytest \
     && SNOWPIPE_SDK_GUARD=required \
        PYTHONPATH="/sidecar:$(/opt/venv/bin/python -c 'import site; print(site.getsitepackages()[0])')" \
        /tmp/testvenv/bin/python -m pytest -q /sidecar/tests \
     && rm -rf /tmp/testvenv

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

RUN apt-get update -y \
     && apt-get install --no-install-recommends -y \
     ca-certificates \
     curl \
     unzip \
     python3 \
     && rm -rf /var/lib/apt/lists/*

# The Snowpipe Streaming (high-performance) python SDK sidecar.
COPY --from=pybuilder /opt/venv /opt/venv

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./materialize-snowflake

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/materialize-snowflake"]
