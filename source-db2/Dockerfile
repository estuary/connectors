ARG BASE_IMAGE=ghcr.io/estuary/base-image:v2

# Build Stage
################################################################################
FROM golang:1.27-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends libxml2

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Install the IBM DB2 clidriver first (provides header files and libraries)
RUN go run github.com/ibmdb/go_ibm_db/installer@v0.5.4

# Set environment variables for CGO to find the clidriver
ENV IBM_DB_HOME=/go/pkg/mod/github.com/ibmdb/clidriver
ENV CGO_CFLAGS=-I${IBM_DB_HOME}/include
ENV CGO_LDFLAGS=-L${IBM_DB_HOME}/lib
ENV LD_LIBRARY_PATH=${IBM_DB_HOME}/lib

COPY go                 ./go
COPY source-boilerplate ./source-boilerplate
COPY sqlcapture         ./sqlcapture

RUN go install -v ./source-boilerplate/...

COPY source-db2 ./source-db2

# Run automated tests. Tests which interact with an external database are opt-in.
ARG TEST_DATABASE=no
ENV TEST_DATABASE=$TEST_DATABASE
ENV CI_BUILD=yes
RUN go test -short -failfast ./source-db2/...

# Build the connector
RUN go build -o ./connector -v ./source-db2/...

# Runtime Stage
################################################################################
FROM ${BASE_IMAGE}

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Install runtime dependencies for the DB2 clidriver
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libxml2

# Copy the DB2 clidriver files needed at runtime
COPY --from=builder /go/pkg/mod/github.com/ibmdb/clidriver /opt/ibm/clidriver
ENV IBM_DB_HOME=/opt/ibm/clidriver
ENV LD_LIBRARY_PATH=/opt/ibm/clidriver/lib
# SQL_C_CHAR results must be UTF-8 for the driver's Go strings.
ENV DB2CODEPAGE=1208

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./source-db2
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL CONNECTOR_PROTOCOL=flow-capture

# Avoid running the connector as root.
USER nonroot:nonroot

ENTRYPOINT ["/connector/source-db2"]
