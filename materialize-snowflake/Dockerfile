# Build Stage
################################################################################
FROM golang:1.17-bullseye as builder

WORKDIR /builder

# Download & compile dependencies early. Doing this separately allows for layer
# caching opportunities when no dependencies are updated.
COPY go.* ./
RUN go mod download

# Download recent flow binaries for usage by tests.
RUN curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz" | tar -zx -C /usr/local/bin/

# Build the connector projects we depend on.
COPY materialize-boilerplate ./materialize-boilerplate
COPY materialize-snowflake   ./materialize-snowflake
COPY testsupport             ./testsupport

# Test and build the connector.
RUN go test  -tags nozstd -v ./materialize-snowflake/...
RUN go build -tags nozstd -v -o ./connector ./materialize-snowflake/...

# Runtime Stage
################################################################################
FROM ghcr.io/estuary/base-image:v1

RUN apt-get update -y \
 && apt-get install --no-install-recommends -y \
      ca-certificates \
      curl \
      unzip \
 && rm -rf /var/lib/apt/lists/*

# Avoid running the connector as root.
USER nonroot:nonroot

WORKDIR /connector
ENV PATH="/connector:$PATH"

# Install snowsql, which is required by the connector.
# This must be done as the nonroot user, since snowsql always puts its actual binaries in ~/.snowsql.
# LC_ALL and LANG are required at runtime by the snowsql cli
# The DEST and LOGIN_SHELL vars are needed by the installer in order to run in non-interactive mode.
# The VERSION vars are only here to make version updates easier.
# The PATH must be modified to include the install location, since .profile will not be loaded.
ENV LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    SNOWSQL_DEST=/home/nonroot/bin \
    SNOWSQL_LOGIN_SHELL=/home/nonroot/.profile \
    SNOWSQL_MINOR_VERSION=1.2 \
    SNOWSQL_FULL_VERSION=1.2.14 \
    SNOWSQL_SHA256=1afb83a22b9ccb2f8e84c2abe861da503336cb3b882fcc2e8399f86ac76bc2a9 \
    PATH="/home/nonroot/bin:${PATH}"
RUN curl -o /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${SNOWSQL_MINOR_VERSION}/linux_x86_64/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && echo "${SNOWSQL_SHA256} /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash" | sha256sum -c - \
  && touch ${SNOWSQL_LOGIN_SHELL} \
  && bash /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  && rm -f /tmp/snowsql-${SNOWSQL_FULL_VERSION}-linux_x86_64.bash \
  # Defying all reason and expectations, _this_ is what actually installs snowsql.
  # It will print a help message as if there was a problem, but it works as long as it exits 0.
  && snowsql -v ${SNOWSQL_FULL_VERSION}

# Bring in the compiled connector artifact from the builder.
COPY --from=builder /builder/connector ./connector

LABEL FLOW_RUNTIME_PROTOCOL=materialize

ENTRYPOINT ["/connector/connector"]
