# This dockerfile is a simple extension of a BASE_CONNECTOR dockerfile to allow for building a
# variant of that base connector with an alternate documentation URL specified via the DOCS_URL
# build argument. The connector must support optionally reading the DOCS_URL environment variable
# for its spec response. CONNECTOR_NAME names the variant for log lines that identify the
# connector, since the variant runs its base connector's binary.

ARG BASE_CONNECTOR
FROM --platform=linux/amd64 ${BASE_CONNECTOR}
ARG DOCS_URL
ENV DOCS_URL="${DOCS_URL}"
ARG CONNECTOR_NAME
ENV CONNECTOR_NAME="${CONNECTOR_NAME}"
