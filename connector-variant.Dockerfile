# This dockerfile is a simple extension of a BASE_CONNECTOR dockerfile to allow for building a
# variant of that base connector with an alternate documentation URL specified via the DOCS_URL
# build argument. The connector must support optionally reading the DOCS_URL environment variable
# for its spec response.

ARG BASE_CONNECTOR
FROM --platform=linux/amd64 ${BASE_CONNECTOR}
ARG DOCS_URL
ENV DOCS_URL="${DOCS_URL}"
