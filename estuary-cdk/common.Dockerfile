# syntax=docker/dockerfile:1
FROM python:3.12-slim as base
FROM base as builder

ARG CONNECTOR_NAME

RUN apt-get update && \
    apt install -y --no-install-recommends \
    python3-poetry

RUN python -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv

WORKDIR /opt/${CONNECTOR_NAME}
COPY ${CONNECTOR_NAME} /opt/${CONNECTOR_NAME}
COPY estuary-cdk /opt/estuary-cdk

RUN poetry install


FROM base as runner

ARG CONNECTOR_NAME
ARG CONNECTOR_TYPE
ARG DOCS_URL
# The USAGE_RATE arg is required, because GH actions doesn't seem to have a way to conditionally
# pass it only for the connectors that should have a 0 rate. Comes from `usage_rate` in the
# `python.yaml` workflow matrix.
ARG USAGE_RATE

LABEL FLOW_RUNTIME_PROTOCOL=${CONNECTOR_TYPE}
LABEL FLOW_RUNTIME_CODEC=json
LABEL dev.estuary.usage-rate=${USAGE_RATE}

COPY --from=builder /opt/$CONNECTOR_NAME /opt/$CONNECTOR_NAME
COPY --from=builder /opt/estuary-cdk /opt/estuary-cdk
COPY --from=builder /opt/venv /opt/venv

ENV DOCS_URL=${DOCS_URL}
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

CMD /opt/venv/bin/python -m $(echo "$CONNECTOR_NAME" | tr '-' '_')