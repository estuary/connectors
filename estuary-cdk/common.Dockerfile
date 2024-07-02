# syntax=docker/dockerfile:1
FROM python:3.12-slim as base
FROM base as builder

ARG CONNECTOR_NAME

RUN apt-get update && \
    apt install -y --no-install-recommends \
    python3-poetry

# install oracle instant client
RUN apt-get update && apt-get install -y --no-install-recommends alien libaio1 wget && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm && \
    wget https://download.oracle.com/otn_software/linux/instantclient/185000/oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-basiclite-18.5.0.0.0-3.x86_64.rpm && \
    alien -i oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64.rpm
ENV LD_LIBRARY_PATH="/usr/lib/oracle/18.5/client64/lib:${LD_LIBRARY_PATH}"

RUN python -m venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv

WORKDIR /opt/${CONNECTOR_NAME}
COPY ${CONNECTOR_NAME}/poetry.lock /opt/${CONNECTOR_NAME}
COPY ${CONNECTOR_NAME}/pyproject.toml /opt/${CONNECTOR_NAME}/pyproject.toml
COPY estuary-cdk /opt/estuary-cdk
RUN poetry install

COPY ${CONNECTOR_NAME} /opt/${CONNECTOR_NAME}

FROM base as runner

ARG CONNECTOR_NAME
ARG CONNECTOR_TYPE
ARG DOCS_URL
# The USAGE_RATE arg is required, because GH actions doesn't seem to have a way to conditionally
# pass it only for the connectors that should have a 0 rate. Comes from `usage_rate` in the
# `python.yaml` workflow matrix.
ARG USAGE_RATE

RUN apt-get update && \
    apt install -y --no-install-recommends \
    openssh-client

LABEL FLOW_RUNTIME_PROTOCOL=${CONNECTOR_TYPE}
LABEL FLOW_RUNTIME_CODEC=json
LABEL dev.estuary.usage-rate=${USAGE_RATE}

COPY --from=builder /opt/$CONNECTOR_NAME /opt/$CONNECTOR_NAME
COPY --from=builder /opt/estuary-cdk /opt/estuary-cdk
COPY --from=builder /opt/venv /opt/venv
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

ENV DOCS_URL=${DOCS_URL}
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

CMD /opt/venv/bin/python -m $(echo "$CONNECTOR_NAME" | tr '-' '_')
