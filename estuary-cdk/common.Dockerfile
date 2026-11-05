# syntax=docker/dockerfile:1
FROM python:3.14-slim AS base
FROM base AS builder

ARG CONNECTOR_NAME

COPY --from=ghcr.io/astral-sh/uv:0.13.3 /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /opt/workspace

# `--frozen` makes uv.lock authoritative, so uv needs neither the other
# members' manifests nor a resolution step -- only the root, the lock, and the
# sources of the member being built.
COPY pyproject.toml uv.lock ./
COPY estuary-cdk/pyproject.toml ./estuary-cdk/
COPY ${CONNECTOR_NAME}/pyproject.toml ./${CONNECTOR_NAME}/

# Third-party dependencies only, so this layer survives any change to the
# connector's or the CDK's own code.
RUN uv sync --package ${CONNECTOR_NAME} --frozen --no-dev --no-install-workspace

COPY estuary-cdk ./estuary-cdk
COPY ${CONNECTOR_NAME} ./${CONNECTOR_NAME}

RUN uv sync --package ${CONNECTOR_NAME} --frozen --no-dev

FROM base AS runner

ARG CONNECTOR_NAME
ARG CONNECTOR_TYPE
ARG DOCS_URL
ARG ENCRYPTION_URL="https://config-encryption.estuary.dev/v1/encrypt-config"
# The USAGE_RATE arg is required, because GH actions doesn't seem to have a way to conditionally
# pass it only for the connectors that should have a 0 rate. Comes from `usage_rate` in
# `.github/python-connectors.yaml`.
ARG USAGE_RATE

RUN apt-get update && \
    apt install -y --no-install-recommends \
    openssh-client \
    tzdata \
    tzdata-legacy

LABEL FLOW_RUNTIME_PROTOCOL=${CONNECTOR_TYPE}
LABEL FLOW_RUNTIME_CODEC=json
LABEL dev.estuary.usage-rate=${USAGE_RATE}

COPY --from=builder /opt/workspace /opt/workspace
COPY --from=builder /opt/venv /opt/venv
COPY --from=ghcr.io/estuary/network-tunnel:dev /flow-network-tunnel /usr/bin/flow-network-tunnel

ENV DOCS_URL=${DOCS_URL}
ENV ENCRYPTION_URL=${ENCRYPTION_URL}
ENV CONNECTOR_NAME=${CONNECTOR_NAME}

CMD ["/bin/sh", "-c", "/opt/venv/bin/python -m $(echo \"$CONNECTOR_NAME\" | tr '-' '_')"]
