# syntax=docker/dockerfile:1
FROM python:3.11-slim as builder

ARG CONNECTOR_NAME
# Adapted from here: https://github.com/orgs/python-poetry/discussions/1879#discussioncomment-7284113

# This disables buffering in python output which can sometimes
# obscure errors that happen as the container is crashing
ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.6.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV="/venv"

ENV PATH="$POETRY_HOME/bin:$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && \
    apt-get install -y \
    apt-transport-https \
    gnupg \
    ca-certificates \
    build-essential \
    git \
    curl

RUN python -m venv $VIRTUAL_ENV

WORKDIR /builder
ENV PYTHONPATH="/builder:$PYTHONPATH"

# install poetry respecting $POETRY_VERSION, $POETRY_HOME
RUN --mount=type=cache,target=/root/.cache curl -sSL https://install.python-poetry.org | python -

# install connector dependencies
COPY $CONNECTOR_NAME ./connector
COPY python ./python
WORKDIR /builder/connector
RUN --mount=type=cache,target=/root/.cache poetry install --no-root --only main

FROM builder

ARG CONNECTOR_NAME
ARG CONNECTOR_TYPE
ARG DOCS_URL

ENV DOCS_URL=$DOCS_URL

LABEL FLOW_RUNTIME_PROTOCOL=$CONNECTOR_TYPE
LABEL FLOW_RUNTIME_CODEC=json

# copy in our built poetry + venv
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /connector
COPY --from=builder /builder/connector ./$CONNECTOR_NAME
COPY --from=builder /builder/python ./python

# Arg substitution in CMD doesn't seem to work
ENV CONNECTOR_NAME=$CONNECTOR_NAME
CMD python -m $CONNECTOR_NAME