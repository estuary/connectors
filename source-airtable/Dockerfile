FROM python:3.11-slim as builder

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
COPY source-airtable ./source-airtable
COPY python ./python
WORKDIR /builder/source-airtable
RUN --mount=type=cache,target=/root/.cache poetry install --no-root --only main

FROM builder

LABEL FLOW_RUNTIME_PROTOCOL=capture
LABEL FLOW_RUNTIME_CODEC=json

# copy in our built poetry + venv
COPY --from=builder $POETRY_HOME $POETRY_HOME
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

WORKDIR /connector
COPY source-airtable ./source-airtable
COPY python ./python


CMD ["python", "-m", "source-airtable"]