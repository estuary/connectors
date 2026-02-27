FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/estuary/connectors"
LABEL org.opencontainers.image.description="Estuary Flow source connector for TCP/TLS socket connections"

WORKDIR /connector

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy and install the connector
COPY pyproject.toml README.md ./
COPY source_socket/ ./source_socket/

RUN pip install --no-cache-dir .

# Set entrypoint
ENTRYPOINT ["source-socket"]