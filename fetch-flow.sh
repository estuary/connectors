#!/bin/bash

# Fetches the latest binary release of Flow and writes the binaries to the `flow-bin/` directory.
# The release name is read from the `FLOW_RELEASE` env variable, but it defaults to "dev".

FLOW_RELEASE="${FLOW_RELEASE:-dev-next}"

mkdir -p flow-bin &&
rm -f flow-bin/* &&
cd flow-bin &&

# Get the older package for etcd and gazette.
curl -L --proto '=https' --tlsv1.2 -sSf "https://github.com/estuary/flow/releases/download/dev/flow-x86-linux.tar.gz" | tar -zx &&

# Overwrite some of the binaries with the latest release.
curl -L -sSf "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flowctl-linux-x86_64" -o flowctl &&
curl -L -sSf "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flowctl-go-linux-x86_64" -o flowctl-go &&
curl -L -sSf "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flow-parser-linux-x86_64" -o flow-parser &&
curl -L -sSf "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flow-schemalate-linux-x86_64" -o flow-schemalate &&
chmod +x flowctl flowctl-go flow-parser flow-schemalate
