#!/bin/bash

# Fetches the latest binary release of Flow and writes the binaries to the `flow-bin/` directory.
# The release name is read from the `FLOW_RELEASE` env variable, but it defaults to "dev".

FLOW_RELEASE="${FLOW_RELEASE:-dev-next}"

# A pinned tagged release whose `flowctl preview` still runs the legacy `runtime`
# crate, i.e. runtime v1. The rolling release above tracks runtime-next, and
# legacy preview is slated to be rewired onto it, so the runtime v1 integration
# tests use this binary (see FLOW_TEST_FLOWCTL in materialize-boilerplate/testutil).
# Tagged releases name the asset differently from the rolling one.
FLOW_RUNTIME_V1_RELEASE="${FLOW_RUNTIME_V1_RELEASE:-v0.6.13}"

mkdir -p flow-bin &&
rm -f flow-bin/* &&
cd flow-bin &&

curl -L -sSf --retry 5 "https://github.com/getsops/sops/releases/download/v3.13.3/sops-v3.13.3.linux.amd64" -o sops &&
curl -L -sSf --retry 5 "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flowctl-linux-x86_64" -o flowctl &&
curl -L -sSf --retry 5 "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flowctl-go-linux-x86_64" -o flowctl-go &&
curl -L -sSf --retry 5 "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flow-parser-linux-x86_64" -o flow-parser &&
curl -L -sSf --retry 5 "https://github.com/estuary/flow/releases/download/${FLOW_RELEASE}/flow-connector-init-linux-x86_64" -o flow-connector-init &&
curl -L -sSf --retry 5 "https://github.com/estuary/flow/releases/download/${FLOW_RUNTIME_V1_RELEASE}/flowctl-x86_64-linux" -o flowctl-v1 &&
chmod +x flowctl flowctl-go flow-parser flow-connector-init flowctl-v1 sops
