#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

singlestore_api_token="$(sops --output-type json --decrypt "$SCRIPT_DIR/api-token.singlestore.yaml" | jq -r 'walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)' | jq -r '.token')"

workspace_id='e6d3b980-8c6a-4b9f-806e-6502c8b7d30e'
curl -XPOST \
  "https://api.singlestore.com/v1/workspaces/$workspace_id/resume" \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $singlestore_api_token" \
  -d '{"disableAutoSuspend": false}'

max_attempts=180
attempt=0

while [ $attempt -lt $max_attempts ]
do
  state=$(curl -s "https://api.singlestore.com/v1/workspaces/$workspace_id" -H "Authorization: Bearer $singlestore_api_token" | jq -r '.state')
  if [[ "$state" == "ACTIVE" ]]; then
    break
  fi

  attempt=$((attempt + 1))

  if [ $attempt -eq $max_attempts ]; then
    echo "Error: Workspace did not become ACTIVE after $max_attempts attempts (15 minutes)" >&2
    exit 1
  fi

  sleep 5
done

echo "Workspace is now ACTIVE (took $attempt attempts)"
