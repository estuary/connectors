#!/usr/bin/env bash
set -euo pipefail

connector_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
docker compose -f "$connector_dir/docker-compose.yaml" exec -T db influx write \
    --host http://localhost:8086 --org estuary --bucket sensors \
    --token made-up-token --precision s <<'EOF'
readings,host=a,region=west value=1,ok=true 1794312000
readings,host=a,region=west value=2,ok=true 1794312060
readings,host=a,region=west value=3,ok=false 1794312120
status,host=a state="up",count=7i 1794312000
EOF
