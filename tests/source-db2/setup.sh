#!/bin/bash
set -euo pipefail

export RESOURCE='{"namespace":"DB2INST1","stream":"TEST_CAPTURE"}'
export CONNECTOR_CONFIG='{"address":"source-db2-db.flow-test:50000","user":"db2inst1","password":"flowtest1","database":"TESTDB","advanced":{"capture_schema":"ASN","watermarksTable":"DB2INST1.FLOW_WATERMARKS","backfill_chunk_size":2,"polling_interval":"1s","source_tag":"db2-test"}}'

# tests/run.sh uses envsubst to render its catalog; macOS may not have gettext.
if ! command -v envsubst > /dev/null 2>&1; then
    command -v python3 > /dev/null 2>&1 || { echo 'Install gettext or Python 3 to render the test catalog.' >&2; return 1; }
    envsubst() {
        python3 -c 'import os, re, sys
sys.stdout.write(re.sub(r"\$(?:\{([A-Za-z_][A-Za-z_0-9]*)\}|([A-Za-z_][A-Za-z_0-9]*))", lambda m: os.environ.get(m[1] or m[2], ""), sys.stdin.read()))'
    }
fi

db2_compose() {
    docker compose -f source-db2/docker-compose.yaml "$@"
}

db2_setup_log=$(mktemp) || return 1

db2_setup() {
    if ! docker network inspect flow-test > /dev/null 2>&1; then
        docker network create flow-test || return 1
    fi
    db2_compose down --volumes || return 1
    db2_compose up --detach || return 1

    local ready=false
    for ((attempt = 0; attempt < 120; attempt++)); do
        if db2_compose logs --no-color db 2>&1 | grep 'Setup has completed' > /dev/null; then
            ready=true
            break
        fi
        sleep 5
    done
    if [[ "$ready" != true ]]; then
        echo 'Timed out waiting for the Db2 image to finish setup.' >&2
        return 1
    fi

    db2_compose exec -T db bash -e < source-db2/docker-initdb.sh || return 1
    db2_compose up --detach --wait --wait-timeout 60 || return 1
}

# The shared runner installs its cleanup trap after setup returns.
if ! db2_setup 2>&1 | tee "$db2_setup_log"; then
    db2_compose logs --no-color db >&2 || true
    db2_compose exec -T --user db2inst1 db bash -lc 'cat ~/asncap.out ~/db2inst1.TESTDB.ASN.CAP.log' >&2 || true
    db2_container=$(db2_compose ps --all --quiet db) || db2_container=
    if [[ -n "$db2_container" ]]; then
        docker inspect --format '{{json .State.Health}}' "$db2_container" >> "$db2_setup_log" 2>&1 || true
    fi
    db2_compose down --volumes || true
    # Keep setup errors and health-check output after the verbose ASN diagnostics.
    cat "$db2_setup_log" >&2
    rm -f "$db2_setup_log"
    return 1
fi
rm -f "$db2_setup_log"
