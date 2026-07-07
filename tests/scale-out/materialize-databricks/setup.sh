#!/bin/bash
# Assembles the preview-next spec for materialize-databricks: a copy of the
# integration testdata config with the scale_out flag enabled, bound to a
# uniquely-named table so that concurrent CI runs don't collide.

CONFIG="${WORKDIR}/config.yaml"
cp "${ROOT_DIR}/materialize-databricks/testdata/config.local.yaml" "${CONFIG}"

sops set "${CONFIG}" '["advanced"]["feature_flags"]' '"allow_existing_tables_for_new_bindings,scale_out"'
sops set "${CONFIG}" '["advanced"]["no_flow_document"]' 'false'
sops set "${CONFIG}" '["syncSchedule"]' '{"syncFrequency": "0s"}'

export TABLE="scale_out_ci_${GITHUB_RUN_ID:-local}_$(date +%s)"
export TASK_NAME="tests/scale-out/materialize-databricks"

# Executes a SQL statement against the test warehouse, emitting result rows as
# tab-separated lines. The decrypted config is only ever visible to the child
# process run by `sops exec-file`.
function dbsql() {
    echo "$1" > "${WORKDIR}/query.sql"
    sops exec-file --output-type json "${CONFIG}" "python3 ${TEST_DIR}/query.py {} ${WORKDIR}/query.sql"
}

function test_cleanup() {
    dbsql "DROP TABLE IF EXISTS \`integration\`.\`some-schema\`.\`${TABLE}\`"
}

cat > "${WORKDIR}/local.flow.yaml" <<EOF
materializations:
  ${TASK_NAME}:
    endpoint:
      local:
        command:
          - ${WORKDIR}/connector
        protobuf: true
        config: config.yaml
    shards:
      logLevel: info
    bindings:
      - source: demo/wikipedia/recentchange-sampled
        resource:
          table: ${TABLE}
        fields:
          recommended: true
EOF
