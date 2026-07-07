#!/bin/bash
# Asserts that the multi-shard run materialized correct data: the table is
# non-empty (session two's recovery replayed session one's staged MERGEs) and
# contains no duplicate keys (each key landed exactly once despite two shards
# staging concurrently and the recovery replay).

COUNTS=$(dbsql "SELECT COUNT(*), COUNT(DISTINCT \`_meta/file\`, \`_meta/offset\`) FROM \`integration\`.\`some-schema\`.\`${TABLE}\`")
TOTAL=$(echo "${COUNTS}" | cut -f1)
DISTINCT=$(echo "${COUNTS}" | cut -f2)

echo "table ${TABLE} has ${TOTAL} rows, ${DISTINCT} distinct keys"

if [[ -z "${TOTAL}" || "${TOTAL}" -eq 0 ]]; then
    bail "the recovery replay did not materialize any rows"
fi
if [[ "${TOTAL}" != "${DISTINCT}" ]]; then
    bail "table contains duplicate keys"
fi
