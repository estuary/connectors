#!/bin/bash

set -euo pipefail

export RESOURCE='{"queue":"estuary-test-orders"}'
export CONNECTOR_CONFIG='{"host":"source-rabbitmq-db.flow-test","port":5672,"virtualHost":"/","username":"test","password":"test","managementPort":15672}'

# tests/run.sh uses envsubst to render its catalog. Some local runners do not
# have gettext installed, so provide substitution for its environment variables.
if ! command -v envsubst >/dev/null 2>&1; then
    function envsubst() {
        python3 -c '
import os, re, sys
pattern = r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))"
sys.stdout.write(re.sub(pattern, lambda m: os.environ.get(m[1] or m[2], ""), sys.stdin.read()))
'
    }
fi

docker compose -f source-rabbitmq/docker-compose.yaml up --detach --wait --wait-timeout 180 || return

api="http://localhost:15673/api"
curl -fsS -u test:test -H 'content-type: application/json' -X PUT \
    "$api/queues/%2F/estuary-test-orders" \
    -d '{"durable":true,"arguments":{"x-queue-type":"classic"}}' || return
# Empty any messages left by an interrupted test before seeding the fixture.
curl -fsS -u test:test -X DELETE "$api/queues/%2F/estuary-test-orders/contents" || return

function rabbitmq_publish() {
    jq -nc --arg message_id "$1" --arg payload "$2" \
        '{properties: {message_id: $message_id, timestamp: 1704067200, headers: {fixture: "rabbitmq"},
                       content_type: "application/json", content_encoding: "utf-8", correlation_id: ("request-" + $message_id),
                       reply_to: "replies", type: "order", app_id: "estuary-test", user_id: "test",
                       priority: 0, delivery_mode: 2, expiration: "86400000"},
          routing_key: "estuary-test-orders", payload: $payload, payload_encoding: "string"}' |
        curl -fsS -u test:test -H 'content-type: application/json' -X POST \
            "$api/exchanges/%2F/amq.default/publish" --data-binary @- |
        jq -e '.routed == true' >/dev/null
}

rabbitmq_publish m1 '{"id":1,"canary":"order-created","total":12.5}' || return
rabbitmq_publish m2 '{"id":2,"canary":"order-updated","items":["sku-1","sku-2"]}' || return
rabbitmq_publish m3 '{"id":3,"canary":"order-cancelled","active":false,"_meta":{"producer":"overwritten"}}' || return
