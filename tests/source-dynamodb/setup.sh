#!/bin/bash

set -e
export DYNAMODB_ACCESS_KEY_ID="${DYNAMODB_ACCESS_KEY_ID:=test}"
export DYNAMODB_SECRET_ACCESS_KEY="${DYNAMODB_SECRET_ACCESS_KEY:=test}"
export DYNAMODB_REGION="${DYNAMODB_REGION:=test}"
export DYNAMODB_ENDPOINT="${DYNAMODB_ENDPOINT:=http://source-dynamodb-db-1.flow-test:8000}"

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"table\": \"${TEST_STREAM}\", \"rcuAllocation\": 1000 }"

export DYNAMODB_LOCAL_ENDPOINT="http://localhost:8000"

# Set ID_TYPE to string because numeric DynamoDB key fields are always output as type: string,
# format: number.
export ID_TYPE=string

config_json_template='{
    "awsAccessKeyId": "${DYNAMODB_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${DYNAMODB_SECRET_ACCESS_KEY}",
    "region": "${DYNAMODB_REGION}",
    "advanced": {
        "endpoint": "${DYNAMODB_ENDPOINT}"
    }
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

docker compose -f source-dynamodb/docker-compose.yaml up --detach

for i in {1..20}; do
    if aws dynamodb list-tables --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" >/dev/null; then
        echo "DynamoDB container ready"
        break
    fi
    echo "DynamoDB container not ready, retrying ${i}"
    sleep 1
done

aws dynamodb create-table \
    --table-name ${TEST_STREAM} \
    --attribute-definitions \
    AttributeName=id,AttributeType=N \
    AttributeName=canary,AttributeType=S \
    --key-schema \
    AttributeName=id,KeyType=HASH \
    AttributeName=canary,KeyType=RANGE \
    --provisioned-throughput \
    ReadCapacityUnits=1,WriteCapacityUnits=1 \
    --table-class STANDARD \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES \
    --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" >/dev/null

for i in {1..20}; do
    status=$(aws dynamodb describe-table --table-name "${TEST_STREAM}" --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" | jq '.Table.TableStatus')
    if [ $status == "\"ACTIVE\"" ]; then
        echo "Table '${TEST_STREAM}' ready"
        break
    fi
    echo "Table not ready (${status}), retrying ${i}"
    sleep 1
done

echo "Adding test data to table '${TEST_STREAM}'"
cat tests/source-dynamodb/data.jsonl | xargs -I '{}' aws dynamodb put-item --table-name "${TEST_STREAM}" --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" --item '{}'
