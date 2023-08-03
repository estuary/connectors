#!/bin/bash

set -e
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:=test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:=test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:=test}"
export DYNAMODB_ENDPOINT="${DYNAMODB_ENDPOINT:=http://source-dynamodb-db-1.flow-test:8000}"

export TEST_STREAM="estuary-test-$(shuf -zer -n6 {a..z} | tr -d '\0')"
export RESOURCE="{ \"table\": \"${TEST_STREAM}\", \"rcuAllocation\": 1000 }"

export DYNAMODB_LOCAL_ENDPOINT="http://localhost:8000"

# Set ID_TYPE to string because numeric DynamoDB key fields are always output as type: string,
# format: number.
export ID_TYPE=string

config_json_template='{
    "awsAccessKeyId": "${AWS_ACCESS_KEY_ID}",
    "awsSecretAccessKey": "${AWS_SECRET_ACCESS_KEY}",
    "region": "${AWS_DEFAULT_REGION}",
    "advanced": {
        "endpoint": "${DYNAMODB_ENDPOINT}"
    }
}'

export CONNECTOR_CONFIG="$(echo "$config_json_template" | envsubst | jq -c)"
echo "Connector configuration is: ${CONNECTOR_CONFIG}".

docker compose -f source-dynamodb/docker-compose.yaml up --detach

retry_counter=0
while true; do
    if aws dynamodb list-tables --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}"; then
        echo "DynamoDB container ready"
        break
    fi

    retry_counter=$((retry_counter + 1))
    if [[ "$retry_counter" -eq "30" ]]; then
        bail "Timeout reached"
    fi

    echo "DynamoDB container not ready, retrying ${retry_counter}"
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

retry_counter=0
while true; do
    status=$(aws dynamodb describe-table --table-name "${TEST_STREAM}" --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" | jq '.Table.TableStatus')
    if [ "$status" == "\"ACTIVE\"" ]; then
        echo "Table '${TEST_STREAM}' ready"
        break
    fi

    retry_counter=$((retry_counter + 1))
    if [[ "$retry_counter" -eq "30" ]]; then
        bail "Timeout reached"
    fi

    echo "Table not ready (${status}), retrying ${retry_counter}"
    sleep 1
done

echo "Adding test data to table '${TEST_STREAM}'"
cat tests/source-dynamodb/data.jsonl | xargs -I '{}' aws dynamodb put-item --table-name "${TEST_STREAM}" --endpoint-url "${DYNAMODB_LOCAL_ENDPOINT}" --item '{}'
