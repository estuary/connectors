#!/bin/bash

set -e

function deleteNamespace() {
    curl --request POST \
        --url https://${PINECONE_INDEX}-${PINECONE_PROJECT_ID}.svc.${PINECONE_ENVIRONMENT}.pinecone.io/vectors/delete \
        --header "Api-Key: ${PINECONE_API_KEY}" \
        --header "accept: application/json" \
        --header "content-type: application/json" \
        --data "
            {
                \"deleteAll\": true,
                \"namespace\": \"${1}\"
            }
            "
}

deleteNamespace "simple"
deleteNamespace "duplicated-keys"
deleteNamespace "multiple-types"
