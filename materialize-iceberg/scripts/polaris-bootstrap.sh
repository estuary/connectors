#!/bin/sh
# Bootstrap a freshly-started Polaris instance for the local docker-compose
# integration test stack. Idempotent across restarts of the bootstrap
# container itself, but assumes Polaris is running with in-memory persistence
# (i.e. starts empty on each `docker compose up`).
#
# Outputs the resulting test principal's credentials to
# /shared/polaris-creds.json so the Go test harness can splice them into the
# connector config.

set -eu

# Install runtime deps. The container image is alpine; both packages are tiny.
apk add --no-cache curl jq >/dev/null

POLARIS_HOST="${POLARIS_HOST:-polaris}"
POLARIS_PORT="${POLARIS_PORT:-8181}"
POLARIS_REALM="${POLARIS_REALM:-POLARIS}"
ROOT_CLIENT_ID="${ROOT_CLIENT_ID:-root}"
ROOT_CLIENT_SECRET="${ROOT_CLIENT_SECRET:-s3cr3t}"

CATALOG_NAME="${CATALOG_NAME:-quickstart_catalog}"
PRINCIPAL_NAME="${PRINCIPAL_NAME:-flow_user}"
PRINCIPAL_ROLE="${PRINCIPAL_ROLE:-flow_user_role}"
CATALOG_ROLE="${CATALOG_ROLE:-flow_admin}"
WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-warehouse}"

# Polaris's `endpoint` is what it hands back to clients via the catalog config
# defaults (and also via vended credentials). Our only S3 client that talks
# to Polaris is the Spark cluster, which lives in the same compose network,
# so the "client-facing" endpoint should also be the internal one. The Go
# connector uses its own static S3 endpoint config and does not consult
# Polaris for the endpoint value.
S3_ENDPOINT_HOST="${S3_ENDPOINT_HOST:-http://rustfs:9000}"
S3_ENDPOINT_INTERNAL="${S3_ENDPOINT_INTERNAL:-http://rustfs:9000}"

BASE="http://${POLARIS_HOST}:${POLARIS_PORT}"
MGMT="${BASE}/api/management/v1"
TOKENS="${BASE}/api/catalog/v1/oauth/tokens"

REALM_HDR="Polaris-Realm: ${POLARIS_REALM}"

log() { echo "[polaris-bootstrap] $*"; }

# If a credentials file from a prior bootstrap is still present and Polaris
# was not torn down (in-memory persistence survives container restart but not
# `compose down -v`), keep the existing credentials and exit. This makes
# `docker compose up --wait` idempotent across repeated invocations of the
# same compose project.
if [ -s /shared/polaris-creds.json ]; then
  if grep -q '"client_id"' /shared/polaris-creds.json && \
     grep -q '"client_secret"' /shared/polaris-creds.json; then
    log "credentials already present at /shared/polaris-creds.json, skipping bootstrap"
    exit 0
  fi
fi

# Acquire a root access token. Polaris occasionally takes another second or two
# after readiness probes pass before the OAuth endpoint is fully online; retry
# briefly to absorb that race.
get_root_token() {
  i=0
  while [ "$i" -lt 300 ]; do
    resp=$(curl -fsS -X POST "${TOKENS}" \
      -H "Content-Type: application/x-www-form-urlencoded" \
      --data-urlencode "grant_type=client_credentials" \
      --data-urlencode "client_id=${ROOT_CLIENT_ID}" \
      --data-urlencode "client_secret=${ROOT_CLIENT_SECRET}" \
      --data-urlencode "scope=PRINCIPAL_ROLE:ALL" 2>/dev/null) || resp=""
    if [ -n "$resp" ]; then
      token=$(echo "$resp" | jq -r '.access_token // empty')
      if [ -n "$token" ]; then
        echo "$token"
        return 0
      fi
    fi
    i=$((i + 1))
    sleep 0.1
  done
  log "failed to acquire root token after 300 attempts"
  return 1
}

# POST to a management endpoint. Treats 409 Conflict as success so the script
# is idempotent across re-runs against the same realm.
mgmt_post() {
  url=$1
  body=$2
  http_code=$(curl -sS -o /tmp/resp -w "%{http_code}" -X POST "$url" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "${REALM_HDR}" \
    -H "Content-Type: application/json" \
    -d "$body")
  case "$http_code" in
    2*|409) cat /tmp/resp ;;
    *) log "POST $url failed with HTTP $http_code: $(cat /tmp/resp)"; return 1 ;;
  esac
}

mgmt_put() {
  url=$1
  body=$2
  http_code=$(curl -sS -o /tmp/resp -w "%{http_code}" -X PUT "$url" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "${REALM_HDR}" \
    -H "Content-Type: application/json" \
    -d "$body")
  case "$http_code" in
    2*|409) cat /tmp/resp ;;
    *) log "PUT $url failed with HTTP $http_code: $(cat /tmp/resp)"; return 1 ;;
  esac
}

log "acquiring root token from ${TOKENS}"
TOKEN=$(get_root_token)
log "root token acquired"

log "creating catalog ${CATALOG_NAME}"
mgmt_post "${MGMT}/catalogs" "$(cat <<EOF
{
  "catalog": {
    "name": "${CATALOG_NAME}",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "s3://${WAREHOUSE_BUCKET}/"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://${WAREHOUSE_BUCKET}/"],
      "endpoint": "${S3_ENDPOINT_HOST}",
      "endpointInternal": "${S3_ENDPOINT_INTERNAL}",
      "pathStyleAccess": true,
      "region": "us-east-1"
    }
  }
}
EOF
)" >/dev/null

log "creating principal ${PRINCIPAL_NAME}"
principal_resp=$(mgmt_post "${MGMT}/principals" "{\"principal\": {\"name\": \"${PRINCIPAL_NAME}\"}}")
client_id=$(echo "$principal_resp" | jq -r '.credentials.clientId // empty')
client_secret=$(echo "$principal_resp" | jq -r '.credentials.clientSecret // empty')

if [ -z "$client_id" ] || [ -z "$client_secret" ]; then
  # Principal already existed (409). Rotate its credentials so we have a known
  # secret to hand back to the test harness.
  log "rotating credentials for existing principal ${PRINCIPAL_NAME}"
  rotate_resp=$(mgmt_post "${MGMT}/principals/${PRINCIPAL_NAME}/rotate" "{}")
  client_id=$(echo "$rotate_resp" | jq -r '.credentials.clientId // empty')
  client_secret=$(echo "$rotate_resp" | jq -r '.credentials.clientSecret // empty')
fi

if [ -z "$client_id" ] || [ -z "$client_secret" ]; then
  log "could not obtain principal credentials"
  exit 1
fi

log "creating principal role ${PRINCIPAL_ROLE}"
mgmt_post "${MGMT}/principal-roles" \
  "{\"principalRole\": {\"name\": \"${PRINCIPAL_ROLE}\"}}" >/dev/null

log "binding principal role ${PRINCIPAL_ROLE} to ${PRINCIPAL_NAME}"
mgmt_put "${MGMT}/principals/${PRINCIPAL_NAME}/principal-roles" \
  "{\"principalRole\": {\"name\": \"${PRINCIPAL_ROLE}\"}}" >/dev/null

log "creating catalog role ${CATALOG_ROLE} in ${CATALOG_NAME}"
mgmt_post "${MGMT}/catalogs/${CATALOG_NAME}/catalog-roles" \
  "{\"catalogRole\": {\"name\": \"${CATALOG_ROLE}\"}}" >/dev/null

log "granting CATALOG_MANAGE_CONTENT to ${CATALOG_ROLE}"
mgmt_put "${MGMT}/catalogs/${CATALOG_NAME}/catalog-roles/${CATALOG_ROLE}/grants" \
  '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' >/dev/null

log "binding catalog role ${CATALOG_ROLE} to principal role ${PRINCIPAL_ROLE}"
mgmt_put "${MGMT}/principal-roles/${PRINCIPAL_ROLE}/catalog-roles/${CATALOG_NAME}" \
  "{\"catalogRole\": {\"name\": \"${CATALOG_ROLE}\"}}" >/dev/null

log "writing credentials to /shared/polaris-creds.json"
cat > /shared/polaris-creds.json <<EOF
{
  "client_id": "${client_id}",
  "client_secret": "${client_secret}",
  "catalog": "${CATALOG_NAME}",
  "principal_role": "${PRINCIPAL_ROLE}"
}
EOF
# Make the output readable by the host user that mounted the directory; the
# bootstrap runs as root inside the container, so without this the test
# harness on the host might not be able to read it on systems with strict
# umask defaults.
chmod 644 /shared/polaris-creds.json

log "bootstrap complete"
