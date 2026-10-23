#!/usr/bin/env bash
#
# Benchmark setup for materialize-s3-iceberg against its local compose stack:
# create the rustfs bucket, a Polaris catalog over it with the bootstrap
# principal granted catalog_admin, and the namespace, as driver_test.go does.
# Reads the bucket, endpoints and catalog from $BENCH_CONFIG.

set -o errexit
set -o pipefail
set -o nounset

[[ -n "${BENCH_CONFIG:-}" && -f "$BENCH_CONFIG" ]] || {
  echo "BENCH_CONFIG must point to the connector config YAML" >&2
  exit 1
}

# The decrypted config travels on stdin: a command line is readable by every
# local user through /proc/<pid>/cmdline.
{ sops -d "$BENCH_CONFIG" 2>/dev/null || cat "$BENCH_CONFIG"; } | python3 /dev/fd/3 3<<'PY'
import json, sys, time, urllib.error, urllib.parse, urllib.request
import yaml

cfg = yaml.safe_load(sys.stdin)
creds, catalog = cfg["credentials"], cfg["catalog"]
api = catalog["uri"].rstrip("/")  # .../api/catalog
management = api.rsplit("/catalog", 1)[0] + "/management/v1"
client_id, client_secret = catalog["credential"].split(":", 1)

def call(method, url, body=None, token=None, form=False, ok=(200, 201, 204)):
    data, headers = None, {}
    if body is not None and form:
        data = urllib.parse.urlencode(body).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as err:
        if err.code in ok:
            return err.code, err.read()
        return err.code, err.read()

# The bucket, through rustfs's S3 API (path-style, static credentials).
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url=cfg["s3_endpoint"],
    region_name=cfg["region"],
    aws_access_key_id=creds["awsAccessKeyId"],
    aws_secret_access_key=creds["awsSecretAccessKey"],
)
try:
    s3.create_bucket(Bucket=cfg["bucket"])
except s3.exceptions.BucketAlreadyOwnedByYou:
    pass

token = None
for _ in range(60):
    try:
        status, body = call("POST", api + "/v1/oauth/tokens", {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        }, form=True)
        if status == 200:
            token = json.loads(body)["access_token"]
            break
    except urllib.error.URLError:
        pass
    time.sleep(1)
if not token:
    sys.exit("could not get a Polaris token")

warehouse = catalog["warehouse"]
base = f"s3://{cfg['bucket']}/{cfg['prefix']}"
call("DELETE", f"{management}/catalogs/{warehouse}", token=token)
status, body = call("POST", f"{management}/catalogs", {"catalog": {
    "name": warehouse,
    "type": "INTERNAL",
    "readOnly": False,
    "properties": {
        "default-base-location": base,
        "s3.endpoint": cfg["s3_endpoint"],
        "s3.access-key-id": creds["awsAccessKeyId"],
        "s3.secret-access-key": creds["awsSecretAccessKey"],
        "s3.region": cfg["region"],
        "s3.path-style-access": "true",
    },
    "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": [base + "/"],
        "roleArn": "arn:aws:iam::000000000000:role/dummy",
        "region": cfg["region"],
        "endpoint": cfg["s3_endpoint"],
        "endpointInternal": "http://rustfs:9000",
        "pathStyleAccess": True,
        "stsUnavailable": True,
    },
}}, token=token)
if status not in (200, 201):
    sys.exit(f"creating Polaris catalog: {status} {body!r}")

status, body = call("PUT", f"{management}/principal-roles/service_admin/catalog-roles/{warehouse}",
                    {"catalogRole": {"name": "catalog_admin"}}, token=token)
if status not in (200, 201, 204):
    sys.exit(f"granting catalog_admin: {status} {body!r}")

status, body = call("POST", f"{api}/v1/{warehouse}/namespaces",
                    {"namespace": [cfg["namespace"]]}, token=token)
if status not in (200, 201, 409):
    sys.exit(f"creating namespace: {status} {body!r}")
PY
