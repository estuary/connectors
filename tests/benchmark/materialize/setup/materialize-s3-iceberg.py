"""Create the bucket, Polaris catalog, catalog_admin grant and namespace that
materialize-s3-iceberg's local stack needs, from the connector config on stdin.
Anything which already exists is left as it is."""

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
import yaml


def call(method, url, body=None, token=None, form=False):
    """Make a request, returning its status and body, errors included."""
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
        return err.code, err.read()


def polaris_token(api, client_id, client_secret):
    """Wait for Polaris to come up and issue a token."""
    for _ in range(60):
        try:
            status, body = call("POST", api + "/v1/oauth/tokens", {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            }, form=True)
            if status == 200:
                return json.loads(body)["access_token"]
        except urllib.error.URLError:
            pass
        time.sleep(1)
    sys.exit("could not get a Polaris token")


def main():
    cfg = yaml.safe_load(sys.stdin)
    creds, catalog = cfg["credentials"], cfg["catalog"]
    api = catalog["uri"].rstrip("/")  # .../api/catalog
    management = api.rsplit("/catalog", 1)[0] + "/management/v1"
    client_id, client_secret = catalog["credential"].split(":", 1)

    # The bucket, through rustfs's S3 API (path-style, static credentials).
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

    token = polaris_token(api, client_id, client_secret)

    warehouse = catalog["warehouse"]
    base = f"s3://{cfg['bucket']}/{cfg['prefix']}"
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
            # Polaris reaches rustfs over the compose network.
            "endpointInternal": "http://rustfs:9000",
            "pathStyleAccess": True,
            # rustfs has no STS; clients use the catalog's s3.* properties.
            "stsUnavailable": True,
        },
    }}, token=token)
    if status not in (200, 201, 409):
        sys.exit(f"creating Polaris catalog: {status} {body!r}")

    status, body = call(
        "PUT", f"{management}/principal-roles/service_admin/catalog-roles/{warehouse}",
        {"catalogRole": {"name": "catalog_admin"}}, token=token)
    if status not in (200, 201, 204):
        sys.exit(f"granting catalog_admin: {status} {body!r}")

    status, body = call("POST", f"{api}/v1/{warehouse}/namespaces",
                        {"namespace": [cfg["namespace"]]}, token=token)
    if status not in (200, 201, 409):
        sys.exit(f"creating namespace: {status} {body!r}")


if __name__ == "__main__":
    main()
