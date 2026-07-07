"""Query the integration Databricks warehouse via the SQL Statement API.

Invoked as: sops exec-file --output-type json <encrypted-config> "python3 query.py {} <sql-file>"
The decrypted JSON config path arrives as argv[1]; only query results are
printed, as tab-separated lines.
"""
import json
import sys
import time
import urllib.request

with open(sys.argv[1]) as f:
    cfg = json.load(f)

host = cfg["address"]
warehouse_id = cfg["http_path"].rstrip("/").split("/")[-1]
creds = cfg["credentials"]
token = creds.get("personal_access_token") or creds["personal_access_token_sops"]
statement = open(sys.argv[2]).read()

req = urllib.request.Request(
    f"https://{host}/api/2.0/sql/statements",
    data=json.dumps({
        "warehouse_id": warehouse_id,
        "statement": statement,
        "wait_timeout": "50s",
    }).encode(),
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
)
resp = json.load(urllib.request.urlopen(req))

while resp["status"]["state"] in ("PENDING", "RUNNING"):
    time.sleep(3)
    poll = urllib.request.Request(
        f"https://{host}/api/2.0/sql/statements/{resp['statement_id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    resp = json.load(urllib.request.urlopen(poll))

if resp["status"]["state"] != "SUCCEEDED":
    print("STATE:", resp["status"]["state"], resp["status"].get("error"))
    sys.exit(1)

for row in (resp.get("result") or {}).get("data_array", []):
    print("\t".join("" if v is None else str(v) for v in row))
