"""Scan metrics of a Databricks benchmark run's statements.

Usage:
  databricks_query_history.py --config CONFIG --since ISO8601 [--until ISO8601]
                              [--like SQL-LIKE-PATTERN] [--types SELECT,MERGE,COPY]

Reads the sops-encrypted connector config for the warehouse and token, then
queries system.query.history through the Statement Execution API and prints
one row per statement: type, durations, files read and pruned, bytes and rows
read, rows produced. The load query of a transaction is the SELECT joining
the target table; COPY INTO and MERGE are the commits. Pruned files are the
files skipped by static predicates and dynamic file pruning together.

The workspace must have the system.query schema enabled and the token's user
must be able to read it.
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.request

COLUMNS = [
    "statement_id", "start_time", "statement_type", "total_duration_ms", "execution_duration_ms",
    "read_files", "pruned_files", "read_bytes", "read_rows", "produced_rows",
    "from_result_cache", "statement_text",
]


def load_config(path):
    raw = subprocess.check_output(["sops", "-d", "--output-type", "json", path])
    cfg = json.loads(raw)
    creds = cfg.get("credentials", {})
    if creds.get("auth_type") != "PAT":
        sys.exit("only PAT credentials are supported")
    warehouse = cfg["http_path"].rstrip("/").rsplit("/", 1)[-1]
    # Encrypted fields carry a _sops suffix in the connector configs.
    token = creds.get("personal_access_token") or creds.get("personal_access_token_sops")
    return cfg["address"].split(":")[0], warehouse, token


def run_statement(host, warehouse, token, statement, parameters):
    req = urllib.request.Request(
        f"https://{host}/api/2.0/sql/statements",
        data=json.dumps({
            "warehouse_id": warehouse,
            "statement": statement,
            "parameters": parameters,
            "wait_timeout": "50s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        body = json.load(resp)
    while body["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll = urllib.request.Request(
            f"https://{host}/api/2.0/sql/statements/{body['statement_id']}",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(poll) as resp:
            body = json.load(resp)
    if body["status"]["state"] != "SUCCEEDED":
        sys.exit(f"statement failed: {json.dumps(body['status'], indent=2)}")
    return body.get("result", {}).get("data_array", [])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--since", required=True, help="statements started at or after this UTC time")
    ap.add_argument("--until", default=None)
    ap.add_argument("--like", default="%", help="SQL LIKE pattern on statement_text")
    ap.add_argument("--types", default="SELECT,MERGE,COPY", help="comma-separated statement types")
    ap.add_argument("--json", action="store_true", help="print rows as JSON lines")
    args = ap.parse_args()

    host, warehouse, token = load_config(args.config)
    types = [t.strip().upper() for t in args.types.split(",") if t.strip()]
    statement = f"""
        SELECT {", ".join(COLUMNS)}
        FROM system.query.history
        WHERE start_time >= :since::timestamp
          AND start_time <  COALESCE(:until::timestamp, current_timestamp())
          AND statement_text LIKE :like
          AND statement_type IN ({", ".join(f"'{t}'" for t in types)})
        ORDER BY start_time
    """
    parameters = [
        {"name": "since", "value": args.since},
        {"name": "until", "value": args.until},
        {"name": "like", "value": args.like},
    ]
    rows = run_statement(host, warehouse, token, statement, parameters)

    if args.json:
        for r in rows:
            print(json.dumps(dict(zip(COLUMNS, r))))
        return

    print(f"{'statement_id':<36} {'start (UTC)':<20} {'type':<7} {'total s':>8} {'exec s':>7} {'files':>7} {'pruned':>7} {'GB read':>8} {'rows read':>13} {'produced':>9}  text")
    for r in rows:
        d = dict(zip(COLUMNS, r))
        num = lambda k: float(d[k] or 0)
        text = " ".join((d["statement_text"] or "").split())[:40]
        print(f"{d['statement_id']:<36} {(d['start_time'] or '')[:19]:<20} {d['statement_type']:<7} {num('total_duration_ms')/1000:>8.1f} "
              f"{num('execution_duration_ms')/1000:>7.1f} {int(num('read_files')):>7} {int(num('pruned_files')):>7} "
              f"{num('read_bytes')/1e9:>8.2f} {int(num('read_rows')):>13,} {int(num('produced_rows')):>9,}  {text}")


if __name__ == "__main__":
    main()
