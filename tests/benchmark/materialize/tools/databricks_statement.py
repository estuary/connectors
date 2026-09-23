"""Run one SQL statement on the connector config's warehouse and print rows.

Usage:
  databricks_statement.py --config CONFIG (--sql "..." | --sql-file PATH)

Companion to databricks_query_history.py, for EXPLAIN and ad-hoc queries
while investigating a benchmark run.
"""

import argparse
import sys

from databricks_query_history import load_config, run_statement


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--sql")
    ap.add_argument("--sql-file")
    args = ap.parse_args()
    sql = args.sql if args.sql is not None else open(args.sql_file).read()
    host, warehouse, token = load_config(args.config)
    for row in run_statement(host, warehouse, token, sql, []):
        print("\t".join("" if v is None else str(v) for v in row))


if __name__ == "__main__":
    sys.path.insert(0, __file__.rsplit("/", 1)[0])
    main()
