# Apache Iceberg capture tests

From the repository root, run:

```sh
uv run --no-sync pytest source-iceberg
```

The snapshot tests use a local REST catalog and storage. A session-scoped pytest fixture starts `docker-compose.yaml` with `docker compose up --wait` under a unique project name and lets Docker assign free host ports. It seeds `test.orders`, then runs capture and discovery against a temporary copy of `test.flow.yaml` using those ports, both locally and in CI. The fixture tears down its project with `docker compose down -v` after the session, leaving other running fixtures alone. Spec and discovery snapshots are managed by pytest-insta.

Standalone Compose and `seed.py` still default to catalog port `8181` and storage port `9010`.

`seed.py` writes five rows in two Parquet files and two append snapshots, with stable snapshot IDs and file paths. `fixtures/capture.jsonl` checks every document field, including metadata, timestamps, decimals, binary values, and nulls. Offline capture tests use the same data and verify checkpoint resume without duplicates.
