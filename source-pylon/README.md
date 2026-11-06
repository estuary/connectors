# Pylon

Captures Pylon issues incrementally, and issue messages, accounts, contacts, users, teams, tags, and custom fields as snapshots.

See [connector documentation](../docs/reference/Connectors/capture-connectors/pylon.md) for streams, intervals, and replication behavior.

## Local credentials

The only Pylon account available for testing is a real production one, so the committed `config.yaml` holds a placeholder API key, which is enough for the spec and discovery tests. For a live capture, replace it locally with a real key in this shape, and don't commit it:

```yaml
credentials:
  credentials_title: API Key
  access_token_sops: <api-key>
region: US
# Optional; when omitted, issues backfill from 30 days ago.
# start_date: 2026-01-01T00:00:00Z
```

Encrypt credential fields using the repository's SOPS key before running Flow:

```shell
sops --encrypt \
  --gcp-kms projects/estuary-theatre/locations/us-central1/keyRings/connector-keyring/cryptoKeys/connector-repository \
  --encrypted-suffix _sops \
  --in-place config.yaml
```

The connector only issues GET requests and the read-only `POST /issues/search`. Keep it that way against any real account.

## Tests

```shell
uv sync --group dev
uv run --group dev pytest
```

The offline tests in `tests/test_capture.py` cover pagination, cursor handling, issue windows, and message fan-out, without loading credentials or calling Pylon.
