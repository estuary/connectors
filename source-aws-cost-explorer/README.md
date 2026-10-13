# AWS Cost Explorer

Captures `daily_costs`, `daily_costs_by_service`, and `daily_costs_by_linked_account` with daily natural-key upserts. Every poll refreshes all configured history still available from AWS. Older and omitted rows retain their last captured values; the connector never infers deletions from missing data.

See [connector documentation](../docs/reference/Connectors/capture-connectors/aws-cost-explorer.md) for IAM permissions, fields, retention, and API costs.

## Local credentials

The committed `config.yaml` holds placeholder credentials, which is enough for the spec and discovery tests. For a live capture, replace it locally with real credentials in this shape, and don't commit them:

```yaml
credentials:
  credentials_title: AWS Access Key
  access_key_id_sops: <access-key-id>
  secret_access_key_sops: <secret-access-key>
  # session_token_sops: <temporary-session-token>
# Optional; when omitted, each poll rereads the last 30 days.
# start_date: 2026-01-01T00:00:00Z
```

Encrypt credential fields using the repository's SOPS key before running Flow:

```shell
sops --encrypt \
  --gcp-kms projects/estuary-theatre/locations/us-central1/keyRings/connector-keyring/cryptoKeys/connector-repository \
  --encrypted-suffix _sops \
  --in-place config.yaml
```

Flow decrypts the endpoint configuration and strips the `_sops` suffix. No credentials or pagination tokens are stored in connector state.

## Tests

```shell
poetry install
poetry run pytest
```

Tests cover spec/discovery, explicit credentials, API error handling, pagination, decimal precision, grouping, UTC dates, and preservation of expired history. Discovery is static and the committed test suite makes no Cost Explorer calls. A real capture incurs AWS API charges.
