# source-mysql

## 2026-09-23

### Changed
- An unset `sslmode` now defaults to `verify_identity` rather than `preferred`,
  so a connection must be encrypted and the server's certificate must be valid
  for the configured host. Existing captures are unaffected because they pin
  `preferred` explicitly. A new capture against a server without TLS, or with
  a certificate that can't be verified, must set `sslmode` explicitly.
- With `ssl_server_ca` empty, `verify_identity` trusts the CAs of Amazon RDS
  (commercial and GovCloud regions) and Google Cloud SQL's shared CA, in
  addition to public certificate authorities.
- A certificate verification failure now explains whether the certificate's
  issuer, host name, or validity was rejected, and which setting fixes it.

## 2026-09-17

### Added
- New `sslmode` advanced option (`disabled`, `preferred`, `required`,
  `verify_ca`, `verify_identity`) with accompanying `ssl_server_ca`,
  `ssl_client_cert`, and `ssl_client_key` options, so that the server's TLS
  certificate can be verified against a CA and, with `verify_identity`, the
  configured hostname. Verification applies to both query and binlog
  replication connections and works through SSH network tunnels.

## 2026-09-04

### Added
- MariaDB system-versioned tables can be discovered and captured, including
  historical row versions, when the `system_versioned_tables` feature flag
  is set.

## 2026-09-03

### Added
- The `credentials` configuration union now also supports Google Cloud IAM
  authentication, for Cloud SQL for MySQL instances with the
  `cloudsql_iam_authentication` flag enabled. The access token obtained through
  the workload identity pool is presented as the database password.

## 2026-09-01

### Added
- The `credentials` configuration union now also supports AWS IAM
  authentication, for Amazon RDS and Aurora instances with IAM database
  authentication enabled. A fresh RDS auth token is minted from the assumed
  role's session credentials for each connection attempt.

## 2026-08-28

### Added
- New `credentials` configuration union supporting username/password and Azure
  IAM authentication, the latter using an Entra access token obtained through
  an Azure App Registration. Existing configs with the legacy top-level
  `password` field keep working and are folded into the new shape
  automatically.

## 2026-08-18

### Added
- New `additional_backfill_filter` advanced option on each binding. When set,
  the filter clause is applied to all backfill queries for that table, so rows
  which the filter excludes are never backfilled. Setting or changing the
  filter requires re-backfilling the binding, while clearing it does not.
  Filters cannot be combined with the `Precise` backfill mode.

## 2026-07-30

### Added
- New `rediscovery_interval` advanced option controls how often the connector
  re-runs discovery while a capture is running, to notice schema changes and
  newly added tables. It defaults to 15 minutes.

### Changed
- Captures no longer run discovery twice when they start up.
- The timing of mid-capture rediscovery is now spread out rather than fixed, so
  captures which started at the same moment no longer query the catalog in
  lockstep every interval. The average rate of rediscovery is unchanged.

