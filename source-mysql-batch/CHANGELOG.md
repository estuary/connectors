# source-mysql-batch

## 2026-09-29

### Changed
- An unset `sslmode` now defaults to `required` rather than `preferred`, so a
  connection must be encrypted and never falls back to plaintext. Capture tasks
  that were relying on the previous `preferred` default before this change have
  been updated to set `sslmode: preferred` explicitly, preserving their behavior.
  A new capture against a server without TLS must set `sslmode` explicitly.
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
  configured hostname. When unset the connector keeps its previous behaviour
  of attempting TLS without verification and falling back to an unencrypted
  connection.

## v1, 2023-09-28
- Beginning of changelog.
