# source-mysql-batch

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
