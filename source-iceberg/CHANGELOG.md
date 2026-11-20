# Changelog

## 2026-11-11

### Added
- Initial release of the Apache Iceberg capture connector.
- AWS SigV4 REST catalog authentication for Glue Data Catalog and S3 Tables, including AWS IAM roles for catalog and S3 access.
- OAuth2 scope and token server URI configuration for Polaris, Snowflake Open Catalog, and external token endpoints.
- Remote S3 signing authenticated with the catalog's OAuth client credential session.
- Capture Parquet, ORC, and Avro data files, including position delete files in all three formats.
- Read imported ORC and Avro data files without field IDs through the table's Iceberg name mapping.
- Schema evolution support for renamed struct fields inside nested lists and maps when reading older data files.
- Skip incremental replace snapshots, including compaction, without re-emitting rows; still read them during initial backfills.
- Discovery skips tables and namespaces that it cannot read or does not support.
- Reload catalog-supplied storage credentials during long reads and after an expired token.
- Decode each position delete file once per snapshot, even when it applies to many data files.
- Checkpoint every 100,000 rows within a data file, so a restart resumes mid-file.
- Report catalog connection, authentication, and permission failures as configuration errors.
- Timestamps and dates in years 0000 through 9999 are captured; values outside that range fail with the column, file, and row.
- `catalog_auth`, `credentials`, and `s3` default to `auth_type: None`; an `s3` object with keys and no `auth_type` uses those keys.
