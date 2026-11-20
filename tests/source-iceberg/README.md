# Apache Iceberg capture test

Run `CONNECTOR=source-iceberg VERSION=local ./tests/run.sh` after building the connector image. Setup starts its own REST catalog and S3-compatible store on `flow-test`, then runs the seeder using the built image's Python dependencies. Cleanup removes both services and their data. No external credentials or host port forwarding are needed.

The fixture has two append snapshots with five rows across two Parquet files. Snapshot IDs and data file paths are fixed so the checked-in snapshot compares all document fields, including `_meta`. Rows cover UTC timestamps, fractional timestamps, decimals, binary values, and nulls. The connector's offline tests read the same fixture and check the expected documents and resumed capture state.

The REST catalog service sets `CATALOG_URI=jdbc:sqlite:/tmp/source-iceberg-catalog.db` so its pooled JDBC connections share a file-backed database. Keep this setting when creating another fixture. SQLite errors such as `no such table: iceberg_tables` indicate a catalog backend problem; a successful `/v1/config` health check does not establish that table metadata can be loaded. Check the REST service logs and database configuration, then recreate and seed the fixture before retrying the capture. The connector only reads the catalog and cannot repair its database.
