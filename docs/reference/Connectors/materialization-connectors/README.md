---
description: Browse Estuary's list of materialization connectors for warehouses, databases, data lakes, and streaming destinations with complete configuration details.
---

# Materialization Connectors

Estuary's available materialization connectors are listed in this section. Each connector has a unique set of requirements for configuration; follow the connector link for a full reference on the system's requirements, properties, and best practices.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

At this time, all the available materialization connectors are created by Estuary.
In the future, other open-source materialization connectors from third parties could be supported.

## Available materialization connectors

### Data warehouse and OLAP connectors

* [Amazon Redshift](./amazon-redshift.md)
* [Azure Fabric Warehouse](./azure-fabric-warehouse.md)
* [ClickHouse](./ClickHouse.md)
* [Databricks](./databricks.md)
* [Google BigQuery](./BigQuery.md)
* [MotherDuck](./motherduck.md)
* [Snowflake](./Snowflake.md)

### Database connectors

* [AlloyDB](./alloydb.md)
* [Amazon DynamoDB](./amazon-dynamodb.md)
* [Amazon MySQL](./MySQL/amazon-rds-mysql.md)
* [Amazon PostgreSQL](./PostgreSQL/amazon-rds-postgres.md)
* [Amazon SQL Server](./SQLServer/amazon-rds-sqlserver.md)
* [Azure SQL Server](./SQLServer/)
* [Google Cloud Bigtable](./google-bigtable.md)
* [Google Cloud MySQL](./MySQL/google-cloud-sql-mysql.md)
* [Google Cloud PostgreSQL](./PostgreSQL/google-cloud-sql-postgres.md)
* [Google Cloud SQL Server](./SQLServer/google-cloud-sql-sqlserver.md)
* [Google Spanner](./google-spanner.md)
* [MongoDB](./mongodb.md)
* [MySQL](./MySQL/)
* [MySQL Heatwave](./mysql-heatwave.md)
* [Pinecone](./pinecone.md)
* [PostgreSQL](./PostgreSQL/)
* [SingleStore](./MySQL/singlestore-mysql.md)
* [SQLite](./SQLite.md)
* [SQL Server](./SQLServer/)
* [Supabase](./PostgreSQL/supabase.md)
* [TimescaleDB](./timescaledb.md)

### Lakehouse connectors

* [Apache Iceberg Tables](./apache-iceberg/apache-iceberg.md)
* [Apache Iceberg Tables in S3 (delta updates)](./amazon-s3-iceberg.md)
* [Bauplan](./apache-iceberg/bauplan.md)
* [Dremio](./apache-iceberg/dremio.md)

### File connectors

* [Apache Parquet Files in Azure Blob Storage](./azure-blob-parquet.md)
* [Apache Parquet Files in GCS](./google-gcs-parquet.md)
* [Apache Parquet Files in S3](./amazon-s3-parquet.md)
* [CSV Files in GCS](./google-gcs-csv.md)
* [CSV Files in S3](./amazon-s3-csv.md)
* [Google Sheets](./Google-sheets.md)

### Event connectors

Integrations with streaming services, queues, and event-based systems outside
of Estuary's [Dekaf connectors](#dekaf-connectors).

* [Amazon EventBridge](./amazon-eventbridge.md)
* [Amazon SNS](./amazon-sns.md)
* [Apache Kafka](./apache-kafka.md)
* [Elasticsearch](./Elasticsearch/Elasticsearch.md)
* [Google Cloud Pub/Sub](./google-pubsub.md)
* [HTTP Webhook](./http-webhook.md)
* [OpenSearch](./Elasticsearch/opensearch.md)

### SaaS/reverse ETL connectors

* [HubSpot](./hubspot.md)
* [Slack](./slack.md)

### Dekaf connectors

[Dekaf](../dekaf/) is Estuary's Kafka-compatible API. Materializations using
Dekaf send data to destinations via the destination's Kafka integration.

* [Bytewax](./Dekaf/bytewax.md)
* [ClickHouse (Dekaf)](./Dekaf/clickhouse.md)
* [Dekaf](./Dekaf/dekaf.md)
* [Imply Polaris](./Dekaf/imply-polaris.md)
* [Materialize](./Dekaf/materialize.md)
* [SingleStore (Dekaf)](./Dekaf/singlestore.md)
* [Startree](./Dekaf/startree.md)
* [Tinybird](./Dekaf/tinybird.md)
