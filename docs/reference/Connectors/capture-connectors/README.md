---
description: Browse Estuary's list of capture connectors for databases, SaaS apps, files, streams, and APIs, with complete configuration details.
---

# Capture Connectors

Estuary's available capture connectors are listed in this section. Each connector has a unique set of requirements for configuration; follow the connector link for a full reference on the system's requirements, properties, and best practices.

Estuary is actively developing new connectors, so check back regularly for the latest additions. We’re prioritizing the development of high-scale technological systems, as well as client needs.

## Available capture connectors

### SQL CDC connectors

The SQL CDC group of database connectors share some common features, from
real-time CDC ingestion to [extra backfill options](/reference/backfilling-data/#resource-configuration-backfill-modes)
and other [advanced features](/guides/customize-dataflows/#sql-captures).

- [AlloyDB](./alloydb.md)
- [Amazon RDS SQL Server](./SQLServer/amazon-rds-sqlserver.md)
- [Azure SQL Server](./SQLServer/)
- [Google Cloud SQL Server](./SQLServer/google-cloud-sql-sqlserver.md)
- [MariaDB](./MariaDB/)
- [Microsoft SQL Server](./SQLServer/)
- [MySQL](./MySQL/)
- [OracleDB](./OracleDB/)
- [PostgreSQL](./PostgreSQL/)

### Other database connectors

Besides [SQL CDC](#sql-cdc-connectors) databases, Estuary also provides capture
connectors for batch and NoSQL databases.

- [Amazon DocumentDB](./MongoDB/amazon-documentdb.md)
- [Amazon Dynamodb](./amazon-dynamodb.md)
- [Azure Cosmos DB](./MongoDB/azure-cosmosdb.md)
- [Google Firestore](./google-firestore.md)
- [IBM Db2 Batch](./db2-batch.md)
- [Microsoft SQL Server Batch](./SQLServer/sqlserver-batch.md)
- [MongoDB](./MongoDB/mongodb.md)
- [MySQL Batch](./MySQL/mysql-batch.md)
- [OracleDB Batch](./OracleDB/oracle-batch.md)
- [PostgreSQL Batch](./PostgreSQL/postgres-batch.md)

### Data warehouse connectors

- [Amazon Redshift](./redshift-batch.md)
- [BigQuery](./bigquery-batch.md)
- [Snowflake](./snowflake.md)

### File connectors

File connectors share features like parser configuration. Except for the
spreadsheet-specific connectors, supported file types include Avro, JSON, CSV,
Protobuf, W3C Extended Log, and Parquet.

- [Amazon S3](./amazon-s3.md)
- [Azure Blob Storage](./azure-blob-storage.md)
- [Dropbox](./dropbox.md)
- [Google Cloud Storage](./gcs.md)
- [Google Drive](./google-drive.md)
- [Google Sheets](./google-sheets.md)
- [HTTP file](./http-file.md)
- [OneDrive](./onedrive.md)
- [SFTP](./sftp.md)
- [SharePoint](./sharepoint.md)
- [Smartsheet](./smartsheet.md)

### Event connectors

Integrations with streaming systems, queue services, and webhooks.

- [Amazon Kinesis](./amazon-kinesis.md)
- [Amazon SQS](./amazon-sqs-native.md)
- [Apache Kafka](./apache-kafka.md)
- [Datadog ingest (webhook)](./http-ingest/datadog-ingest.md)
- [Google Cloud Pub/Sub](./google-pubsub.md)
- [HTTP ingest (webhook)](./http-ingest/http-ingest.md)
- [Intercom ingest (webhook)](./http-ingest/intercom-ingest.md)
- [Jira ingest (webhook)](./http-ingest/jira-ingest.md)
- [Twilio ingest (webhook)](./http-ingest/twilio-ingest.md)

### SaaS connectors

#### Marketing, ads, and socials

- [AppsFlyer](./appsflyer.md)
- [Brevo](./brevo.md)
- [Criteo](./criteo.md)
- [Facebook Marketing](./facebook-marketing-native.md)
- [Google Ads](./google-ads.md)
- [Impact](./impact.md)
- [Iterable](./iterable-native.md)
- [Klaviyo](./klaviyo-native.md)
- [LinkedIn Pages](./linkedin-pages.md)
- [Mailchimp](./mailchimp-native.md)

#### BI and analytics

- [Google Analytics BigQuery Exports](./google-analytics-4-bigquery-exports.md)
- [Google Analytics Data API](./google-analytics-data-api-native.md)
- [Looker](./looker.md)
- [NetSuite SuiteAnalytics](./netsuite-suiteanalytics.md)
- [NetSuite SuiteQL](./netsuite-suiteql.md)

#### CRMs, sales, and support

- [Ada](./ada.md)
- [Front](./front.md)
- [Gainsight NXT](./gainsight-nxt.md)
- [Genesys](./genesys.md)
- [Gong](./gong.md)
- [Hubspot](./HubSpot-real-time.md)
- [Intercom](./intercom-native.md)
- [Outreach](./outreach.md)
- [RingCentral](./ringcentral.md)
- [Salesforce](./Salesforce/salesforce-native.md)
- [Zendesk Chat](./zendesk-chat.md)
- [Zendesk Support](./zendesk-support-native.md)
- [Zoho CRM](./zoho-crm.md)

#### Commerce and payments

- [Alpaca](./alpaca.md)
- [Braintree](./braintree.md)
- [Chargebee](./chargebee-native.md)
- [Commercetools](./commercetools.md)
- [Gladly](./gladly.md)
- [QuickBooks](./quickbooks.md)
- [Shopify](./shopify-native.md)
- [Stripe](./stripe-realtime.md)
- [Zuora](./zuora.md)

#### Finance, hiring, and operations

- [Ashby](./ashby.md)
- [Greenhouse](./greenhouse-native.md)
- [Microsoft Dynamics 365 Finance and Operations](./dynamics-365-finance-and-operations.md)
- [Sage Intacct](./sage-intacct.md)

#### Workflow and incidents

- [Airtable](./airtable-native.md)
- [Asana](./asana.md)
- [Calendly](./calendly.md)
- [Datadog](./datadog.md)
- [GitHub](./github.md)
- [Incident.io](./incident-io.md)
- [Jira](./jira-native.md)
- [Monday](./monday.md)
- [Navan](./navan.md)
- [Sentry](./sentry.md)

#### Other applications

- [Apple App Store](./apple-app-store.md)
- [Google Play](./google-play.md)
- [Iterate](./iterate.md)
- [Pendo](./pendo.md)
- [Qualtrics](./qualtrics.md)
- [Twilio](./twilio.md)

### Third party connectors

Estuary supports open-source connectors from third parties. These connectors operate in a **batch** fashion,
capturing data in increments. When you run these connectors in Estuary, you'll get as close to real time as possible
within the limitations set by the connector itself.

Typically, we enable SaaS connectors from third parties to allow more diverse data flows.

- [Aircall](./aircall.md)
- [Amazon Ads](./amazon-ads.md)
- [Amplitude](./amplitude.md)
- [Bing Ads](./bing-ads.md)
- [Braze](./braze.md)
- [Confluence](./confluence.md)
- [Exchange Rates API](./exchange-rates.md)
- [Freshdesk](./freshdesk.md)
- [GitLab](./gitlab.md)
- [Google Analytics 4](./google-analytics-4.md)
- [Google Universal Analytics](./google-analytics.md)
- [Google Search Console](./google-search-console.md)
- [Harvest](./harvest.md)
- [Instagram](./instagram.md)
- [LinkedIn Ads](./linkedin-ads.md)
- [Marketo](./marketo.md)
- [MixPanel](./mixpanel.md)
- [Notion](./notion.md)
- [Paypal Transaction](./paypal-transaction.md)
- [Pinterest](./pinterest.md)
- [Recharge](./recharge.md)
- [SendGrid](./sendgrid.md)
- [Slack](./slack.md)
- [Snapchat](./snapchat.md)
- [SurveyMonkey](./survey-monkey.md)
- [TikTok Marketing](./tiktok.md)
- [WooCommerce](./woocommerce.md)
- [YouTube Analytics](./youtube-analytics.md)
