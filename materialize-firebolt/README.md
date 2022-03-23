materialize-firebolt
====================

Flow Materialization for [Firebolt](Firebolt.io). This materialization only supports delta-updates mode since that's the only supported mode by Firebolt.

This materialization creates a main table which can be either a `FACT` or a `DIMENSION` table (see Firebolt's documentation on [working with tables](https://docs.firebolt.io/working-with-tables.html)). Besides the main table, an `EXTERNAL` table will be created with the suffix `_external` which will serve as an interface over the JSON files stored in the S3 bucket.

The S3 bucket will get JSON documents uploaded to it, as well as special `flow.materialization_spec` files which are used by Flow to keep track of changes in materialization specification.

On a high-level, these are the steps taken by materialize-firebolt:

1. Validate the collection schema and projections to make sure the data can be materialized to Firebolt
2. Create a main Firebolt table according the collection schema and projections if necessary
3. Create an external Firebolt table according the collection schema and projections if necessary
4. For documents in a transaction, add them all as JSON documents to a single file (see [Source JSON record structure](https://docs.firebolt.io/working-with-semi-structured-data/mapping-json-to-table.html#source-json-record-structure)), and upload the file to S3
5. Once the changes have been committed to Flow runtime, move the data from the external table to the main table
