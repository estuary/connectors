# GraphQL

This subpackage provides the `BulkJobManager` for [executing bulk query jobs](https://shopify.dev/docs/api/usage/bulk-operations/queries) via Shopify's GraphQL API.

## `BulkJobManager`

The `BulkJobManager` is responsible for submitting queries, checking the status of those queries, and retrieving the URL containing query results. Parsing query results is handled by the `process_result` static method defined on each stream's Pydantic model. The `BulkJobManager` can also cancel queries, although that functionality is currently only used upon connector start up to clean up ongoing queries between connector restarts.


## `{stream_name}.py`

Since fields must be explicitly specific in GraphQL queries, each stream has a dedicated `{stream_name}.py` that contains the stream-specific GraphQL query listing out all of the fields and the query-specific parsing logic of the JSONL result.

Shopify places [restrictions](https://shopify.dev/docs/api/usage/bulk-operations/queries#operation-restrictions) on bulk queries. Consult Shopify's API documentation to determine what fields are available for a specific stream. Bulk job errors are not extremely specific, so testing a query before trying to execute it via the bulk API is recommended.

Shopify places query results into a [JSONL file](https://shopify.dev/docs/api/usage/bulk-operations/queries#the-jsonl-data-format), and nested connections (ex: variants of a product) appear on separate lines after their parents. Result parsing functions read the JSONL file one line at a time and rely on the order of nested connections to build up records before yielding them.
