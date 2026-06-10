# GraphQL

This subpackage handles GraphQL access to Shopify's Admin API: [bulk query jobs](https://shopify.dev/docs/api/usage/bulk-operations/queries) via the `BulkJobManager`, and standard queries via the `ShopifyGraphQLClient`.

## `BulkJobManager`

The `BulkJobManager` is responsible for submitting queries, checking the status of those queries, and retrieving the URL containing query results. Parsing query results is handled by the `process_result` static method defined on each stream's Pydantic model. The `BulkJobManager` can also cancel queries, although that functionality is currently only used upon connector start up to clean up ongoing queries between connector restarts.


## `{stream_name}.py`

Since fields must be explicitly specific in GraphQL queries, each stream has a dedicated `{stream_name}.py` that contains the stream-specific GraphQL query listing out all of the fields and the query-specific parsing logic of the JSONL result.

Shopify places [restrictions](https://shopify.dev/docs/api/usage/bulk-operations/queries#operation-restrictions) on bulk queries. Consult Shopify's API documentation to determine what fields are available for a specific stream. Bulk job errors are not extremely specific, so testing a query before trying to execute it via the bulk API is recommended.

Shopify places query results into a [JSONL file](https://shopify.dev/docs/api/usage/bulk-operations/queries#the-jsonl-data-format), and nested connections (ex: variants of a product) appear on separate lines after their parents. Result parsing functions read the JSONL file one line at a time and rely on the order of nested connections to build up records before yielding them.


## Nested connection resolution

Bulk operations reject a connection nested inside a list, so a `Refund`'s `refundLineItems` (under the `Order.refunds` list) or a `Fulfillment`'s `fulfillmentLineItems` (under `Order.fulfillments`) can't be captured in bulk. Streams that need such connections run on the non-bulk path and declare them as `NESTED_CONNECTIONS` on their Pydantic model, resolved by `nested_resolver.py`:

- The connection's first page is spliced inline into the outer query, at a `# {{ field }}` placeholder in the stream's `QUERY` (or, for a connection nested inside another, in that connection's `node_selection`). Empirical testing has shown a list-nested connection's `first` isn't multiplied across the outer page, so this is somewhat cheap - most parents have few children and are captured fully inline with no extra requests. Streams lower `OUTER_PAGE_SIZE` to keep the inline query's cost under Shopify's per-query ceiling.
- Only when a parent's inline page reports `hasNextPage` does the resolver page the remainder, via focused `node(id:)` re-queries seeded from the inline cursor (`overflow_page_size` sizes these independently of the inline page).

The resolver flattens each connection into a plain list under the field name, stripping all `edges`/`pageInfo`/cursor metadata, and mutates the parent documents in place.

Each `NestedConnection` declares a `parent_path` locating the parent object(s) carrying it, so a connection anywhere in the document can be resolved. For example, `["refunds"]` is each object in the `refunds` list, `[]` is the document node itself, and a multi-step path descends fields, fanning out over any list it meets. Two rules follow from the `node(id:)` drain: every parent must be a `Node` with an `id` and connections resolve in declaration order - resolving one flattens it to a list, so a connection nested inside another must be declared after the connection it sits within.
