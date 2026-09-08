---
description: Use the Hyperline connector to sync customers, invoices, subscriptions, quotes, products, transactions, audit logs, and more, using API key authentication.
---

# Hyperline

This connector captures data from [Hyperline](https://www.hyperline.co/) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [audit_logs](https://docs.hyperline.co/api-reference/endpoints/audit-logs/list-audit-logs) | Incremental |
| [coupons](https://docs.hyperline.co/api-reference/endpoints/coupons/list-coupons) | Full Refresh |
| [customer_credits](https://docs.hyperline.co/api-reference/endpoints/customers/list-customers-credits) | Incremental |
| [customer_segments](https://docs.hyperline.co/api-reference/endpoints/customers/list-customer-segments) | Full Refresh |
| [customers](https://docs.hyperline.co/api-reference/endpoints/customers/list-customers-1) | Incremental |
| [features](https://docs.hyperline.co/api-reference/endpoints/features/list-features) | Full Refresh |
| [invoices](https://docs.hyperline.co/api-reference/endpoints/invoices/list-invoices-1) | Incremental |
| [invoicing_entities](https://docs.hyperline.co/api-reference/endpoints/invoicing-entities/list-invoicing-entities) | Full Refresh |
| [price_books](https://docs.hyperline.co/api-reference/endpoints/price-books/list-price-books) | Full Refresh |
| [price_configurations](https://docs.hyperline.co/api-reference/endpoints/price-configurations/list-price-configurations) | Full Refresh |
| [products](https://docs.hyperline.co/api-reference/endpoints/products/list-products) | Full Refresh |
| [promotion_codes](https://docs.hyperline.co/api-reference/endpoints/coupons/list-promotion-codes) | Full Refresh |
| [quotes](https://docs.hyperline.co/api-reference/endpoints/quotes/list-quotes) | Incremental |
| [subscription_transitions](https://docs.hyperline.co/api-reference/endpoints/subscriptions/list-subscription-transitions) | Full Refresh |
| [subscriptions](https://docs.hyperline.co/api-reference/endpoints/subscriptions/list-subscriptions) | Incremental |
| [tax_rates](https://docs.hyperline.co/api-reference/endpoints/taxes/list-tax-rates) | Full Refresh |
| [transactions](https://docs.hyperline.co/api-reference/endpoints/transactions/list-transactions) | Full Refresh |
| [users](https://docs.hyperline.co/api-reference/endpoints/users/list-users) | Full Refresh |
| [wallets](https://docs.hyperline.co/api-reference/endpoints/wallets/list-wallets) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
Audit-log events carry no unique identifier from Hyperline. The connector computes a synthetic ID `_meta/estuary_id` by hashing each event's `type`, `happened_at`, and associated entity ids to deduplicate events.

The `transactions` resource is captured with periodic full refreshes because Hyperline's transactions endpoint offers no change-tracking filter and transactions mutate in place (e.g. status transitions and refunds).
:::

## Prerequisites

To set up the Hyperline source connector, you'll need a Hyperline [API key](https://docs.hyperline.co/api-reference/docs/authentication) (created in your workspace under **Settings → API**). Keys prefixed with `prod_` capture from the production environment (`api.hyperline.co`); keys prefixed with `test_` capture from the sandbox environment (`sandbox.api.hyperline.co`) — the connector routes automatically based on the key prefix.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Hyperline source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/access_token`** | API Key | The Hyperline API key. | string | Required |
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `API Key`. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. Any data generated before this date will not be replicated. If left blank, the start date will be set to 30 days before the present date. | string | |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | PT5M |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hyperline:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
          start_date: 2024-01-01T00:00:00Z
    bindings:
      - resource:
          name: customers
        target: ${PREFIX}/customers
      - resource:
          name: invoices
        target: ${PREFIX}/invoices
      - resource:
          name: subscriptions
        target: ${PREFIX}/subscriptions
      {...}
```
