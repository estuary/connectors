---
description: Use the Amazon Selling Partner connector to capture a seller's orders, financial transactions, settlement reports, all-orders report rows and daily sales and traffic figures into Estuary. Authenticates with Login with Amazon credentials for a Selling Partner API application.
---

# Amazon Selling Partner

This connector captures data from an Amazon seller account into Estuary collections.
It reads the [Selling Partner API (SP-API)](https://developer-docs.amazon.com/sp-api/) with
Login with Amazon credentials: the Orders API, the Finances API and the Reports API.

## Supported data resources

| Stream                      | Description                                                                                  | Replication            | Key                         |
| --------------------------- | -------------------------------------------------------------------------------------------- | ---------------------- | --------------------------- |
| `orders`                    | Orders, with their items, proceeds, expenses, promotions, cancellation, fulfillment, packages, tax and payment sections. | Incremental + backfill | `/orderId`                  |
| `financial_transactions`    | Transactions from the Finances API.                                                          | Incremental + backfill | `/transactionId`            |
| `settlement_reports`        | Each row of the settlement reports Amazon generates for the account.                         | Incremental + backfill | `/reportId`, `/rowNumber`   |
| `all_orders`                | Each row of Amazon's all-orders report (orders by last update), with every column.           | Incremental + backfill | `/reportId`, `/rowNumber`   |
| `sales_and_traffic_by_date` | Daily catalog-wide sales and traffic totals for each marketplace, from the Sales and Traffic report. Discovered disabled. | Incremental + backfill | `/marketplaceId`, `/date`   |

`orders` keeps the copy of each order with the latest `lastUpdatedTime`, and
`sales_and_traffic_by_date` keeps the copy of each day from the newest report, whatever order
the backfill and the incremental sync commit them in.

Only keys, cursors and the fields the connector uses are declared on the write schema; the rest
is filled in by schema inference.

## Limitations

- **`all_orders` rows repeat once per report.** Rows are keyed by report and row number, so an
  order line appears once for each report that includes it, including the hour each daily poll
  reads again. A line's current state is in its newest report (latest `reportCreatedTime`).
  Summing rows per order overcounts.
- **`all_orders` keeps every column**, including the buyer's `ship-city`, `ship-state`,
  `ship-postal-code`, `ship-country` and `cpf` (Brazilian taxpayer number). To leave any of them
  out of your destination, deselect them with the materialization's field selection.
- **`all_orders` needs an interval of at least one hour** and cannot be re-backfilled on a
  schedule, since each re-read adds new copies of its rows. `createReport` accepts at most 25
  marketplace IDs for it.
- **`sales_and_traffic_by_date` needs the Brand Analytics role** on your SP-API application, so it
  is discovered disabled. Enable it only if the application has that role. Each daily poll reads
  the last 30 complete days again.
- **Settlement reports are kept by Amazon for 90 days.** Older settlement reports cannot be
  captured, whatever the start date. Each poll lists the last three days of reports again, to
  catch reports Amazon lists late; a report listed more than three days after it was created is
  captured only by backfilling the binding while Amazon still keeps it.
- **`orders` does not request the `BUYER`, `RECIPIENT` or `FULFILLMENT_ORDERS` sections.** Buyer
  and recipient sections carry buyer personal data, which Amazon releases only to applications
  approved for restricted roles. The fulfillment-orders section holds fulfillment order IDs for
  Easy Ship and Seller Flex orders.
- Amazon serves about two years of order history, all-orders reports and Sales and Traffic
  reports. Data older than that is not captured.
- Each region (North America, Europe, Far East) is captured separately. Use one capture per region.

## Rate limits

Amazon limits each SP-API operation separately, with a steady rate and a burst. The connector
paces each operation to Amazon's published default limits, for example:

| Operation                  | Rate                  | Burst |
| -------------------------- | --------------------- | ----- |
| `searchOrders`             | 0.0056 per second     | 20    |
| `listTransactions`         | 0.5 per second        | 10    |
| `getReports`               | 0.0222 per second     | 10    |
| `createReport`             | 0.0167 per second     | 15    |
| `getReport`                | 2 per second          | 15    |
| `getReportDocument`        | 0.0167 per second     | 15    |

A request Amazon still rejects as throttled is retried after a wait. Shorter binding intervals use
more of each budget.

## Prerequisites

- An Amazon seller account (Seller Central) and its marketplace IDs.
- A Selling Partner API application registered for that account, with these roles:
  - **Inventory and Order Tracking** for `orders` and `all_orders`.
  - **Finance and Accounting** for `financial_transactions` and `settlement_reports`.
  - **Brand Analytics** for `sales_and_traffic_by_date`.
- The application's Login with Amazon client ID and client secret, and a refresh token. See
  [Authentication](#authentication).

## Authentication

The connector authenticates with Login with Amazon (LWA). To get the credentials:

1. In Seller Central, open the developer console and register an SP-API application with the
   roles listed under [Prerequisites](#prerequisites).
2. Copy the application's **LWA client ID** and **LWA client secret**.
3. Authorize the application for your seller account (self-authorization) and copy the
   **refresh token** Amazon shows.

See Amazon's [self-authorization guide](https://developer-docs.amazon.com/sp-api/docs/self-authorization).

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Amazon Selling Partner source connector.

### Properties

#### Endpoint

| Property                                           | Title                            | Description                                                                                                                                  | Type    | Required/Default |
| -------------------------------------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/credentials`**                                 | Authentication                   | Login with Amazon credentials.                                                                                                               | object  | Required         |
| **`/credentials/client_id`**                       | LWA Client ID                    | The SP-API application's Login with Amazon client ID.                                                                                        | string  | Required         |
| **`/credentials/client_secret`**                   | LWA Client Secret                | The SP-API application's Login with Amazon client secret.                                                                                    | string  | Required         |
| **`/credentials/refresh_token`**                   | Refresh Token                    | The refresh token issued when the selling account authorized the application.                                                               | string  | Required         |
| **`/region`**                                      | Region                           | The SP-API region of the selling account: `North America`, `Europe` or `Far East`.                                                          | string  | Required         |
| **`/marketplace_ids`**                             | Marketplace IDs                  | Amazon marketplace IDs to capture, such as `ATVPDKIKX0DER` for Amazon.com. All must belong to the selected region.                           | array   | Required         |
| `/start_date`                                      | Start Date                       | UTC date and time from which to capture data. If blank, 30 days before the present, moving forward each time the capture restarts.          | string  | 30 days ago      |
| `/advanced/window_size`                            | Window Size                      | Size of the date windows orders and financial transactions are requested in, as an ISO 8601 duration.                                        | string  | P1D              |
| `/advanced/overlap`                                | Incremental Overlap              | How far each incremental request reaches back before the previous one ended, to catch orders Amazon makes searchable late.                   | string  | PT10M            |
| `/advanced/financial_transactions_lookback`        | Financial Transactions Lookback  | How far behind the present financial transactions are read a final time, to catch deferred transactions Amazon has since released.           | string  | P90D             |
| `/advanced/sandbox`                                | Use Sandbox                      | Send requests to Amazon's SP-API sandbox, which returns fixed example responses. For testing only.                                           | boolean | `false`          |

#### Bindings

| Property    | Title    | Description                                                                                                                         | Type   | Required/Default |
| ----------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Name     | Name of the resource to capture.                                                                                                    | string | Required         |
| `/interval` | Interval | Interval between data syncs for this resource. Defaults: `PT15M` for orders and financial transactions, `PT1H` for settlement reports, `P1D` for the other streams. | string | Varies           |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-amazon-selling-partner:v1
        config:
          credentials:
            credentials_title: Login with Amazon
            client_id: <secret>
            client_secret: <secret>
            refresh_token: <secret>
          region: North America
          marketplace_ids:
            - ATVPDKIKX0DER
          start_date: "2026-01-01T00:00:00Z"
    bindings:
      - resource:
          name: orders
          interval: PT15M
        target: ${PREFIX}/orders
      - resource:
          name: financial_transactions
          interval: PT15M
        target: ${PREFIX}/financial_transactions
      - resource:
          name: settlement_reports
          interval: PT1H
        target: ${PREFIX}/settlement_reports
      - resource:
          name: all_orders
          interval: P1D
        target: ${PREFIX}/all_orders
```
