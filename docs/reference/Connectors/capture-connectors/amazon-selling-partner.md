---
description: Capture Amazon seller orders, financial transactions, settlement reports, all-orders rows, daily sales and traffic totals, and catalog items into Estuary.
---

# Amazon Selling Partner

This connector captures data from an Amazon seller account into Estuary collections.
It reads the [Selling Partner API (SP-API)](https://developer-docs.amazon.com/sp-api/) with
Login with Amazon credentials: the Orders API, Finances API, Reports API and Catalog Items API.

## Supported data resources

| Stream                      | Description                                                                                  | Replication            | Key                         |
| --------------------------- | -------------------------------------------------------------------------------------------- | ---------------------- | --------------------------- |
| `orders`                    | Orders, with their items, proceeds, expenses, promotions, cancellation, fulfillment, packages, tax and payment sections. | Incremental + backfill | `/orderId`                  |
| `financial_transactions`    | Transactions from the Finances API.                                                          | Incremental + backfill | `/transactionId`            |
| `settlement_reports`        | Each row of the settlement reports Amazon generates for the account.                         | Incremental + backfill | `/reportId`, `/rowNumber`   |
| `all_orders`                | Each row of Amazon's all-orders report (orders by last update), with every column.           | Incremental + backfill | `/reportId`, `/rowNumber`   |
| `catalog_items`             | Listed ASIN catalog summaries, seller SKUs and listing status for each marketplace. | Full refresh | `/asin`, `/marketplaceId` |
| `sales_and_traffic_by_date` | Daily catalog-wide sales and traffic totals for each marketplace, from the Sales and Traffic report. Discovered disabled. | Incremental + backfill | `/marketplaceId`, `/date`   |

`orders` keeps the copy of each order with the latest `lastUpdatedTime`.
`sales_and_traffic_by_date` keeps each day's copy from the newest report.
These rules apply regardless of when backfill and incremental syncs commit.

The write schema declares keys and fields the connector uses.
Schema inference fills in other fields.

## Limitations

- **`all_orders` rows repeat once per report.** Rows are keyed by report and row number.
  An order line appears in each report that includes it, including the hour each daily poll reads again.
  A line's current state is in its newest report, by `reportCreatedTime`.
  Summing rows per order overcounts.
- **`all_orders` keeps every column**, including `ship-city`, `ship-state`, `ship-postal-code`, `ship-country` and `cpf` (Brazilian taxpayer number).
  To omit fields from your destination, deselect them with the materialization's field selection.
- **`all_orders` needs an interval of at least one hour** and cannot be backfilled on a schedule.
  Each backfill adds new copies of its rows.
  `createReport` accepts at most 25 marketplace IDs for it.
- **`sales_and_traffic_by_date` needs the Brand Analytics role**, so it is discovered disabled.
  Enable it only if the application has that role.
  It covers complete UTC days only.
  Each daily poll reads up to the last 30 complete days again.
- **Settlement reports are kept by Amazon for 90 days.** Older reports cannot be captured, whatever the start date.
  Each poll lists the last three days of reports again, to catch reports Amazon lists late.
  Reports listed later require backfilling the binding while Amazon still keeps them.
- **`orders` does not request the `BUYER`, `RECIPIENT` or `FULFILLMENT_ORDERS` sections.**
  Buyer and recipient sections carry personal data, which Amazon releases only to applications approved for restricted roles.
  The fulfillment-orders section holds fulfillment order IDs for Easy Ship and Seller Flex orders.
- Amazon serves about two years of order history, all-orders reports and Sales and Traffic reports.
  Data older than that is not captured.
- Each region (North America, Europe, Far East) is captured separately.
  Use one capture per region.

## Notes

- The first data row of each settlement report is a summary row, marked `summaryRow: true`.
  Settlement reports are not filtered by the configured marketplaces.
- Financial transactions have three read passes: realtime, late and lookback.
  Realtime stays 15 minutes behind the present.
  Late reads the same time again after 72 hours to catch late postings.
  Lookback reads it again after the configured delay to catch released deferred transactions.
  Changes after that final pass are not captured.
- The first orders backfill request waits about six minutes at the default rate.
  The rate bucket starts with one token and needs three for backfill, leaving two reserved for incremental requests.
  At 0.0056 tokens per second, two tokens take about 357 seconds to refill.
  A recent window's upper bound can add a wait: it must lag the present by 2 minutes and 30 seconds.
- Requested reports have a two-hour timeout while waiting for Amazon to finish.
  A timeout or `FATAL` status fails the capture.
  After two cancellations for the full requested range, the connector treats it as empty and skips it.
  This applies to `all_orders`, `sales_and_traffic_by_date` and the reports used by `catalog_items`.
  Settlement reports only read reports Amazon has already finished.
- `catalog_items` gets ASINs from the All Listings Report (`GET_MERCHANT_LISTINGS_ALL_DATA`) for each marketplace.
  It looks up catalog summaries with `searchCatalogItems` and includes seller SKUs, listing status and refresh time.
  It refreshes daily by default, and its interval must be at least three hours.
  ASINs the catalog lookup does not return are not captured.
  An ASIN that drops out of the All Listings Report is not deleted: its document keeps the values from its last refresh.
- Sandbox mode skips the marketplace participation check after testing credentials.
  If a configured marketplace is missing from the account's participation list, the connection test shows:

  ```text
  The selling account does not participate in marketplace(s) <IDs> in the <region> region. Marketplaces it participates in: <IDs or none>.
  ```

## Rate limits

Amazon limits each SP-API operation separately, with a steady rate and a burst.
The connector starts with these default limits and adopts rates Amazon reports in response headers:

| Operation                  | Rate                  | Burst |
| -------------------------- | --------------------- | ----- |
| `searchCatalogItems`       | 2 per second          | 2     |
| `searchOrders`             | 0.0056 per second     | 20    |
| `listTransactions`         | 0.5 per second        | 10    |
| `getReports`               | 0.0222 per second     | 10    |
| `createReport`             | 0.0167 per second     | 15    |
| `getReport`                | 2 per second          | 15    |
| `getReportDocument`        | 0.0167 per second     | 15    |

A request Amazon still rejects as throttled is retried after a wait.
Shorter binding intervals use more of each budget.

## Prerequisites

- An Amazon seller account (Seller Central) and its marketplace IDs.
- A Selling Partner API application authorized by that account, with these roles:
  - **Inventory and Order Tracking** for `orders` and `all_orders`. Amazon also accepts some
    other roles for the Orders API.
  - **Finance and Accounting** for `financial_transactions` and `settlement_reports`.
  - **Brand Analytics** for `sales_and_traffic_by_date`.
  - **Product Listing** for `catalog_items`, including the catalog lookup.
- The application's Login with Amazon client ID and client secret, and a refresh token.
  See [Authentication](#authentication).

The connection test checks credentials and marketplace participation, but does not test stream roles.
A missing stream role fails the binding at run time.
The Sales and Traffic error names the Brand Analytics role.
The catalog lookup's HTTP 403 error names Product Listing as the likely missing role.

## Authentication

The connector accepts **Login with Amazon** (LWA) credentials, which you enter yourself.
Signing in through the Estuary dashboard is not available yet.

To get the credentials:

1. In Seller Central, open the developer console and register an SP-API application with the
   roles listed under [Prerequisites](#prerequisites).
2. Copy the application's **LWA client ID** and **LWA client secret**.
3. Authorize the application for your seller account (self-authorization) and copy the
   **refresh token** Amazon shows.

See Amazon's [self-authorization guide](https://developer-docs.amazon.com/sp-api/docs/self-authorization).

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the Data Flow specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors.
The values and sample below provide configuration details for this connector.

### Properties

#### Endpoint

| Property                                           | Title                            | Description                                                                                                                                  | Type    | Required/Default |
| -------------------------------------------------- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ------- | ---------------- |
| **`/credentials`**                                 | Authentication                   | Login with Amazon credentials.                                                                                                               | object  | Required         |
| `/credentials/credentials_title` | Authentication Type | Must be `Login with Amazon`. | string | `Login with Amazon` |
| **`/credentials/client_id`**                       | LWA Client ID                    | The SP-API application's Login with Amazon client ID.                                                                                        | string  | Required         |
| **`/credentials/client_secret`**                   | LWA Client Secret                | The SP-API application's Login with Amazon client secret.                                                                                    | string  | Required         |
| **`/credentials/refresh_token`**                   | Refresh Token                    | The refresh token issued when the selling account authorized the application.                                                               | string  | Required         |
| **`/region`**                                      | Region                           | The SP-API region of the selling account: `North America`, `Europe` or `Far East`.                                                          | string  | Required         |
| **`/marketplace_ids`**                             | Marketplace IDs                  | 1 to 50 unique marketplace IDs, such as `ATVPDKIKX0DER` for Amazon.com. All must belong to the selected region. `all_orders` allows at most 25.                           | array   | Required         |
| `/start_date`                                      | Start Date                       | UTC date and time for historical capture. If omitted, UTC midnight 30 days before each stream first starts backfilling. Catalog refreshes do not use it.          | string  | 30 days ago      |
| `/advanced/window_size`                            | Window Size                      | Date window size for orders and financial transactions. ISO 8601 duration from `PT1H` to `P30D`, inclusive.                                        | string  | P1D              |
| `/advanced/overlap`                                | Incremental Overlap              | How far incremental order requests reach back to catch late search results. ISO 8601 duration from zero (`PT0S`) to `PT1H`, inclusive.                   | string  | PT10M            |
| `/advanced/financial_transactions_lookback`        | Financial Transactions Lookback  | Delay before the final financial transaction read, to catch released deferred transactions. ISO 8601 duration from `P4D` to `P180D`, inclusive.           | string  | P90D             |
| `/advanced/sandbox`                                | Use Sandbox                      | Send requests to Amazon's SP-API sandbox, which returns fixed example responses. For testing only.                                           | boolean | `false`          |

The backfill start is saved in each stream's state before work begins.
Restarting the capture resumes that saved backfill position.
The allowed duration ranges include both endpoints.

#### Bindings

| Property    | Title    | Description                                                                                                                         | Type   | Required/Default |
| ----------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------------- |
| **`/name`** | Name     | Name of the resource to capture.                                                                                                    | string | Required         |
| `/interval` | Interval | Interval between syncs. Defaults: `PT15M` for orders and financial transactions. `PT1H` for settlement reports. `P1D` for all-orders, Sales and Traffic, and catalog items. | string | Varies           |

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
      - resource:
          name: catalog_items
          interval: P1D
        target: ${PREFIX}/catalog_items
```
