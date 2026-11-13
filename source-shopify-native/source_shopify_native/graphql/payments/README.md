# Shopify Payments streams

The `payouts` and `balance_transactions` streams capture [Shopify Payments](https://shopify.dev/docs/api/admin-graphql/2026-01/objects/ShopifyPaymentsAccount) data. Shopify's API has several constraints that shape how they work, so this file collects them in one place.

## Constraints

- Payouts and balance transactions have no top-level query. They're lists on the `ShopifyPaymentsAccount` object, so every query goes through `shopifyPaymentsAccount { ... }`. Their resources set `QUERY_ROOT = "shopifyPaymentsAccount"` and a `CONNECTION_PATH` down to the list.
- Reading the account needs `read_shopify_payments_accounts` or `read_shopify_payments`, on top of `read_shopify_payments_payouts`. That's why their `QUALIFYING_SCOPES` has two sets.
- Neither payouts nor balance transactions have an updated-at field to read changes by.
- A payout's status can change after it's issued. Shopify staff say a paid payout can come back failed up to about 10 business days later.
- A balance transaction's `associatedPayout` id is null until the transaction is put in a payout. That happens when the payout is created, which depends on the store's payout schedule. For manual payouts, it's whenever the merchant asks for one.
- A payout can't list its own transactions. The only way to get them is the `payments_transfer_id` filter on the account's `balanceTransactions` list. Shopify lists that filter but doesn't say what it matches, so we assume it matches the payout's `legacyResourceId`.
- Development stores can't generate payouts, so none of this can be tested against real data.

## How the streams work

- `balance_transactions` reads new transactions by `processed_at` every 5 minutes and backfills the same way. Most transactions are read before they're in a payout, so they're captured with a null payout id.
- `payouts` backfills by `issued_at`. Once a day, it re-reads every payout issued since 30 days before its last read, which catches status changes.
- In the same daily read, `payouts` reads the transactions of each payout issued since 7 days before its last read. It writes them to the `balance_transactions` binding as `AssociatedDocument`s, which fills in their payout ids.
- Both transaction queries come from one builder in `BalanceTransactions`, so they select the same fields. If they didn't, each read would overwrite fields the other captured.
- `validate` rejects `balance_transactions` without `payouts`, because nothing else fills in the payout ids.
- The daily read fails if a payout's first page of transactions holds none of its own, which would mean `payments_transfer_id` doesn't match the way we assume. That fails the whole capture. It's deliberate, so a wrong assumption gets noticed and fixed.

## Alternatives we ruled out

- Re-reading every balance transaction each day. A store with 10,000 orders a day adds roughly 2 GB of transactions a year, and a daily re-read would read all of it every day.
- Re-reading the last N days of transactions by `processed_at`. No N covers manual payouts, which can come weeks after a transaction is processed.
- A second subtask per store for the daily re-read. It doubles the subtasks every capture runs, and the `payouts` stream already lists the payouts.

## Known gaps

- A Shopify Plus shop can have several business entities, each with its own payments account. The streams read the shop-level account. Shopify doesn't say which account that is in a shop with several entities, and it's probably the primary entity's. Reading every entity's account would need a subtask per entity.
- Two reads of the same transaction can overlap at the moment its payout is created. If the older read's null payout id is written last, the transaction keeps it until the next daily `payouts` read writes it again with the id.
