# Changelog

## 2026-11-10

### Added

- `orders` captures orders from the Orders API as Amazon updates them.
- `financial_transactions` captures transactions from the Finances API.
- `settlement_reports` captures each row of the settlement reports Amazon
  generates for the account.
- `all_orders` captures each row of Amazon's all-orders report with every
  column, including the buyer's shipping city, state, postal code and country
  and `cpf`.
- `sales_and_traffic_by_date` is discovered disabled and captures daily sales
  and traffic figures for each marketplace from Amazon's Sales and Traffic
  report.
