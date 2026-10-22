---
description: Capture daily AWS Cost Explorer totals and breakdowns by service and linked account, refreshing available costs while preserving older captured history.
---

# AWS Cost Explorer

This connector captures daily monetary costs from the AWS Cost Explorer API into Estuary collections.

## Supported data resources

| Resource | Row | Replication Mode |
|---|---|---|
| [daily_costs](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html) | UTC usage day | Periodic keyed refresh |
| [daily_costs_by_service](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html) | UTC usage day and service | Periodic keyed refresh |
| [daily_costs_by_linked_account](https://docs.aws.amazon.com/aws-cost-management/latest/APIReference/API_GetCostAndUsage.html) | UTC usage day and linked account ID | Periodic keyed refresh |

Each resource is mapped to its own collection. Keys are `date`, `date` plus `service`, and `date` plus `linked_account_id`, respectively. Linked account names are included when AWS returns them.

All resources include `date`, exclusive `period_end`, `estimated`, and a `metrics` object with `UnblendedCost`, `BlendedCost`, `AmortizedCost`, `NetUnblendedCost`, and `NetAmortizedCost`. Each metric contains `amount` as an exact decimal string and `unit`. Usage quantities are excluded because combining services can combine incompatible units.

:::tip
Each sync reads complete days through yesterday UTC. The first sync, and any backfill, reads from the start date. Later syncs reread from the first day of the month before the previous sync, which picks up estimates and billing corrections until AWS finalizes each month. Without a start date, each sync rereads the last 30 days.

Older captured rows remain as last observed when they leave AWS's available history. Missing rows do not produce deletion events, including missing service or account groups within the available range. Explicit zero-cost values returned by AWS update existing rows. Costs outside AWS's retention can no longer be refreshed, and data that expired before the connector first captured it cannot be recovered through this API.
:::

AWS provides daily data for the current month and previous 13 months. Each poll clamps its query to that available range. See [Cost Explorer API best practices](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-api-best-practices.html).

Each resource syncs once every 24 hours by default, and no more often. AWS charges $0.01 per requested API page. Each resource requests at least one page per sync. The first sync and backfills read the whole start-date range, which takes more pages, especially for `daily_costs_by_service`. Retries and validation add requests. See [Cost Explorer pricing](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-what-is.html).

## Prerequisites

- Enable [Cost Explorer](https://docs.aws.amazon.com/cost-management/latest/userguide/ce-enable.html). AWS can take time to prepare the initial data.
- Provide AWS credentials with `ce:GetCostAndUsage`. The connector only reads cost data.
- To capture organization-wide costs, use credentials in the organization management account with the necessary billing visibility. Member accounts see the data AWS permits them to access.

A minimal IAM policy is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ce:GetCostAndUsage",
      "Resource": "*"
    }
  ]
}
```

The connector signs requests using the AWS SDK and a long-term access key. The Cost Explorer endpoint is `https://ce.us-east-1.amazonaws.com`, regardless of the regions where usage occurred.

## Configuration

Configure the connector in the Estuary web app or a catalog specification.

### Endpoint properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/credentials/credentials_title` | Authentication | Set to `AWS Access Key`. | string | `AWS Access Key` |
| **`/credentials/access_key_id`** | Access Key ID | AWS access key ID. | string | Required |
| **`/credentials/secret_access_key`** | Secret Access Key | AWS secret access key. | string | Required |
| `/start_date` | Start Date | Timestamp whose containing UTC date is the first usage day to capture. | string | The last 30 days |

### Binding properties

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | One of the supported resource names. | string | Required |
| `/interval` | Interval | Time between the starts of consecutive refreshes, as an ISO 8601 duration. At least `P1D`. | string | `P1D` |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-aws-cost-explorer:v1
        config:
          credentials:
            credentials_title: AWS Access Key
            access_key_id: <access-key-id>
            secret_access_key: <secret-access-key>
          start_date: 2026-01-01T00:00:00Z
    bindings:
      - resource:
          name: daily_costs
          interval: P1D
        target: ${PREFIX}/daily_costs
      - resource:
          name: daily_costs_by_service
          interval: P1D
        target: ${PREFIX}/daily_costs_by_service
      - resource:
          name: daily_costs_by_linked_account
          interval: P1D
        target: ${PREFIX}/daily_costs_by_linked_account
```
