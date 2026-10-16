---
description: Use the Okta connector to sync users, groups, applications, application assignments, and System Log events, using API token authentication.
---

# Okta

This connector captures data from the Okta Management API into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [users](https://developer.okta.com/docs/api/openapi/okta-management/management/tag/User/#tag/User/operation/listUsers) | Incremental |
| [groups](https://developer.okta.com/docs/api/openapi/okta-management/management/tag/Group/#tag/Group/operation/listGroups) | Incremental |
| [applications](https://developer.okta.com/docs/api/openapi/okta-management/management/tag/Application/#tag/Application/operation/listApplications) | Full Refresh |
| [application_users](https://developer.okta.com/docs/api/openapi/okta-management/management/tag/ApplicationUsers/#tag/ApplicationUsers/operation/listApplicationUsers) | Full Refresh |
| [system_log](https://developer.okta.com/docs/api/openapi/okta-management/management/tag/SystemLog/#tag/SystemLog/operation/listLogEvents) | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

:::tip
Okta only reports user and group deletions in its System Log. If the API token can't read the System Log, the `users` and `groups` streams still capture new and updated records, but not deletions, and the `system_log` binding is disabled when the connector discovers its bindings. Deletions that happen before System Log access is granted are not recovered later.

Okta retains System Log events for 90 days, so the `system_log` stream can't backfill events older than that, regardless of the configured start date. Because `users` and `groups` read deletions from the same log, a capture paused for more than 90 days can't recover the deletions that happened in the gap.

`users` and `groups` deliberately trail the present by 10 minutes, giving Okta time to make recent changes visible to its search indexes. `system_log` events arrive as soon as Okta releases them to its polling API, typically about a minute after they occur.
:::

## Prerequisites

To set up the Okta source connector, you'll need:

- Your Okta org domain, such as `dev-123456.okta.com`, `acme.okta-emea.com`, or a custom domain like `login.acme.com`.
- An Okta [API token](https://developer.okta.com/docs/guides/create-an-api-token/main/), created in the Admin Console under **Security > API > Tokens**. A token inherits the admin role of the account that created it; that role must be able to list users and, to capture deletions and the `system_log` stream, view the System Log. Okta revokes tokens that go 30 days without use.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Okta source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/domain`** | Okta Domain | The Okta org domain the Management API is served from, without a scheme. Examples: `dev-123456.okta.com`, `acme.okta-emea.com`, `acme.oktapreview.com`, or a custom domain such as `login.acme.com`. | string | Required |
| **`/credentials/credentials_title`** | Credentials Title | Name of the credentials set. Set to `API Token`. | string | Required |
| **`/credentials/access_token`** | API Token | The Okta API token. | string | Required |
| `/start_date` | Start Date | UTC date and time in the format `YYYY-MM-DDTHH:MM:SSZ`. `users` and `groups` are backfilled from the objects last updated after this instant, and `system_log` from the events published at or after it; `applications` and `application_users` are always captured in full. If left blank, the start date will be set to 30 days before the present. | string | |
| `/advanced/window_size` | Window Size | Date window size for the `users` and `groups` backfill in ISO 8601 format. ex: P30D means 30 days, PT6H means 6 hours. | string | P30D |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-okta:v1
        config:
          domain: dev-123456.okta.com
          credentials:
            credentials_title: API Token
            access_token: <secret>
          start_date: 2026-01-01T00:00:00Z
    bindings:
      - resource:
          name: users
          interval: PT5M
        target: ${PREFIX}/users
      - resource:
          name: application_users
          interval: PT1H
        target: ${PREFIX}/application_users
      {...}
```

## Application Assignments

The `application_users` stream re-lists every application's user assignments each interval, one hour by default. It includes assignments to inactive applications. Each document records the application it belongs to in `_meta/app_id`. When the assignment was granted through a group, `_meta/group_id` records that group; for direct assignments, it's null. A user assigned both directly and through a group appears once, as a direct assignment.
