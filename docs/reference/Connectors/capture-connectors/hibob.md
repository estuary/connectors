---
description: Use the HiBob connector to sync employees, employee fields, named lists, and employment, lifecycle, and work history, using service user authentication.
---

# HiBob

This connector captures data from [HiBob](https://www.hibob.com/) into Estuary collections.

## Supported data resources

The following data resources are supported:

| Resource | Replication Mode |
|----------|------------------|
| [employees](https://apidocs.hibob.com/reference/post_people-search) | Full Refresh |
| [employee_fields](https://apidocs.hibob.com/reference/get_company-people-fields) | Full Refresh |
| [named_lists](https://apidocs.hibob.com/reference/get_company-named-lists) | Full Refresh |
| [employment_history](https://apidocs.hibob.com/reference/get_bulk-people-employment) | Full Refresh |
| [lifecycle_history](https://apidocs.hibob.com/reference/get_bulk-people-lifecycle) | Full Refresh |
| [work_history](https://apidocs.hibob.com/reference/get_bulk-people-work) | Full Refresh |

By default, each resource is mapped to an Estuary collection through a separate binding.

- `employees` includes every field in the `employee_fields` metadata that the service user can view, including custom fields. Fields whose values change without any HR change are excluded: avatar and cover image URLs, tenure and age counters, and People Analytics risk scores. Terminated employees are included. Department, site, and other list-type fields hold named list item IDs, which can be joined to `named_lists`.
- `employment_history` holds FTE, weekly hours, and contract history; `lifecycle_history` holds start, leave, and termination events; `work_history` holds department, title, site, and manager history.

:::tip
HiBob's API has no way to request only changed records, so every resource is re-captured in full on each interval (one hour by default). Documents deleted in HiBob are removed from the collections. A poll with no changes captures nothing, but any change to a resource re-captures every document in it. Expect one full re-capture of each resource on the first poll after the initial backfill.

History entry `id`s are only unique per employee. Use `employeeId` and `id` together to identify a history entry.
:::

## Prerequisites

To set up the HiBob source connector, you'll need a HiBob [service user](https://apidocs.hibob.com/docs/api-service-users) ID and token. A HiBob admin can create one under **Settings > Integrations > Automation > Service Users**.

Service users start with no permissions. Add the service user to a permission group that grants:

- **People's data > People's fields**: **View** on the categories to capture, and **View history** on the Employment, Lifecycle, and Work categories. Sensitive fields require both **View** and **Edit** to return values.
- **People's data > Access data for**: select people by condition and remove the default **Lifecycle status equals Employed** condition. Otherwise, terminated employees are silently omitted.

HiBob omits fields the service user is not permitted to view without returning an error, so missing fields usually indicate a missing permission.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the HiBob source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Name of the credentials set. Set to `Service User`. | string | Required |
| **`/credentials/username`** | Service User ID | The ID of the HiBob service user. | string | Required |
| **`/credentials/password`** | Service User Token | The token generated for the HiBob service user. | string | Required |
| `/advanced/sandbox` | Sandbox | Capture from the HiBob sandbox environment (`api.sandbox.hibob.com`) instead of production. | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Name of the data resource. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | PT1H |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-hibob:v1
        config:
          credentials:
            credentials_title: Service User
            username: <service user ID>
            password: <secret>
    bindings:
      - resource:
          name: employees
        target: ${PREFIX}/employees
      - resource:
          name: employment_history
        target: ${PREFIX}/employment_history
      {...}
```
