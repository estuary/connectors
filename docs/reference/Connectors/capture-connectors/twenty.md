---
description: Capture standard and custom objects from Twenty Cloud or self-hosted Twenty, including updates, soft deletes, and restored records, using API key authentication.
---

# Twenty

This connector captures data from Twenty into Estuary collections.

## Supported data resources

The connector discovers active objects from your workspace's metadata API. It captures standard and custom objects, plus the `noteTargets` and `taskTargets` join objects that identify which record a note or task is attached to. Remote objects and Twenty's other system objects are excluded. New custom objects appear when you re-discover the capture.

Renaming an object makes it appear as a new collection on re-discovery; the old collection stops updating. If an object is removed or deactivated while the capture runs, it stops being captured once the capture next restarts: reading a removed object fails and restarts the capture. After a restart the capture skips that binding with a warning and keeps capturing the other objects; if none of the bound objects remain, it fails with an error instead. Publishing the capture fails validation until you remove the binding.

All objects are read through [Twenty's REST API](https://docs.twenty.com/developers/extend/api).

| Resource | Replication Mode |
|----------|------------------|
| people | Incremental |
| companies | Incremental |
| opportunities | Incremental |
| notes | Incremental |
| tasks | Incremental |
| workflows | Incremental |
| dashboards | Incremental |
| noteTargets | Incremental |
| taskTargets | Incremental |
| Custom objects | Incremental |

By default, each resource is mapped to an Estuary collection through a separate binding.

Each object is backfilled in full while new changes are polled incrementally using `updatedAt`. The connector waits about one minute for recent writes to settle before including them in a poll.

Twenty's normal delete moves a record to the trash and sets `deletedAt`. The connector captures these soft deletes as deletion events (`_meta/op: d`), retaining the document's last values. Restoring a record advances `updatedAt`, so the restored record is captured as an update.

:::tip
Permanent deletes, such as emptying the trash or using an API delete without soft delete, cannot be detected by polling. Their side effects on other records can also be invisible: destroying a company can clear a person's `companyId` without changing that person's `updatedAt`, and can remove related `noteTargets`.

Set a binding's `schedule` to a cron expression to re-read that object in full periodically. For example, `0 0 * * *` starts a backfill every day at 00:00 UTC. This refreshes surviving records whose links were cleared. Records that were permanently deleted remain in the Estuary collection, including permanently removed join records.
:::

Documents omit `searchVector`, Twenty's internal search index. Related records are captured in their own bindings; documents retain relation IDs without nested relation objects.

## Prerequisites

To set up the Twenty source connector, you'll need:

- A Twenty workspace, either Twenty Cloud or self-hosted. The connector is tested with Twenty Cloud and self-hosted Twenty v2.52.
- An API key, created in Twenty's **Settings** under **API** / **APIs & Webhooks**. The key is shown only once, so copy it when you create it.
- A role for that key with the **Data Model** settings permission and read access to every object you want to capture. The connector needs Data Model to read the workspace's metadata. Reading workflows also needs the **Workflows** permission.
- For self-hosted Twenty, your server's base URL.

Objects the role can't read are discovered as disabled bindings. After you give the role access, enable the binding yourself; re-discovery won't enable it. If the role later loses access to an enabled object, the capture fails and restarts on that object. Publishing also fails with an error naming it. Restore access or disable its binding to fix both. Discovery reads one record of each object, so it takes longer in workspaces with many objects.

## Rate limits

Twenty Cloud's [documented API limit](https://docs.twenty.com/developers/extend/api) is 100 requests per minute. The connector slows down and retries when it receives HTTP 429. Each request reads up to 200 records.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification file.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Twenty source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| `/base_url` | Base URL | The address of your Twenty server's API. Leave the default for Twenty Cloud; for self-hosted Twenty, use your server's address, like https://twenty.example.com. | string | `https://api.twenty.com` |
| `/credentials/credentials_title` | Credentials Title | Set to `API Key`. | string | `API Key` |
| **`/credentials/access_token`** | API Key | A Twenty API key, created in Twenty under Settings > APIs & Webhooks. | string | Required |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Name | Name of this resource | string | Required |
| `/interval` | Interval | Interval between updates for this resource | string | `PT5M` on discovered bindings |
| `/schedule` | Schedule | Schedule to automatically rebackfill this binding. Accepts a cron expression. | string | Empty (no automatic backfill) |

Set `name` to the object's plural API name, such as `people` or `companies`. The `interval` uses an ISO 8601 duration, such as `PT5M` for five minutes. Discovery sets it to `PT5M`; the generic resource schema defaults to `PT0S` if you omit it when writing a binding by hand. `PT0S` polls continuously and can use up Twenty Cloud's rate limit, so set `PT5M` or longer.

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-twenty:v1
        config:
          base_url: https://api.twenty.com
          credentials:
            credentials_title: API Key
            access_token: <secret>
    bindings:
      - resource:
          name: people
          interval: PT5M
          schedule: "0 0 * * *"
        target: ${PREFIX}/people
      - resource:
          name: companies
          interval: PT5M
          schedule: "0 0 * * *"
        target: ${PREFIX}/companies
```
