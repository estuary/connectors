---
description: Capture Sage Intacct finance data with Estuary's connector and a Web Services developer license. Supported data resources include customers, vendors, projects, tax details, items, and General Ledger journal entries.
---

# Sage Intacct

This connector captures data from Sage Intacct into Estuary collections.

## Supported objects

The following objects support an incremental strategy for capturing new and
updated documents:
* CUSTOMER
* APTERM
* CLASS
* DEPARTMENT
* EMPLOYEE
* GLACCOUNT
* LOCATION
* TAXDETAIL
* VENDOR
* TRXCURRENCIES
* GLJOURNAL
* PROJECT
* ITEM
* TASK

:::info
The incremental strategy for the above objects captures deletions by querying the AUDITHISTORY object.
If the credentials provided to the connector do not have permission to query the AUDITHISTORY object,
deletions will not be captured.

Collections are keyed on `RECORDNO`, and a deletion can only be captured when Sage's audit entry
identifies the deleted record by its `RECORDNO`. For some objects Sage instead records the object's
user-facing ID (for example `VENDORID` for VENDOR or `CUSTOMERID` for CUSTOMER). Those deletions
are skipped and the deleted record remains in the collection.
:::

These objects support capturing via periodic snapshotting:
* COMPANYPREF

### General Ledger objects

These General Ledger objects use the same incremental strategy:
* GLBATCH (journal entries)
* GLENTRY (journal entry lines)

General Ledger bindings include the records made inside an entity of a
multi-entity company. Their backfills read all entries.

:::info
Sage does not record every General Ledger deletion in AUDITHISTORY. A deleted
or replaced row can remain in the collection until the binding is backfilled.
A backfill removes the rows that no longer exist in Sage from your
destination. For captures created before 2026-12-01, contact
Estuary support to enable this.

Backfills can be scheduled with the `schedule` resource config setting. No
schedule is set by default, since each backfill re-reads the whole object.
:::

## Prerequisites

To use this connector, you'll need:
* A [Sage Intacct Account](https://www.sage.com/en-us/sage-business-cloud/intacct/)
* An active Web Services developer license with sender ID and password provisioned

The Web Services subscription must be enabled, and the sender ID must be
authorized for your company to make API calls. Reference the [Sage Intacct Web
Services Doc](https://developer.intacct.com/web-services/) for more information.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the
catalog specification files. The values and specification sample below provide
configuration details specific to the Sage Intacct source connector.

### Properties

#### Endpoint

| Property                                    | Title                           | Description                                                                                                                                                                                                               | Type    | Required/Default |
|---------------------------------------------|---------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|------------------|
| **`/sender_id`**                            | Sender ID                       | Web Services Sender ID                                                                                                                                                                                                    | string  | Required         |
| **`/sender_password`**                      | Sender Password                 | Web Services Sender Password                                                                                                                                                                                              | string  | Required         |
| **`/company_id`**                           | Company ID                      | Sage Intacct Company ID                                                                                                                                                                                                   | string  | Required         |
| **`/user_id`**                              | User ID                         | Sage Intacct User ID                                                                                                                                                                                                      | string  | Required         |
| **`/password`**                             | Password                        | Sage Intacct Password                                                                                                                                                                                                     | string  | Required         |
| `/advanced`                                 | Advanced Options                | Options for advanced users. You should not typically need to modify these.                                                                                                                                                | object  |                  |
| `/advanced/include_company_id_in_documents` | Include Company ID in Documents | Include the configured Sage Intacct Company ID in captured documents, with the field name 'COMPANY_ID'. Every captured document will have the same value for this field, equal to the configured Sage Intacct Company ID. | boolean |                  |

#### Bindings

| Property    | Title    | Description                 | Type   | Required/Default |
|-------------|----------|-----------------------------|--------|------------------|
| **`/name`** | Name     | Name of this resource       | string | Required         |
| `/interval` | Interval | Interval between data syncs | string | PT6H             |
| `/schedule` | Backfill schedule | The schedule for automatically backfilling this binding. Accepts a cron expression. If left empty, the binding will not automatically backfill. | string |                  |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-sage-intacct:v1
        config:
          sender_id: my_sender_id
          sender_password: secret_sender_password
          company_id: my_company
          user_id: my_user_id
          password: secret_user_password
    bindings:
      - resource:
          name: CUSTOMER
          interval: PT6H
        target: ${PREFIX}/CUSTOMER
      {...}
```
