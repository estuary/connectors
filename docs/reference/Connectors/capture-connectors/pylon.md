---
description: Capture Pylon support data with Estuary's connector, including issues, issue messages, accounts, contacts, users, teams, tags, and custom fields, using API key authentication.
---

# Pylon

This connector captures data from Pylon into Estuary collections.

[Pylon](https://www.usepylon.com/) is a B2B customer support platform that manages issues across Slack, email, chat, and other channels.

## Supported data resources

The following data resources are supported through the [Pylon API](https://docs.usepylon.com/pylon-docs/developer/api/api-reference):

* [issues](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/issues)
* [issue_messages](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/messages)
* [accounts](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/accounts)
* [contacts](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/contacts)
* [users](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/users)
* [teams](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/teams)
* [tags](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/tags)
* [custom_fields](https://docs.usepylon.com/pylon-docs/developer/api/api-reference/custom-fields)

By default, each resource is mapped to an Estuary collection through a separate binding.

Issues are captured incrementally, about five minutes after each update. Deleted issues are not captured. All other resources are refreshed in full on each sync. Issue messages include internal notes, carry an `issue_id`, and require at least one request per issue, so consider a longer interval for that binding in large organizations.

## Prerequisites

* A Pylon [API token](https://docs.usepylon.com/pylon-docs/developer/api/authentication). Pylon admins can create one under **Settings > Integrations & dev > API tokens**.
* The region your Pylon organization is hosted in: `US` or `EU`.

## Configuration

You configure connectors either in the Estuary web app, or by directly editing the catalog specification files.
See [connectors](../../../concepts/connectors.md#using-connectors) to learn more about using connectors. The values and specification sample below provide configuration details specific to the Pylon source connector.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/credentials/credentials_title`** | Authentication Method | Set to `API Key`. | string | Required |
| **`/credentials/access_token`** | API Key | Your Pylon API token. Admins can create one under Settings > Integrations & dev > API tokens. | string | Required |
| `/region` | Region | The region where your Pylon organization is hosted. EU organizations use the api.eu.usepylon.com API. | string | `US` |
| `/start_date` | Start Date | UTC date and time in the format YYYY-MM-DDTHH:MM:SSZ. Issues updated before this date will not be replicated; other streams are always captured in full. | string | 30 days before the present |

#### Bindings

| Property | Title | Description | Type | Required/Default |
|---|---|---|---|---|
| **`/name`** | Data resource | Resource in Pylon from which collections are captured. | string | Required |
| `/interval` | Interval | Interval between data syncs. | string | `PT5M` for issues, `P1D` for issue_messages, `PT1H` otherwise |

### Sample

```yaml
captures:
  ${PREFIX}/${CAPTURE_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/source-pylon:v1
        config:
          credentials:
            credentials_title: API Key
            access_token: <secret>
          region: US
          start_date: "2026-01-01T00:00:00Z"
    bindings:
      - resource:
          name: issues
        target: ${PREFIX}/issues
      - resource:
          name: issue_messages
        target: ${PREFIX}/issue_messages
      - resource:
          name: accounts
        target: ${PREFIX}/accounts
      - resource:
          name: contacts
        target: ${PREFIX}/contacts
      - resource:
          name: users
        target: ${PREFIX}/users
      - resource:
          name: teams
        target: ${PREFIX}/teams
      - resource:
          name: tags
        target: ${PREFIX}/tags
      - resource:
          name: custom_fields
        target: ${PREFIX}/custom_fields
```
