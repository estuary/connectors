---
description: Deliver Estuary collection data into Slack channels with this connector, and customize sender settings like display name and icon logo.
---

# Slack

This connector lets you materialize data from Estuary directly into Slack channels.

### Prerequisites

To use this connector, ensure you have the following:

1. An active Slack workspace with appropriate permissions.
2. Slack credentials and access token for authentication.
3. At least one Estuary collection.

### Configuration

The Slack connector is available for use in the Estuary web application. To learn more about connectors and how to set them up, read our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/access_token` | Access Token | The Slack API access token for authentication. | string | Required |
| `/client_id` | Client ID | Client ID for authentication. | string | Required |
| `/client_secret` | Client Secret | The Slack API client secret. | string | Required |
| `/advanced/raw_messages` | Raw Messages | Send the `text` and `blocks` of each document to Slack as written instead of escaping them. See [Message format](#message-format). | boolean | `false` |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| `/source` | Source | Source data in Estuary to be sent to Slack. | string | Required |
| `/channel` | Channel | The ID of the Slack channel to send messages to. | string | Required |
| `/display_name` | Display Name | The display name for the sender in Slack. | string | |
| `/logo_emoji` | Logo Emoji | The emoji to be used. | string |  |

### Message format

Each document is sent as one Slack message, built from these fields of the source collection:

| Field | Description | Required |
| --- | --- | --- |
| `ts` | Message timestamp in RFC 3339 format. Documents with a timestamp more than 10 minutes in the past are skipped. | Required |
| `text` | The message text. | Required |
| `blocks` | An array of [Block Kit](https://docs.slack.dev/block-kit/) blocks for a rich message. | Optional |

By default, the connector escapes `&`, `<`, and `>` in `text` and in every string inside `blocks`, so Slack displays them as written. Formatting such as `*bold*`, `_italic_`, and emoji codes still applies, but `<...>` sequences such as `<!channel>`, `<@U123>`, and `<url|label>` are not turned into mentions or links.

To send mentions and labeled links, enable **Raw Messages** under Advanced configuration. The connector then sends `text` and `blocks` unchanged, and Slack interprets every `<...>` sequence in them. The setting applies to all bindings of the materialization, so only enable it when everything written to the source collections is trusted. Escape `&`, `<`, and `>` yourself in any text that should appear literally, as described in Slack's [formatting guide](https://docs.slack.dev/messaging/formatting-message-text/#escaping).

### Sample

```yaml
materializations:
  ${PREFIX}/${MATERIALIZATION_NAME}:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-slack:v1
        config:
          credentials:
            auth_type: OAuth
            access_token: {secret}
            client_id: {your_client_id}
            client_secret: {secret}
    bindings:
      - source: ${PREFIX}/source_name
        resource:
          channel: "id: C05A95LJHSL"
          sender_config:
            display_name: Task Monitor
            logo_emoji: ":eyes:"
```
