---
description: Send Estuary collection data to HTTP endpoints with the webhook connector. Configure target URLs, custom headers, and relative paths.
---

# HTTP Webhook

This connector lets you materialize data from Estuary directly to specified HTTP endpoints via webhooks.

## Prerequisites
To use this materialization connector, you’ll need the following:

- A server or service that can accept HTTP requests at the target endpoint.
- At least one Estuary collection.

## Configuration
The Webhooks connector is available for use in the Estuary web application. To learn more about connectors and setting them up, visit our guide on [using connectors](https://docs.estuary.dev/concepts/connectors/#using-connectors).

## Properties

### Endpoint

| Property           | Title          | Description                              | Type   | Required/Default       |
|--------------------|----------------|------------------------------------------|--------|------------------------|
| **`/address`**     | Address        | The URL of the endpoint to send data to. | string | Required               |
| **`/headers/customHeaders`**     | Headers        | Array of additional headers to include in the HTTP request. | object |               |
| **`/advanced/feature_flags`** | Feature Flags | For Estuary internal use. Only modify as directed by Estuary support. | string |               |

### Bindings

| Property           | Title          | Description                                    | Type   | Required/Default       |
|--------------------|----------------|------------------------------------------------|--------|------------------------|
| **`/relativePath`**| Relative Path  | The relative path on the server where data will be sent. | string | Required               |

## Payload format

Each request is a `POST` whose body is a JSON array of documents from one binding.

Each element is a JSON object of the binding's [selected fields](/guides/customize-materialization-fields/), keyed by field name.
All recommended fields are selected by default, including metadata such as `flow_published_at` that isn't part of the document, and you can exclude fields one by one:

```json
[{"id": 1, "status": "shipped", "flow_published_at": "2026-10-23T14:00:00Z"}]
```

Nested locations are selected as separate fields named by their path, such as `customer/name`.
To send nested objects whole instead, select fields to a depth of 1 with `fields.recommended: 1`.
Locations the collection schema doesn't describe aren't fields, so they're only sent as part of the full document, `flow_document`.
It isn't selected by default; include it to add the full document to each element.

## Sample

```yaml
bindings:
- source: ProductionData/orders/orderDetails
  resource:
    relativePath: webhook/estuary
endpoint:
  connector:
    image: ghcr.io/estuary/materialize-webhook:v1
    config:
      address: http://192.168.1.100:3000/
      headers:
        customHeaders:
          - name:  my-header
            value: my-value
          - name:  another-header
            value: another-value
```
