---
description: The Qdrant connector materializes Estuary data collections into Qdrant collections with OpenAI vector embeddings.
---

# Qdrant

This connector materializes documents from Estuary collections into existing [Qdrant](https://qdrant.tech/) collections.

The connector uses the [OpenAI Embeddings API](https://platform.openai.com/docs/guides/embeddings) to create one vector embedding for each document.
Then the connector upserts the vector and the document into Qdrant.

## Prerequisites

You need:

* The host name and gRPC port of the Qdrant server.
* If the Qdrant deployment requires authentication, you need a Qdrant API key.
* An OpenAI API key.

The default vector size is `1536`. This size is correct for the default `text-embedding-3-small` model.
The default distance metric is `Cosine`.

## Collection Setup

If a Qdrant collection does not exist, the connector creates it during `Apply`.
The new collection contains one unnamed dense vector with the configured vector size and distance metric.

## Embedding Input

For each document, the connector sends the selected materialization fields to OpenAI as one text input.
The text includes each field name in a consistent order.

By default, the connector selects scalar fields.

Use projected fields to include arrays or objects.

## Qdrant Payload

Each Qdrant point contains the complete Estuary document in the `flow_document` payload field.
The connector stores the document as a JSON string.
You can use this field to retrieve the document from a Qdrant search result.

The connector creates a deterministic UUIDv5 point ID from the packed collection key.
Each update for the same collection key uses the same point ID.
Thus, the update replaces the existing Qdrant point.

### Properties

#### Endpoint

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/qdrantHost`** | Qdrant Host | This value is the host name of the Qdrant server. | string | Required |
| `/qdrantPort` | Qdrant gRPC Port | This value is the gRPC port of the Qdrant server. | integer | `6334` |
| `/qdrantUseTls` | Use TLS | This value controls TLS for the Qdrant gRPC connection. | boolean | `false` |
| `/qdrantApiKey` | Qdrant API Key | Qdrant uses this API key for authentication. | string | Optional |
| **`/openAiApiKey`** | OpenAI API Key | OpenAI uses this API key for authentication. | string | Required |
| `/vectorSize` | Vector Size | This value is the number of dimensions in each vector. | integer | `1536` |
| `/distanceMetric` | Distance Metric | Qdrant uses this metric to compare vectors. | string | Default: `Cosine`. Options: `Cosine`, `Euclid`, `Dot`, or `Manhattan` |
| `/embeddingModel` | Embedding Model ID | This value is the model ID for OpenAI embeddings. | string | `"text-embedding-3-small"` |
| `/advanced/openAiOrg` | OpenAI Organization | This value is the organization name for OpenAI requests. | string | Optional |

#### Bindings

| Property | Title | Description | Type | Required/Default |
| --- | --- | --- | --- | --- |
| **`/collection`** | Qdrant Collection | This Qdrant collection receives the vectors. If the collection does not exist, the connector creates it. | string | Required |

### Sample

```yaml
materializations:
  ${PREFIX}/${mat_name}:
    endpoint:
      connector:
        image: "ghcr.io/estuary/materialize-qdrant:v1"
        config:
          qdrantHost: your-cluster.cloud.qdrant.io
          qdrantPort: 6334
          qdrantUseTls: true
          qdrantApiKey: <YOUR_QDRANT_API_KEY>
          openAiApiKey: <YOUR_OPENAI_API_KEY>
    bindings:
      - resource:
          collection: your-collection
        source: ${PREFIX}/${COLLECTION_NAME}
```

## Delta Updates

This connector supports only [delta updates](/concepts/materialization/#delta-updates).
An upsert for a point ID is idempotent.
If the connector retries an upsert, Qdrant replaces the point that has the same collection key.
