---
description: Learn how to build your own connector for use with the Estuary platform. Integrate with any system, customize your connection, and plug-and-play with any other Estuary connector.
---

# Custom Connectors

You can build your own integrations to plug into your Estuary data flows. This
is useful if Estuary does not yet support your desired source system, or if
your integration is tied to a very specific use case and you want to apply
custom business logic to the connector as you ingest data.

:::tip
Custom connectors are currently supported for **captures**. Custom derivations
and materializations are coming soon.
:::

## Estuary's Connector Development Kit

Custom integrations are based on Estuary's CDK, or Connector Development Kit.
The CDK provides functionality common across connectors so new integrations
work seamlessly with Estuary's runtime.

Estuary's CDK currently supports [captures](./create-capture.md) with further
support planned, and is available in Python.

### Basic connector layout

With the CDK, each integration contains three connector-specific modules.

* `api`: interacts directly with the capture API to produce models
* `models`: defines expected models for API outputs
* `resources`: binds models and API functions together into resources that can
be captured

These modules work together to read data from the source API and translate it
into discrete resources that Estuary can capture.

This is the logic that custom connectors need to fill in.

### Common functionality

The CDK provides a `common` module to collect functionality used across
Estuary connectors. You can utilize the provided functionality to implement the
desired capture _strategy_ for each resource.

Capture strategies include:

* **Incremental:** Captures a logical log of changes
* **Backfill:** Captures paginated backfills of resources; can be run concurrently with incremental
* **Snapshot:** Captures periodic full snapshots
* **Webhook:** Captures push updates rather than pull updates

Each resource strategy is implemented slightly differently. You can use or view
the [`classify-stream-types` skill](https://github.com/estuary/connectors/blob/main/.claude/skills/classify-stream-types/SKILL.md)
for implementation examples.

## Custom Connector Specification

Tasks based on custom connectors must be defined in your [catalog](/concepts/catalogs)
like any other task. Make sure you are familiar with [capture](/concepts/captures)
specification files and managing your catalog via [flowctl](/concepts/flowctl).

See the guide on [creating a custom capture](./create-capture.md) for an
example specification.
