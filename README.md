# Estuary Connectors

This repo hosts development for connectors for use with Flow.

The `source-*` connectors all implement the [Airbyte
spec](https://docs.airbyte.io/understanding-airbyte/airbyte-specification). Flow uses the Airbyte
protocol for capturing data from external systems. Source connectors in this repo can (at least
theoretically) be used with either Flow or Airbyte, or anything else that uses the Airbyte protocol.

The `materialize-*` connectors all implement the [Flow Materialize gRPC protocol](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto), and only work with Flow.

All connectors in this repository are dual licensed under MIT or Apache 2.0 at your discretion.

