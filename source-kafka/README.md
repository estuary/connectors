# `source-kafka` Connector

This is an [Airbyte Specification-compatible](https://docs.airbyte.io/understanding-airbyte/airbyte-specification) connector that reads from Kafka topics/partitions.

## Getting Started

To get started using this connector with Estuary Flow, run `flowctl-go discover --image ghcr.io/estuary/source-kafka:TAG`.

The remainder of this README is geared toward developers who wish to contribute or integrate the connector in other projects.

## Connector Development

The easiest way to start developing on the connector is from Docker. The latest images are published at [Estuary's Package Registry](https://github.com/orgs/estuary/packages)

The `spec` command is a great way to verify it's working.

```bash
docker run --rm -it ghcr.io/estuary/source-kafka:TAG spec
```

The `check` command can verify your connection configuration.

```bash
docker run -it --rm -v $(pwd)/YOUR_CONFIG.json:/config.json ghcr.io/estuary/source-kafka:TAG check --config=/config.json
```

The `discover` command can list the topics and partitions available in the connected Kafka cluster.

```bash
docker run -it --rm -v $(pwd)/YOUR_CONFIG.json:/config.json ghcr.io/estuary/source-kafka:TAG discover --config=/config.json
```

The `read` command will begin consuming messages from Kafka.

```bash
docker run -it --rm -v $(pwd)/YOUR_CONFIG.json:/config.json -v $(pwd)/YOUR_CATALOG.json:/catalog.json ghcr.io/estuary/source-kafka:TAG read --config=/config.json --catalog=/catalog.json
```

## Configuration

To connect to the cluster, you'll need to craft a `config.json` file that matches the layout of the `spec` command.

The Kafka cluster will need to be configured to allow client to connect. See the [`librdkafka` documentation on `bootstrap_servers`](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md#global-configuration-properties) for more information.

Example:

```json
{
  "bootstrap_servers": ["localhost:9092"]
}
```

## Catalog

To read data from a topic, you'll need to craft a `catalog.json` file that matches the layout of the `discover` command.

Example:

```json
{
  "streams": [
    {
      "name": "topic-name",
      "json_schema": {
        "type": "object"
      }
    }
  ],
  "estuary.dev/tail": true,
  "estuary.dev/range": {
    "begin": "00000000",
    "end": "ffffffff"
  }
}
```

#### Catalog Extensions

It supports two [estuary.dev](estuary.dev) extensions to the spec:
1. `estuary.dev/tail` to enable a never-ending connector that constantly receives data from the data source.
2. `estuary.dev/range` to enable reading from a subset of the partitions of a Kafka topic.


## Developer Setup

#### Local Kafka

You'll need to make sure you have a local Kafka cluster running. This is most
easily done by using the `infra` tools from the top level of the
`estuary/connectors` repo.

```bash
make start_local_infra
```

This will launch a dockerized Kafka + Zookeeper instance and expose a client listener on port 9092.


#### Install `kafkactl`

The easiest way to interact with Kafka in an adhoc fashion is to grab the latest
release of [`kafkactl`](https://github.com/deviceinsight/kafkactl). This
provides a simple CLI for adding/removing topics and producing/consuming
messages.

#### Build

Cargo handles building the connector.

```bash
cd path/to/estuary/connectors/source-kafka
cargo build
```

## Tests

First, make sure you've followed the [Developer Setup](#developer-setup) instructions above.

#### Run the End to End Tests

To verify everything is working correctly with [Flow](github.com/estuary/flow), you can run the connector repo tests.

```bash
cd path/to/estuary/connectors
CONNECTOR=source-kafka VERSION=<<DOCKER_IMAGE_TAG>> ./tests/run.sh
```

#### Run the Connector Tests

To run the unit tests:

```bash
cd path/to/estuary/connectors/source-kafka
cargo test --lib
```

To run the integration tests which connect to Kafka:

```bash
cd path/to/estuary/connectors/source-kafka
make test_setup
cargo test
```
