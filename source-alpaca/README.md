# Flow Alpaca Source Connector

This is a Flow capture connector which captures trade data from the [Alpaca Market Data
API](https://alpaca.markets/).

**Notes for Use Alpaca Free Accounts:** It is possible to use the connector with credentials from an
Alpaca free account. When configuring the connector to use with an Alpaca free account, make sure
the connector "Advanced" configuration for `Free Plan` is enabled. This will ensure data is not
requested within the limits of a free account, which prohibit queries for data within the last 15
minutes. Also note that only the `iex` exchange will work properly with a free plan. 

See the [Alpaca Getting Started
Docs](https://alpaca.markets/docs/trading/getting-started/#creating-an-alpaca-account-and-finding-your-api-keys)
for more information on setting up an account and finding your credentials to use when configuring
the connector.

## Current Limitations

- This initial version of the connector has been tested with 10-20 symbols in a single capture.
  Using a significantly larger number of symbols may result in API errors. Future support to handle
  larger numbers of symbols is planned.
- The collection written to by this capture will contain duplicate trade documents. This is because
  the connector combines data collection from the real-time streaming API as well as the historical
  data API to provide the lowest-latency version of available trades, as well as ensuring a 100%
  comprehensive set of data is always collected if the websocket connection is interrupted.
  Materializations from the resulting collection configured to use [Delta
  Updates](https://docs.estuary.dev/concepts/materialization/#delta-updates) should take care to
  ensure that the destination supports "last write wins" reductions. 

## Real-Time Streaming

The connector captures data in real-time using the [Alpaca websocket API for market
data](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/realtime/). This
stream is a "best effort" source of data: It can only be initiated from the present moment in time,
and if the stream is interrupted there is no way to resume streaming from a previous time. While
this mechanism of data collection provides the lowest possible latency, it can result in gaps where
data is missed, and is not able to provide any historical backfill capabilities.

## Comprehensive Backfill

Paired with the real-time streaming data source, a comprehensive backfill is concurrently executed
using the [Alpaca historical market data
API](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/).
This backfill works forward in time, starting from the configured "start date" and continuing to the
present time. This will ensure that all trade records are captured.

Upon initial startup of the connector, the backfill is run until it has caught up with the present
before the streaming client is started. At that point, the backfill will continue to periodically
run to remain caught up.

## Configuration

### Basic Configuration

A typical configuration will include authentication credentials, the feed to pull market data from,
symbols to monitor, and a date to start the data collection.

### Advanced Configuration

For additional control, the following advanced configuration options are available. These should not
normally be needed, but may be useful in some circumstances:
- `Free Plan`: Set this if you are connecting with credentials for an Alpaca free account.
- `Stop Date`: This will stop the backfiller at a certain date. The backfill will not proceed beyond
  this time. Note that real-time streaming _will_ capture trade records beyond this date, unless it
  is disabled completely.
- `Disable Real-Time Streaming`: Set this to only run the historical backfill for data collection.
  This could be useful if you want to ensure the collection has 100% of trades documents with no
  duplicates and are not concerned about the lowest possible latency for recent trades.
- `Disable Historical Backfill`: Set this to only run the real-time streaming collector. This could
  be useful if you are not interested in historical data or a complete history and only want the
  most recent trades.
- `Maximum Backfill Interval` & `Minimum Backfill Interval`: These configurations control how large
  of a time period the backfill will retrieve for each "chunk" of time, and what the minimum amount
  of time must be for each chunk. These may be useful when backfilling very large sets of data under
  limited network bandwidth.