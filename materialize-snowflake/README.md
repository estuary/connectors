# Snowflake

This materialization writes Flow collections into Snowflake tables. It has three
write paths. The connector picks one path for each binding when the task starts.
The path controls how rows arrive in Snowflake, not what the rows contain.

| Write path | How to select it | Bindings it serves |
| --- | --- | --- |
| Staged files | the default | standard updates and delta updates |
| Snowpipe Streaming | `snowpipe_streaming` feature flag, on by default | delta updates only |
| Snowpipe Streaming v2 | `snowpipe_streaming_v2` feature flag, off by default | delta updates only |

Staged files write a temporary stage and then run a `COPY INTO` or a `MERGE`
statement. Both streaming paths send rows to Snowflake over HTTPS and skip the
warehouse. A binding that cannot use a streaming path falls back to staged files.

## Staged Files

TODO

## Snowpipe Streaming

This method builds batches with Parquet files in a very particular format.
It is brittle and is being deprecated.

## Snowpipe Streaming v2

This path sends each document to Snowflake as soon as the connector receives it.
The rows appear in the table within seconds. Snowflake ingests them with its own
serverless service, so this method doesn't need a Snowflake warehouse. A task that
commits small transactions often therefore costs less than it does on the
staged-file path, which wakes a warehouse for every transaction.

A warehouse does run for some DDL tasks.

To turn on Snowpipe Streaming v2 for a task:

1. Add the `snowpipe_streaming_v2` feature flag to the endpoint configuration.
2. Add the `enable-runtime-v2` shard flag to the task specification.

Snowpipe Streaming v2 requires key-pair authentication (Snowflake calls this JWT
authentication). It also requires Estuary Runtime v2, hence the
`enable-runtime-v2` shard flag. It can only be used with delta updates.

### Glossary

This write path joins two systems, and each one names the parts its own way. The
table below says which system a word comes from. The rest of this section uses
the bare words.

| Term | Whose | What it means |
| --- | --- | --- |
| Channel | Snowflake | A named connection to one table. Snowflake stores a committed offset token for each channel, and that token is what recovery reads. |
| Append | Snowflake | One call that sends a batch of rows to a channel. The v2 SDK names the method `append_rows`. |
| Offset token | Snowflake | An opaque string that the connector attaches to an append. Snowflake keeps the token of the last append it stored. |
| Commit | Snowflake | The moment the ingest service stores an offset token durably. No SQL transaction takes part. |
| Sidecar | Estuary | The Python process that holds the Snowflake SDK. The connector talks to it over a unix socket. |
| Binding | Estuary | One collection mapped to one table. |
| Shard | Estuary | One running copy of the task. A shard owns a range of the collection key, and that range appears in the offset token. |
| Transaction | Estuary | The unit of work that ends with a checkpoint. |

### How a row reaches the table

Snowflake doesn't have a Snowpipe Streaming v2 SDK for Golang, so we use their
Python SDK and communicate with a Python sidecar process via unix socket. The
messages are NDJSON.

The sidecar opens one Snowpipe channel for each Estuary binding+shard.

```
   Flow runtime
        │  Store(document)
        ▼
   ┌─────────────────────────────────────────┐
   │  materialize-snowflake (Go)             │
   │                                         │
   │    convert ──► batch buffer ──► append  │
   │                (10,000 rows or 8 MiB)   │
   └────────────────────┬────────────────────┘
                        │  one append at a time, per channel
                        ▼
   ┌─────────────────────────────────────────┐
   │  Python sidecar                         │
   │  (snowpipe-streaming SDK)               │
   └────────────────────┬────────────────────┘
                        │  HTTPS
                        ▼
                  Snowflake table
```

The connector converts each document to a JSON row and holds it in a buffer. It
appends the buffer to the channel at 10,000 rows or at 8 MiB, whichever limit it
reaches first. One append per channel is in flight at a time. A slow channel
therefore applies back pressure to the buffer instead of opening more requests.
All channels together buffer 128 MiB at most.

Every append carries an offset token. Snowflake stores the token of the last
append that it committed, and the connector reads that token back when it starts.

```
   42@00000000-7fffffff
   │  │        └─ key-end of the shard
   │  └─ key-begin of the shard
   └─ this append ends at document 42 of the channel
```

At the end of a Flow transaction, the connector waits until Snowflake commits the
token of its last append. The Flow checkpoint then records the same document
count. The two numbers describe the same position, one on each side.

### How recovery avoids duplicate rows

A shard can fail at any moment. On initialization, the connector compares the
committed token in Snowflake to the counter in the Flow checkpoint. The
difference is the number of documents that Snowflake already holds. The connector
skips those documents as the runtime replays them.

```
   documents of the channel:   1   2   3   4   5   6   7   8
   the Flow checkpoint records:        ▲ 3
   Snowflake committed:                        ▲ 5
                              └───── skip ──────┘└─ append ─┘
                                already in       sent to
                                the table        Snowflake
```

### What else changes on this path

- A backfill drops and re-creates the table, because a channel cannot outlive the
  table that it appends to. Grants on the old table do not survive.
- `retain_existing_data_on_backfill` does not work on this path, for the same
  reason.
- A binding cannot move off this write path. The publication succeeds, and the
  task then refuses to start, naming the binding. To take a binding off the path,
  backfill it, which starts it on the new path with an empty checkpoint.
- A binding can move onto this write path at any time, with one condition. If the
  checkpoint still holds work that the previous path staged and did not finish,
  the task refuses to start. Restore the previous path and let one transaction
  finish that work, then move the binding again. A backfill also clears it.
- The path adopts a table that another path created. Apply records this generation
  in the comment of a table that records none. The guard against a replaced
  generation then works for that table too.
- The sidecar is crash-only. The connector never restarts it. The runtime restarts
  the connector, and recovery replays through the offset token.
