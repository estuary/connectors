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
| Shard | Estuary | One running copy of the task. A shard owns a range of the key-hash space, and it subdivides that range across its channels. |
| Transaction | Estuary | The unit of work that ends with a checkpoint. |

### How a row reaches the table

Snowflake doesn't have a Snowpipe Streaming v2 SDK for Golang, so we use their
Python SDK and communicate with a Python sidecar process via unix socket. The
messages are NDJSON.

The sidecar opens four Snowpipe channels for each Estuary binding+shard, one per
equal key range of the shard's key-hash range. The connector routes each document
to a channel by the same packed-key hash the runtime routes documents to shards
by, so a channel's contents depend only on the data, never on how many shards the
task runs. Snowflake meters throughput per channel, so four channels also give
each shard four times the single-channel ceiling.

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
reaches first. One append per channel is pending at a time. A slow channel
therefore applies back pressure to the buffer instead of opening more requests.
All channels together buffer 128 MiB at most.

Every append carries an offset token. Snowflake stores the token of the last
append that it committed, and the connector reads that token back when it starts.

```
   42@00000000-3fffffff
   │  │        └─ key-end of the channel's key range
   │  └─ key-begin of the channel's key range
   └─ this append ends at document 42 of the channel
```

At the end of a Flow transaction, the connector waits until Snowflake commits the
token of its last append. The Flow checkpoint then records the same document
count. The two numbers describe the same position, one on each side.

### How recovery avoids duplicate rows

A shard can fail at any moment. On initialization, the connector compares, for
each channel, the committed token in Snowflake to that channel's routed index in the
Flow checkpoint. The difference is the number of documents that Snowflake already
holds. The connector skips those documents as the runtime replays them.

```
   documents of the channel:   1   2   3   4   5   6   7   8
   the Flow checkpoint records:        ▲ 3
   Snowflake committed:                        ▲ 5
                              └───── skip ──────┘└─ append ─┘
                                already in       sent to
                                the table        Snowflake
```

### How a shard split or join continues

The runtime assigns a document to a shard by a hash of its key, and this path
routes the document to a channel by the same hash. A midpoint split therefore
hands each child shard whole channels — two of the parent's four — with their
committed offset tokens, and a join hands the parent all eight of its children's.
The inheriting shard continues each channel where its token left off, including
the rows of an interrupted transaction, so a topology change during a backfill
neither wedges the task nor duplicates rows.

An inherited layout still works, but at fewer or more than four channels. At the
first transaction boundary where every channel is committed, the shard converges:
it first records the four channels of its own range in the checkpoint, and only
after that record is durable does it drop the inherited channels and route to its
own. Either half of that convergence can be interrupted and repeats safely.

### What else changes on this path

- A backfill truncates the table like any other backfill. The v2 materialization
  runtime stops every shard of the specification being replaced before Apply
  runs, so nothing appends while the table is truncated, and the backfill's shards
  derive channel names under a fresh state key, so they share nothing with the
  channels the truncate left standing. Those channels are swept the first time a
  shard of the backfill opens the binding. `retain_existing_data_on_backfill`
  works on this path the same as any other.
- Two tasks may not stream into one table. Every channel name carries the task it
  was derived for, and a shard lists the channels on the table's pipe before it
  opens any of its own, so a second task is rejected naming the first, and creates
  nothing the first could see. Since a backfill truncates rather than drops, a
  deleted or renamed task's channels stay on the table and reject the same way;
  backfilling the binding with `always_drop_tables_on_backfill` set drops the
  table and every channel on it. A channel of no Estuary shape, from some other
  high-performance client, cannot be attributed and is left alone.
- A binding can leave this path only for the `snowpipe_streaming` path, by naming
  `snowpipe_streaming` explicitly in `feature_flags` and removing
  `snowpipe_streaming_v2`, while keeping the task on the v2 materialization
  runtime. The publication logs a warning per binding it moves this way. The
  task's first transaction on the new path drops the binding's v2 channels, and
  every document Snowflake had already committed beyond the
  checkpoint is materialized again — permanently, since this path serves
  delta-updates bindings — so the duplicate count is C − K summed over the
  binding's channels, where C is what each channel had committed and K is what
  the checkpoint recorded for it. A later return to `snowpipe_streaming_v2`
  starts the binding on fresh channels. Any other departure is rejected at
  publication, and again at startup, naming the binding. The remedy is a
  backfill, which starts the binding on the new path with an empty checkpoint.
- A binding can move onto this write path at any time. Work that the path it
  leaves staged and did not finish is drained by the machinery that staged it,
  while the binding's rows go to this path. Only when that drain is impossible,
  because the `snowpipe_streaming` path cannot reopen its channel on the table, is
  the task rejected at startup, naming the table and the count outstanding.
  Restore that path for one transaction, or backfill the binding.
- The sidecar is crash-only. The connector never restarts it. The runtime restarts
  the connector, and recovery replays through the offset token.
