# The Snowpipe Streaming v2 sidecar protocol

The streaming v2 write path spans two processes. The Go connector owns
transactions and row encoding; a Python sidecar owns the Snowpipe Streaming
SDK. They talk over one socket with the NDJSON RPC protocol described here.
The client is `streamv2_rpc.go`, the server is `sidecar/snowpipe_sidecar/`.

## Lifecycle

```
Connector (Go)                          Sidecar (Python)
      |                                         |
      |-- spawn; auth token on stdin ---------->|
      |<------------------ ready line on stdout |
      |-- dial the socket --------------------->|
      |                                         |
      |-- configure {profile, auth} ----------->|
      |<------------------------------------ ok |
      |-- open_channel (one per channel) ------>|
      |<--------- status {committed_token, ...} |
      |                                         |
      |  per transaction:                       |
      |-- append header + row payload --------->|
      |<------------------------- ok {appended} |
      |-- wait_commit {token} ----------------->|
      |<-------------------------------- status |
      |                                         |
      |-- shutdown ---------------------------->|
      |<----------------------- ok, then exit 0 |
```

The connector spawns the sidecar and writes a random auth token to its stdin,
where no argv or environ listing can see it. The sidecar binds a unix domain
socket (or, started with `--tcp`, an ephemeral localhost port) and prints one
line to stdout:

```json
{"ready": true, "transport": "uds"}
{"ready": true, "transport": "tcp", "port": 54321}
```

The connector dials the socket. The first request must be `configure`, and it
must echo the auth token. Everything after that is channel work.

## Framing

Every request and every response is one JSON line. `id` matches a response to
its request, so the connector may keep many requests in flight and responses
may arrive in any order.

```
→ {"id": 7, "op": "channel_status", "params": {"channel": "c1"}}
← {"id": 7, "ok": true, "result": {"committed_token": "41", ...}}
```

`append` is the one exception. Its rows follow the header line as a raw
payload of exactly `payload_len` bytes — a JSON array of row objects — and the
next line begins right after them. Neither side's reader parses the rows: the
sidecar verifies the array holds `row_count` objects, then hands their bytes
to the SDK as they are.

```
→ {"id": 8, "op": "append", "params": {"channel": "c1", "start_token": "41",
     "end_token": "42", "row_count": 2, "payload_len": 33}}
→ [{"k":1,"v":"a"},{"k":2,"v":"b"}]
← {"id": 8, "ok": true, "result": {"appended": 2}}
```

## Operations

| Op | Params | Result |
| --- | --- | --- |
| `configure` | `profile`, `auth` | — |
| `open_channel` | `database`, `schema`, `table`, `channel` | channel status |
| `append` | `channel`, `start_token`, `end_token`, `row_count`, `payload_len` | `{"appended": n}` |
| `wait_commit` | `channel`, `token`, `timeout_s` | channel status |
| `channel_status` | `channel` | channel status |
| `close_channel` | `channel`, `drop` | — |
| `shutdown` | — | — |

A channel status is Snowflake's authoritative answer for the channel:

```json
{"committed_token": "42", "rows_error_count": 0, "last_error_message": ""}
```

`committed_token` is null when the channel has never committed.
`rows_error_count` counts rows Snowflake rejected over the channel's whole
life; a clean commit does not reset it.

- `open_channel` opens or reopens a channel. Its status carries the committed
  token that recovery reads.
- `append`'s tokens are those of the batch's first and last rows.
- `wait_commit` blocks until the channel's committed token equals `token`.
- `close_channel` with `drop` also drops the channel in Snowflake, discarding
  its committed token instead of leaving it for a later open of the same name.
- `shutdown` flushes everything and exits 0. Closing the socket does too.

## Ordering

The sidecar runs `configure`, `open_channel`, and `shutdown` one at a time.
Each channel gets its own worker, so ops on the same channel run in the order
they were sent, and a slow `wait_commit` on one channel never delays an
`append` to another.

## Errors and failure

A failed op answers with `ok: false` and a stable `code` for logs:

```json
{"id": 9, "ok": false, "code": "invalid_rows", "error": "append declared 2 row(s) but its payload holds 3"}
```

The failure policy is crash-only. Every error is fatal to the write path;
neither side retries an op. The sidecar exits non-zero on a malformed request
or an unhandled exception. The connector never restarts it: a dead sidecar or
a broken socket fails the task, the runtime restarts the connector, and
recovery replays from the offset tokens Snowflake holds. Each op also carries
a Go-side timeout, and a timeout abandons the socket, because a connection
that missed a response can no longer be trusted to match ids.

## Logging

The socket carries no logs. The sidecar writes JSON ops-log lines to stderr,
and the connector relays them into its own log stream.
