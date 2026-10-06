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

The connector spawns the sidecar with `--uds <path>`, naming a socket inside a
temporary directory that only the connector's user can enter. It writes a
random auth token to the sidecar's stdin, where no argv or environ listing can
see it. The sidecar binds the socket, accepts exactly one connection, and
prints one line to stdout:

```json
{"ready": true}
```

The connector dials the socket. The first request must be `configure`, and it
must echo the auth token. Everything after that is channel work.

The token stops a process that reaches the socket before the connector does
from issuing ops on it. That protection adds little to the directory's
permissions, because only a process running as the connector's user can reach
the socket, and such a process can read the token from the connector's memory.
The token is defense in depth. It cannot stop an interloper from taking the
one accepted connection, which fails the connector's dial and so the task.

## Framing

Every request and every response is one JSON line. `id` matches a response to
its request, so the connector may keep many requests pending and responses
may arrive in any order.

```
→ {"id": 7, "op": "channel_status", "params": {"channel": "c1"}}
← {"id": 7, "ok": true, "result": {"committed_token": "41", ...}}
```

`append` is the one exception. Its rows follow the header line as a raw
payload of exactly `payload_len` bytes, holding a JSON array of row objects,
and the next line begins right after them. The sidecar's reader consumes the
payload without parsing it. The channel's worker then scans the array to check
that it holds `row_count` rows and that each row is an object, and hands each
row's bytes to the SDK as they are.

```
→ {"id": 8, "op": "append", "params": {"channel": "c1", "start_token": "41",
     "end_token": "42", "row_count": 2, "payload_len": 33}}
→ [{"k":1,"v":"a"},{"k":2,"v":"b"}]
← {"id": 8, "ok": true, "result": {"appended": 2}}
```

## Operations

| Op               | Params                                                            | Result            |
|------------------|-------------------------------------------------------------------|-------------------|
| `configure`      | `profile`, `auth`                                                 | —                 |
| `open_channel`   | `database`, `schema`, `table`, `channel`                          | channel status    |
| `append`         | `channel`, `start_token`, `end_token`, `row_count`, `payload_len` | `{"appended": n}` |
| `wait_commit`    | `channel`, `token`, `timeout_s`                                   | channel status    |
| `channel_status` | `channel`                                                         | channel status    |
| `close_channel`  | `channel`, `drop`                                                 | —                 |
| `shutdown`       | —                                                                 | —                 |

A channel status is Snowflake's authoritative answer for the channel:

```json
{"committed_token": "42", "rows_error_count": 0, "last_error_message": ""}
```

`committed_token` is null when the channel has never committed.
`rows_error_count` counts rows Snowflake rejected over the channel's whole
life. A clean commit does not reset it, and it survives both a channel reopen
and a new client session.

- `open_channel` opens a channel by name, including one that an earlier
  session left in Snowflake. Its status carries the committed token that
  recovery reads. Opening a name that this session already holds open is an
  error.
- `append`'s tokens are those of the batch's first and last rows.
- `wait_commit` blocks until the channel's committed token equals `token`, or
  fails after `timeout_s` seconds.
- `close_channel` with `drop` also drops the channel in Snowflake, discarding
  its committed token instead of leaving it for a later open of the same name.
- `shutdown` closes every channel and client, which flushes what the SDK has
  buffered, then replies and exits 0. It does not wait for channel ops still
  queued behind it.

## Ordering

The sidecar runs `configure`, `open_channel`, and `shutdown` one at a time on
a control worker. Each open channel gets its own worker, so ops on the same
channel run in the order they were sent, and a slow `wait_commit` on one
channel never delays an `append` to another.

A channel's worker exists only once its `open_channel` has succeeded, and an
op sent to a channel without a worker fails with `unknown_channel`. The
connector therefore awaits the `open_channel` reply before it sends any op on
that channel.

## Errors and failure

A failed op answers with `ok: false`, an `error` message, and a `code`:

```json
{"id": 9, "ok": false, "code": "invalid_rows", "error": "append header states 2 row(s) but its payload holds 3"}
```

The sidecar's own codes are `auth`, `protocol`, `unknown_channel`,
`invalid_rows`, and `sdk_error`. An SDK error that names its own error code
carries that name instead. The connector may branch on a code, so the
sidecar's codes are stable. The connector does so for `unknown_channel` when
it drops a channel, because a channel it has not opened must be opened before
it can be dropped.

The failure policy is otherwise crash-only. Any other failed op is fatal to
the write path, and the connector retries none. The sidecar retries only an
`append` that the SDK refuses as `RECEIVER_SATURATED`, because that is the
SDK's flow-control signal. It backs off for up to 90 seconds, which stays
under the connector's `append` timeout. The sidecar exits non-zero on a
malformed request or an unhandled exception. The connector never restarts it.
A dead sidecar or a broken socket fails the task, the runtime restarts the
connector, and recovery replays from the offset tokens Snowflake holds. Each
op also carries a Go-side timeout, and a timeout abandons the socket, because
a connection that missed a response can no longer be trusted to match ids.

When the connection closes, or the sidecar receives SIGTERM, it exits 0
without flushing. Anything the SDK still buffered is recovered the same way,
from the offset tokens.

## Logging

The socket carries no logs. The sidecar writes JSON ops-log lines to stderr,
and the connector relays them into its own log stream. A stderr line that is
not ops-log JSON, such as output from the SDK's native core, is wrapped in a
log record rather than passed through.
