# source-sqs architecture

Maintainer-facing notes on how this connector works and the invariants to
preserve when changing it. See [README.md](README.md) for an overview and the
SQS behaviors that shaped this design, and [BENCHMARKS.md](BENCHMARKS.md) for
the measurements behind the pipeline sizing constants in `pull.go`.

## Overview

SQS's native primitives map directly onto Flow's capture protocol, so the
connector is a thin transport layer.

| SQS primitive | Flow protocol primitive |
|---|---|
| `ReceiptHandle` | External cursor, per `Opened.explicit_acknowledgements` |
| `DeleteMessage` | Side effect of `Request.Acknowledge` |
| Visibility timeout | Redelivery safety net for un-acked work |
| The queue itself | Persistent record of unfinished work, so no connector state needs persisted |
| FIFO message-group locking | Per-group ordered emission |

## Design principles

1. No connector-owned state. Checkpoints are always `{}`. The queue is the
   record of unfinished work, so there is nothing to resume from.
2. Failure handling leans on redelivery. Every failure path (a crash, a
   network error, a slow commit, an expired handle) results in a duplicate,
   not a lost message.
3. Everything is pipelined. Receivers never block on commit latency and
   the transaction loop never waits for acknowledgements. Many checkpoints are
   outstanding at once, which the protocol explicitly supports; it's the
   reason `Acknowledge.checkpoints` is a count rather than an identifier.
4. Scale comes from goroutines, not coordination. ReceiveMessage on the same
   queue from N consumers returns disjoint sets, so there is no partitioning
   or range assignment anywhere in the connector.
5. Buffers are byte-bounded. Every internal buffer is sized assuming every
   message may be the 1 MiB maximum, so the connector stays memory-bounded for
   any message-size mix.
6. Denied deletes are fatal. The connector has to be able to delete messages
   from the queue before their visibility timeouts expire, and a capture that
   can't delete would otherwise look healthy while duplicates pile up.

## Protocol integration

The connector opts into explicit acknowledgements via `stream.Ready(true)`.
Two protocol properties the bookkeeping relies on:

- `Acknowledge.checkpoints` is a positional count, not an identifier. The
  runtime combines connector checkpoints into transactions and acknowledges
  them as a batch, in checkpoint-emission order. The connector's bookkeeping
  is therefore a simple FIFO of pending entries.
- Acknowledgements never span sessions. The ack counter starts at zero per
  session in both runtimes, so a restarted connector can never receive an
  acknowledgement for a prior process's checkpoints. Checkpoints committed but
  not acknowledged when a session ends just redeliver.

Two runtime behaviors constrain the design:

- Acknowledgements must be consumed within 10 seconds or the runtime fails
  the task, so the ack path can never stall on downstream work.
- The runtime enforces its own read window. It stops reading connector
  output once ~32 MB accumulates in the pending transaction, emission then
  blocks, and the body-bytes semaphore propagates the pause to receivers, so
  no connector-side windowing is needed.

## Pipeline

Four stages connected by bounded channels. Receivers are per binding. The
transaction loop, ack processor, and delete workers are per process. Sizing
constants live at the top of `pull.go`.

```
        ┌─ binding (queue) ────────────────────────────┐
 SQS    │ receiver[0..N) ──► transform (in receiver) ──┼──┐
Receive └──────────────────────────────────────────────┘  │
Message                                                   ▼
                                             batches chan (bounded)
                                                          │
                                        ╔═ transaction loop (1 goroutine) ═╗
                                        ║ 1. read batch, drain more        ║
                                        ║    without waiting               ║
                                        ║ 2. emit Captured documents       ║
                                        ║ 3. pending.push({handles, t₀})   ║
                                        ║ 4. emit Checkpoint{state: {}}    ║
                                        ╚══════════════════════════════════╝
                                                          │ pending FIFO
                                                          ▼
                                        ╔═ ack reader + processor ═════════╗
                                        ║ Request.Acknowledge{N} pops N    ║
                                        ║ entries, dispatches delete jobs  ║
                                        ╚══════════════════════════════════╝
                                                          │ deletes chan
                                                          ▼
 SQS Delete                             ╔═ delete workers (M goroutines) ══╗
 MessageBatch  ◄────────────────────────║ DeleteMessageBatch with retries  ║
                                        ╚══════════════════════════════════╝
```

### Receivers (`receiver.go`)

A fleet of goroutines per binding, each long-polling `ReceiveMessage` at 10
messages per call, the API cap. Before each call a receiver acquires 10 slots
from the binding's `inflight` semaphore and a worst-case 10 MiB from the
process-wide body-bytes budget, trimming both to actual size on response.
Message transform runs here so its CPU cost parallelizes across the fleet
instead of serializing in the transaction loop.

The SDK retryer's 5 attempts absorb transient receive failures. `OverLimit`
gets an explicit backoff-and-retry in the receive loop because the SDK models
it as a non-retryable client fault even though it is transient. It signals
the queue-wide in-flight quota is exhausted, usually by external consumers
sharing the queue. Anything else that survives the SDK's retries fails the
task, which is safe since unreceived messages just remain in the queue.

FIFO receives set an explicit `ReceiveRequestAttemptId` so a retry of a
receive whose response was lost recovers the same messages instead of leaving
the group locked for the full visibility timeout.

A binding that goes a minute without receiving a message shrinks itself. All
but a single scout receiver park, and the scout asks for one message per
call, dropping its reservation from 10 MiB to 1. Any received message
wakes the full fleet within half a second, since long polls return the
moment something arrives. The reason this exists: the budget semaphore
grants in FIFO order, but an empty 20 second long poll holds its
reservation 500x longer than a busy receive does. Without the shrink, a
single idle binding's fleet would hold full worst-case reservations around
the clock and crowd busy bindings out of the shared budget. With it, an
idle binding costs ~1 MiB and dozens of quiet queues can sit alongside one
running at full tilt.

The shrink is re-evaluated in both directions on every receive-loop
iteration, so a binding that goes quiet drains back down to scouts within
one poll cycle after the idle threshold, and oscillating between modes is
free because there is no state to build or tear down. The only ramp cost of
waking is that the scout's first receive carries one message instead of
ten. Bindings start idle for the same reason. A scout's long poll returns
the instant a backlog exists, so a busy queue is at full asks within one
receive, while a restarting capture with many quiet queues avoids a minute
of full worst-case reservations from fleets that have nothing to do.

### Transaction loop (`pull.go`)

The sole emitter of documents and checkpoints. It drains whatever batches are
already waiting into one checkpoint.

Two invariants to preserve:

- Pending entries are pushed before the checkpoint is emitted. The ack
  reader runs concurrently and the runtime can acknowledge a checkpoint the
  instant it is written, so pushing after would race the pop.
- Pending-push order equals checkpoint-emission order. Acknowledgements
  are positional, so this is what maps an ack to the right receipt handles.
  It holds because a single goroutine does both.

### Ack reader and processor (`ack.go`)

`readAcknowledgements` is a detached goroutine that forwards `Acknowledge`
messages. It is detached rather than in the errgroup because stream.Recv
cannot be cancelled. `processAcknowledgements` pops one pending entry per
acknowledged checkpoint and dispatches its delete jobs. This path must
never block, because of the runtime's 10-second ack deadline. The `deletes`
channel is sized so sends cannot block, transitively bounded by the
per-binding unacked caps, and an acknowledgement with no matching pending
entry is a protocol violation that fails the task.

### Delete workers (`ack.go`)

Consume delete jobs, one per receive batch with at most 10 handles, and call
`DeleteMessageBatch`. Partial failures retry the failed subset briefly, then
drop. Every outcome except one returns nil, since dropped or expired handles
mean the message redelivers as a duplicate. The exception is `AccessDenied`,
which fails the task. A capture that can receive but not delete looks
healthy while duplicates pile up without bound, so both Validate, via a
fabricated-handle probe, and the runtime path treat it as fatal.

Jobs carry their receive time, and a handle already past the queue's
visibility timeout is skipped entirely since the message will redeliver
regardless. On shutdown, workers make a brief best-effort drain pass so
already-committed messages are deleted rather than redelivered.

### Stats logger (`stats.go`)

Logs per-queue counters every 30s, covering `received`, `deleted`,
`redelivered`, `deleteExpired`, `deleteDropped`, `unacked`, `msgsPerSec`, and
`queueDepth`, plus checkpoint ack latency, and a `connectorStatus` rollup of
the form "N messages behind across K of M queues" emitted only when it
changes. `deleted` tracking `received` is the health signal that matters.

## Backpressure and memory

Two semaphores gate receivers over different windows of a message's life:

| Semaphore | Scope | Unit | Held | Bounds |
|---|---|---|---|---|
| `inflight` | per binding | messages | receive → delete | redelivery exposure, SQS in-flight quota use |
| `bodyBytes` | process | bytes | receive → emission | connector memory |

The two bounds are independent. Bodies never outlive emission, so memory
stays bounded even with thousands of messages awaiting acknowledgement, and
the unacked cap keeps redelivery blast radius small even when bodies are tiny.

With multiple bindings, the body budget is the shared ceiling and capacity
follows demand without any cross-binding scheduler. Receivers reserve only
what they ask for, idle bindings shrink to scouts (see Receivers above), and
bindings that are all genuinely busy hold same-sized reservations for
similar durations, so the FIFO splits the budget roughly evenly among them.
Aggregate throughput across queues stays near the single-queue ceiling, and
captures with several sustained high-rate queues should split across task
shards, each of which brings its own budget and fleets.

Scouts are cheap but not free, which puts a practical bound on binding
count. At ~1 MiB per idle binding, several dozen quiet queues can sit
alongside one running at full tilt, but in the hundreds of bindings the
scout reservations themselves start to crowd the budget, the stats logger's
serial queue-depth polling stops fitting its 30 second interval, and the
HTTP client's connection cap comes into range. Captures approaching that
scale should split across task shards or separate captures.

There is no feedback cycle. Emission never waits on acknowledgement. If
Flow stops reading, the transaction loop blocks mid-emission, stops
releasing body budget, and receivers pause. Excess backlog just waits in
SQS. Worst case the connector holds ~448 MiB of bodies plus small
overheads, well under the 1 GiB task limit. See BENCHMARKS.md for the
derivation.

## FIFO ordering

SQS locks a message group while any of its messages are in flight. The
connector deletes only after the runtime acknowledges the covering
checkpoint. Composing the two gives ordered capture with no extra machinery:

> Group G's batch k+1 cannot be received, by any receiver on any task shard,
> until batch k's messages are deleted. Batch k is deleted only after its
> checkpoint durably committed. Therefore every document of batch k commits
> before any document of batch k+1 is even received.

This survives scale-out because group locking is enforced queue-globally by
SQS, not per consumer.

Caveats:

- Redelivered duplicates can appear out of order. First deliveries are
  always in order; only duplicates violate it, and they carry the same key
  and content, so the FIFO collection key collapses them.
- Per-group throughput is gated by commit latency. A group is locked from
  receive until delete, so each group sustains at most roughly
  `10 / (receive + commit+ack + delete)` messages per second. Aggregate FIFO
  throughput is `active groups × per-group rate`, and group cardinality is
  decided by the producer, not something the connector can change.

## Document transform (`transform.go`)

A body that is exactly one JSON object is spread into the document root.
Anything else (a scalar, an array, non-JSON text, an object followed by
trailing content such as an NDJSON batch) is captured verbatim under
`body`, so no message shape is ever lost or fails the task. Bodies are
parsed with `UseNumber` so integers beyond float64's 2^53 precision, like
snowflake IDs and nanosecond timestamps, re-serialize exactly. The
connector's `_meta` is written last and always wins over any `_meta` in the
body.

Collection keys are chosen to be stable across redeliveries. SQS assigns
them at send time, so duplicates collapse under keyed reduction.

- Standard queues use `/_meta/messageId`.
- FIFO queues use `/_meta/messageGroupId, /_meta/sequenceNumber`. The pair
  is unique, since sequence numbers strictly increase within a group, and
  clusters materialized tables by group in queue order. `messageId` is
  deliberately not a third component, and `deduplicationId` is not a key
  because SQS only gives it meaning within a 5-minute window.

## Failure modes

| Failure | Handling | Outcome |
|---|---|---|
| Crash mid-batch | Visibility timeout re-exposes messages | Duplicates |
| Receive transient (5xx, throttle) | SDK retries (5 attempts) | None |
| `OverLimit` (shared in-flight quota) | Log, back off 5s, continue | Delayed receive |
| Receive auth denied / queue gone | Fail the task with a clear error | None |
| Lost FIFO receive response | Same `ReceiveRequestAttemptId` on SDK retry | None, group not blocked |
| Delete partial failure | Retry failed handles briefly, then drop | Duplicates after visibility expiry |
| Handle expired before delete | Skip the call, count it | Duplicate (already redelivering) |
| Delete auth denied | **Fatal**, also probed at Validate | None, redelivers to a fixed deploy |
| Commit slower than visibility timeout | Unacked cap bounds exposure, stats surface it | Duplicates, collapsed by key |
| Runtime stalls | Semaphores pause receivers, backlog waits in SQS | Delay only |

## Shutdown

On stream close or SIGTERM the receivers stop, the ack reader exits on EOF,
which surfaces as `errCaptureStopped` and is treated as a clean stop, delete
workers drain briefly, and the process exits. Anything received but not
deleted redelivers after its visibility timeout.

## Scaling

Task shards are the scaling unit and need zero coordination. SQS distributes
work via the visibility model, receipt handles are shard-local, and
acknowledgements are session-scoped. For standard queues N shards give ~N×
throughput. For FIFO, shards help only up to the number of distinct active
message groups. The shared queue-level budgets, meaning the 120k in-flight
quota and receive concurrency, leave room for over a hundred shards at the
internal constants.

Flow-side, journal count is the first bottleneck and the v2 runtime splits
journals automatically. See BENCHMARKS.md for measured scaling data.

## Out of scope

Things considered and skipped, and why:

- Visibility-extension heartbeats. `ChangeMessageVisibility` heartbeats
  would double API volume, and the unacked cap plus a sane visibility
  timeout of 180s or more bounds duplicate amplification without them.
- The SQS Extended Client Library, which stashes payloads of up to 2 GB in
  S3. The throughput math is materially different and it interacts with
  Flow's 64 MB document limit, so it's scoped as a separate feature.

## References

- [FIFO queue delivery logic](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html) for group locking and `ReceiveRequestAttemptId`
- [Amazon SQS message quotas](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)
