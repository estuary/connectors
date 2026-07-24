# source-sqs

This is a Flow [capture connector](https://docs.estuary.dev/concepts/captures/)
that captures messages from Amazon SQS queues. It supports both standard and
FIFO queues, auto-detected per queue, and deletes each message from SQS
after Flow has durably committed the checkpoint containing it.

Delivery is at-least-once. Failure modes degrade to a redelivered
duplicate rather than data loss, and duplicates collapse downstream under the
collection key. The connector keeps no state of its own and its checkpoints
are always empty because the queue itself is the record of unfinished work.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the internal design and
[BENCHMARKS.md](BENCHMARKS.md) for measured throughput and the sizing
rationale behind the pipeline constants.

## SQS behaviors that shape this connector

### Visibility timeout

SQS has no acknowledgements of its own. A received message is merely hidden
for the queue's visibility timeout, and unless deleted before that deadline
it becomes visible again and redelivers. This is the connector's safety net,
since a crash between receive and commit loses nothing. It is also the main
duplicate source. A message's receipt handle must stay valid from receive
until Flow commits and the delete lands, so the visibility timeout has to
cover the worst-case commit stall. The connector warns at
Validate if a queue's timeout was turned down below the 30s SQS default and
recommends 180s or more. Handles that expire anyway are detected, skipped,
and counted as `deleteExpired` in the stats, since the message is
redelivering regardless.

### Limits and quotas

- Message bodies are at most 1 MiB, raised from 256 KiB in August 2025.
  Every internal buffer is sized assuming max-size messages, so the connector
  stays memory-bounded for any size mix.
- ReceiveMessage returns at most 10 messages per call.
- 120,000 messages per queue may be in flight at once, shared across all
  consumers of the queue. The connector caps its own unacked messages at
  1,000 per binding.
- Standard queues have no published per-queue throughput quota. FIFO
  queues default to 300 API calls per second per action, which is 3,000
  msg/sec with full batches. High-throughput mode raises that by an order of
  magnitude in major regions.

### FIFO queues

AWS enforces the `.fifo` name suffix at queue creation, so queue type is
detected from the URL. Three FIFO behaviors matter here.

- Group locking: while any message of a group is in flight, SQS returns
  no further messages for that group, queue-wide. Combined with
  delete-after-commit, this gives per-group ordered capture without any
  connector-side machinery, and the guarantee survives running multiple task
  shards. The flip side is that each group sustains roughly 10 messages per
  commit round trip, so aggregate FIFO throughput depends on how many
  groups are active, which is a producer decision.
- Receive retry idempotency: a `ReceiveRequestAttemptId` is set on every
  FIFO receive so that when a call's response is lost, the retry recovers
  the same messages instead of leaving the group locked for the full
  visibility timeout.
- Sequence numbers: SQS assigns each FIFO message a sequence number that
  strictly increases within its group and is stable across redeliveries.
  The discovered collection key is therefore `messageGroupId` plus
  `sequenceNumber`, which clusters materialized tables by group in queue
  order.

## Captured documents

A body that parses as exactly one JSON object is spread into the document
root. Any other body, such as plain text, an array, a scalar, or an object
with trailing content like an NDJSON batch, is captured whole as a string
under `body`. Message metadata lands under `_meta` with `queueUrl`,
`messageId`, `sentTimestamp`, `approximateReceiveCount`, and
`messageAttributes`, plus `messageGroupId`, `sequenceNumber`, and
`deduplicationId` on FIFO queues.

Discovered collection keys are `/_meta/messageId` for standard queues and
`/_meta/messageGroupId` with `/_meta/sequenceNumber` for FIFO queues. Both
are assigned by SQS at send time, so redelivered duplicates carry the same
key and collapse in keyed reduction.

## Configuration

The credentials need `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and
`sqs:GetQueueAttributes` on each bound queue, plus `sqs:ListQueues` for
discovery.

Note that this capture consumes the queue. Messages are deleted once
committed, so other consumers sharing the queue will see only the messages
this connector has not yet received.

## Connector development

The automated tests run against LocalStack:

```bash
# Start the LocalStack SQS emulator
docker compose -f source-sqs/docker-compose.yaml up --wait

# Run the full test suite against it
./source-sqs/run_tests.sh

# Or directly, with snapshot updates
UPDATE_SNAPSHOTS=true TEST_DATABASE=yes go test -v ./source-sqs/
```

Unit tests for URL parsing, config validation, the channel pipeline, the
document transform, and receive-error handling need no backend and run with
`-short`, which is what the Dockerfile build uses. Integration tests cover
standard capture across all body shapes, FIFO multi-group ordering, and
redelivery, and they assert the queue is empty after each capture, which
proves every message went through the full emit, acknowledge, and delete
path.

Stress tests and benchmarks live in `stress_test.go` and never run in CI.
See the file header for usage and [BENCHMARKS.md](BENCHMARKS.md) for the
measurement runbook.
