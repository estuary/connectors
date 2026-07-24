# source-sqs benchmarks

What we measured while building this connector, and why the pipeline sizing
constants at the top of `pull.go` are what they are. Reproduction runbooks
are at the end. See [ARCHITECTURE.md](ARCHITECTURE.md) for the design these
numbers validate.

## Sizing quick reference

A single task shard running the shipped constants demonstrated
**~37M messages/hour** (10,400 msg/sec) end-to-end against real AWS with
2 KB messages, over a ~38ms effective cross-cloud round trip. The bottleneck
was receive round-trip time, so at in-region RTTs the same fleet arithmetic
projects ~14–16k msg/sec, about **50M messages/hour**.

A shard's rate is the smaller of two limits. The message-count ceiling comes
from API round trips through the receive and delete fleets (measured
10.4k/sec cross-cloud, ~14k/sec projected in-region) and is what caps small
messages. The byte ceiling of at least ~21 MB/sec (~75 GB/hour) is the
highest byte rate demonstrated; the commit pipeline was unloaded at that
rate, so it's really a measured floor, not a true ceiling. Estimated rate ≈
min(message ceiling, 21 MB/sec ÷ average message size), which gives per
shard:

| Avg message size | Limiting factor | Msgs/hour per shard |
|---|---|---|
| ≤ 1 KB | message rate | ~37M measured, ~50M projected in-region |
| 2 KB | transitional | **~37M measured** |
| 4 KB | bytes | ~19M |
| 8 KB | bytes | ~9.5M |
| 16 KB | bytes | ~4.7M |
| 64 KB | bytes | ~1.2M |
| 256 KB | bytes | ~295k |
| 1 MiB (max) | bytes | ~74k |

The rows for sizes above 2 KB are conservative, since the byte ceiling is a
measured floor.

Caveats:

- Journals, not shards, are the first Flow-side lever. Scaling was
  linear with journal count (1 journal ≈ 1.8k/s, 4 ≈ 7.2k/s,
  8 ≈ connector-bound), and the top rates assume ~8 journals, which the v2
  runtime provides by splitting automatically. A single-journal collection
  caps near 6.5M msgs/hour regardless of the connector.
- In-region RTT of ~20–30ms is assumed. The fleets are sized in round
  trips, so higher RTT divides these numbers proportionally.
- These are drain rates against a deep backlog, meaning what a shard can
  catch up at, not a required arrival rate.
- FIFO queues follow different math. Per-group throughput is ~10
  messages per commit round trip, so aggregate FIFO rate is active groups
  times the per-group rate, capped by the standard-queue ceilings. At
  measured commit latencies that is order-100 msg/sec per group, so a FIFO
  queue needs roughly 100 or more active groups to approach the standard
  numbers.

## Methodology

Testing ran at five tiers, each measuring something the previous one can't.
All five were run during development.

| Tier | What's real | What it measures |
|---|---|---|
| 1. Go test suite (CI) | connector code, LocalStack SQS | correctness, FIFO semantics |
| 2. Local stress tests | full `Pull` pipeline, LocalStack | relative regressions, CPU profiles |
| 3. `flowctl preview` | runtime protocol machinery | protocol correctness end-to-end |
| 4. mise stack + LocalStack | real recovery log + journals | commit+ack latency, per-shard ingest |
| 5. EC2 + real AWS SQS | real SQS | absolute receive+delete rate |

For throughput runs you don't need a producer that sustains the target
rate, just a deep backlog: ReceiveMessage doesn't care when messages
arrived, so pre-seed the queue and measure how fast it drains.

## Measured results

### AWS SQS (tier 5)

1M × 2 KB messages per run, real queue in us-east-2 with untouched default
settings including the 30s visibility timeout, captured by a published task
on a GCP-VM stack at ~25ms cross-cloud RTT, matching the ~20ms design basis.

| Run | Sustained end-to-end rate | Bottleneck | Commit queueing (ackLatency avg / max) | Redelivered |
|---|---|---|---|---|
| 1 journal | ~1,800 msg/sec | journal append capacity | saturated | 0 |
| 4 journals | ~7,200 msg/sec ≈ 60 GB/hr | journal append capacity | saturated, 3.5s / 4.9s | 50 in 1M (cold start) |
| 8 journals, 30 delete workers | ~8,650 msg/sec ≈ 71 GB/hr | delete fleet | none, 13ms | 10 in 1M |
| 8 journals, 48 delete workers | **~10,400 msg/sec ≈ 21 MB/sec ≈ 75 GB/hr** | receive round trip | none, 13ms / 94ms | 20 in 1M (cold start) |

How to read the table: the bottleneck is whatever capped each run's rate,
and ackLatency is the commit round trip, so seconds mean a standing queue
ahead of the commit pipeline while milliseconds mean it is unloaded.

What we learned from these runs:

- Journal scaling is linear until the connector becomes the bottleneck.
  Scaling went 1 journal → 1.8k/s, 4 → 7.2k/s, 8 → connector-bound. With 8
  journals the commit pipeline is unloaded, with 13ms ack latency at
  saturation, and the bottleneck was the delete-worker fleet. At the
  then-configured 30 workers, 30 × 10 handles ÷ ~29ms RTT predicts ≈ 8.6k
  deletes/sec, exactly as measured, with `unacked` pinned at the cap full
  of acked-but-undeleted messages.
- Default SQS settings held under saturation. Duplicates were 0.001% to
  0.005% of messages, all from cold-start transients and none ongoing, even
  with the 30s default visibility timeout. Zero delete failures, zero
  expired handles, clean drain to 0/0.
- The emulator is more lenient than the real API. Real `GetQueueAttributes`
  rejects an explicit `FifoQueue` attribute request on standard queues with
  `InvalidAttributeName`, where LocalStack tolerates it. Validate requests
  `All` because this tier caught that.

**Final-configuration run.** The same protocol re-run at the
shipped constants of 40 receivers, 48 delete workers, and the 1,000 unacked
cap. Sustained **~10,400 msg/sec ≈ 21 MB/sec ≈ 75 GB/hour**, draining 1M
messages in 97 seconds from shard enable to an empty queue. The delete-fleet
resize held. `deleted` tracked `received` within a few hundred messages all
run, with zero delete failures and zero expired handles, while `unacked`
floated at ~560 against the 1,000 cap. Ack latency averaged 13ms, so Flow
was unloaded, and redeliveries were 20 in 1M, all in the cold-start
interval. The bottleneck is now receive round-trip time. 40 receivers × 10
messages ÷ the ~38ms effective cross-cloud round trip predicts almost
exactly the observed rate, so an in-region deployment with its lower RTT
should reach the ~14k/sec projection.

### LocalStack ceilings (tier 2 context)

The emulator saturates around 3k msg/sec, or 5–6k API ops/sec, so local
stress numbers are relative signals for regressions and CPU profiles, not
absolute throughput. Point at real AWS for absolute numbers.

## How the pipeline constants were chosen

These measurements are baked into the internal constants in `pull.go`. Fleet
sizing is deliberately connector-internal rather than user configuration.
If we decide to have the capability to tune these pipeline constants to
best suite individual tasks' use cases, we can expose them in the endpoint
config later. The right values fall out of RTT arithmetic, the 1 GiB task
memory ceiling, and the runs above.

- 40 receivers per binding: 10 messages per call at ~20–30ms in-region
  RTT gives ~500 msg/sec per receiver, so 40 sustain ~14–20k msg/sec with
  headroom over the 50M msgs/hour design target. Each receiver reserves a
  10 MiB worst case, so the fleet size also fixes worst-case body memory.
- 48 delete workers: tier 5 measured the delete fleet as the first
  bottleneck at 30 workers, around 8.6k deletes/sec. 48 puts the delete
  ceiling near 17k/sec at the same RTT, above the receive ceiling. The
  final-configuration run confirmed the fix, with the end-to-end rate moving
  to 10.4k/sec and the bottleneck shifting to receive round-trip time.
- 1,000 unacked messages per binding: Little's-law occupancy at target
  rates is ~650–1,400, so the cap covers steady state without letting a
  large backlog of received-but-uncommitted messages build up. It bounds
  worst-case ack latency and leaves over 100 shards of headroom under the
  120k in-flight quota.
- 448 MiB body budget: 40 receiver reservations plus headroom, the hard
  worst-case bound on body memory for any message-size mix. AWS documents no
  total-size cap on a ReceiveMessage response, so a hard guarantee must
  reserve in-flight × 1 MiB. The floor works out to ~350 MiB at 14k/s and
  ~25ms RTT. Steady-state usage is a few MB.

## Reproducing these numbers

Runbooks are kept only for the tiers still worth re-running. Tier 1 is the
ordinary CI test suite, and tier 4's manual journal experiments are retired
because the v2 runtime splits journals automatically, with its findings
folded into the measured results above.

### Local stress tests (LocalStack)

```bash
docker compose -f source-sqs/docker-compose.yaml up --wait
STRESS_TEST=yes TEST_DATABASE=yes go test ./source-sqs/ -v \
  -run TestStressCapture -cpuprofile cpu.out
```

Knobs via env: `STRESS_MESSAGES` (default 20000), `STRESS_BODY_BYTES` (1024),
`STRESS_GROUPS` (FIFO, 32), `STRESS_ACK_DELAY` (simulated commit latency),
and `STRESS_PARTS` (adds a low-cardinality `part` field for capturing into
logically-partitioned collections, which is how the journal-scaling runs
were seeded). See the `stress_test.go` header for the transform
microbenchmark and the seed-only load generator, `TestStressSeed` with
`STRESS_QUEUE_URL`.

### Real-AWS throughput (tier 5)

Run from EC2 in the queue's region, or the numbers measure your ISP. A 2–4
vCPU instance matches the sizing basis. Credentials need `sqs:CreateQueue`,
`sqs:DeleteQueue`, `sqs:SendMessage`, `sqs:ReceiveMessage`,
`sqs:DeleteMessage`, and `sqs:GetQueueAttributes`. The test creates and
cleans up its own queues.

```bash
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
STRESS_TEST=yes TEST_DATABASE=yes \
STRESS_MESSAGES=1000000 STRESS_BODY_BYTES=2048 \
  go test ./source-sqs/ -v -run 'TestStressCapture$' -timeout 30m
```

Watch the capture rate, the 30s stats lines with `msgsPerSec` steady and
`queueDepth` falling linearly, a short delete drain at the end, and instance
CPU, which should be about 2 cores at the target rate. A shortfall is either
CPU-bound or latency-bound, and the fix for the latter is the
`receiversPerBinding` sizing in `pull.go`.

For the FIFO variant, `-run TestStressCaptureFifo`, the queue needs
high-throughput mode enabled, since the default FIFO quota caps at 3,000
msg/sec, and `STRESS_GROUPS` in the hundreds, or the run measures group
locking rather than capacity. Expected ceiling ≈ `groups × 10 / round-trip`.

### flowctl preview smoke test (tier 3)

Cheap to re-run after connector changes:

```bash
docker compose -f source-sqs/docker-compose.yaml up --wait
docker exec source-sqs-db-1 awslocal sqs create-queue --queue-name preview-stress
./build-local.sh source-sqs
STRESS_TEST=yes TEST_DATABASE=yes STRESS_MESSAGES=50000 \
  STRESS_QUEUE_URL=http://localhost.localstack.cloud:4566/000000000000/preview-stress \
  go test ./source-sqs/ -v -run TestStressSeed

flowctl preview --source sqs-preview.flow.yaml --network flow-test --log-json \
  > preview-docs.jsonl 2> preview-logs.jsonl
```

The `sqs-preview.flow.yaml` spec points the endpoint at
`http://source-sqs-db-1.flow-test:4566` with a collection keyed on
`/_meta/messageId`. Verify the doc count matches the seeded count, the queue
drains to 0/0, which proves deletes flow from the real runtime's
Acknowledge, and stats lines appear on stderr every 30s.

Gotchas observed in practice:

- Captured docs go to stdout, logs to stderr. Without `--log-json` the
  connector's INFO-level stats lines are hidden entirely.
- At high rates flowctl's per-doc stdout printing can itself become the
  bottleneck, so always redirect stdout to a file or /dev/null.
