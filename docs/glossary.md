# Glossary

Terms used across Estuary and this repository.
Where a term is overloaded, each sense is listed, with the sense this
repository usually means first.

### Acknowledgement

1. In the capture protocol, `Request.Acknowledge` tells a connector that some
   number of its checkpoints have durably committed.
   It is sent only if the connector asked for it in `Response.Opened`.
   See [Request.Acknowledge](capture/README.md#requestacknowledge).
2. In the materialization protocol, `Request.Acknowledge` opens every
   transaction by telling the connector that the previous transaction has
   committed to the recovery log.
   A connector that stages data and applies it after the commit does that
   work here, idempotently, and answers with `Response.Acknowledged`.
   See the [materialization document](materialize/README.md).
3. An "ack" in the source system's sense: deleting an SQS message, acking a
   Pub/Sub message, or answering a webhook with a 200.
   Connectors perform this source-side ack only after receiving the runtime's
   acknowledgement for the checkpoint that covered the data.

### Agent

1. `flow-agent`, or just "the agent": the control plane process that runs
   background work driven by the control plane database, such as
   publications, discoveries and the per-task controllers.
   When something is described as happening "in the control plane" for a
   task, the agent is usually the process doing it.
2. `agent-api`: the HTTP and GraphQL service that the UI, `flowctl` and
   integrations talk to.
   It shares its logic with the agent through the `control-plane-api` crate.

### Apply

`Request.Apply`, sent before a session opens to let the connector set up or
alter the endpoint for the specification about to run.
For captures it is usually a no-op.
For materializations it is where destination resources are created and
altered, e.g. tables created and columns added, to match the bound
collections and their selected fields.
See [Request.Apply](capture/README.md#requestapply).

### Auto-discover

The control plane re-running `Request.Discover` in the background for a
capture with `autoDiscover` set, by default every two hours, and republishing
the capture if bindings or schemas changed.
See [Request.Discover](capture/README.md#requestdiscover).

### Backfill

1. Reading the existing contents of a resource from the beginning, as opposed
   to reading only new changes.
2. The per-binding `backfill` counter in a capture or materialization
   specification.
   Incrementing it is how a binding is backfilled.
   For a capture, the increment changes the binding's state key, so the
   connector finds no state for it and starts over.
   For a materialization, the increment gives the runtime a fresh read
   position on the source collection, and the connector's Apply deletes and
   recreates the destination resource.
3. In `sqlcapture`, a table's mode while its existing rows are being scanned,
   as opposed to `Active`, when only replication events are emitted for it.
4. In `estuary-cdk`, the `fetch_page` task that pages through a resource's
   history concurrently with incremental capture.

### Binding

The pairing of a collection with an endpoint resource within a task.
A capture binding captures a source resource into a `target` collection, and
a materialization binding materializes a `source` collection into a
destination resource.
A task has an ordered list of them, and protocol messages such as
`Response.Captured` and `Request.Store` refer to a binding by its index in
that list.

### Boilerplate

The Go libraries that implement the protocol plumbing so a connector only
supplies the endpoint-specific parts.
`source-boilerplate` is the capture one, and `materialize-boilerplate` is
the materialization one.

### Build

A publication produces a build of the affected specifications, identified by
a build id.

### Capture

1. A task that reads from a source system into collections.
2. The single long-lived RPC over which a capture connector and the runtime
   communicate, `rpc Capture(stream Request) returns (stream Response)`.
   There is no message named `Capture`.
3. `CaptureSpec`, the built form of a capture specification that the runtime
   hands to the connector in `Request.Apply` and `Request.Open`.

### Catalog

The set of specifications a tenant has published: captures, collections,
materializations, derivations, and tests.
Catalog names are slash-separated paths such as `acmeCo/marketing/orders`.
The first segment is the tenant, and any leading path is a prefix.
`acmeCo` is the conventional prefix in connector test catalogs, which live in
`test.flow.yaml` files next to the connector code.

### CDC

Change data capture: observing row-level changes by reading a database's
replication log, such as the Postgres write-ahead log or the MySQL binlog,
rather than by querying tables.

### CDK

1. `estuary-cdk`, the Python library for writing capture connectors in this
   repository.
2. The Airbyte CDK, a third-party framework.
   Connectors that wrap an Airbyte connector do so through `estuary-cdk`'s
   `CaptureShim`, and are called shim connectors.

### Checkpoint

1. `Response.Checkpoint`, a message from the connector marking a point at
   which every preceding document is complete and carrying a connector state
   update.
   The documents since the previous checkpoint plus the checkpoint itself
   form a checkpoint sequence.
   See [Response.Checkpoint](capture/README.md#responsecheckpoint).
2. A synthetic checkpoint, which the runtime injects when a checkpoint
   sequence grows past its byte limit.
   It advances no connector state and degrades delivery to at-least-once.
3. The runtime checkpoint: the runtime's record of how far it has read the
   source collections, normally kept in the recovery log.

### Collection

1. A Flow collection: an append-only set of JSON documents with a schema and
   a key, stored as journals in cloud storage.
   Captures write into collections and materializations read from them.
2. A MongoDB or Firestore collection, which is a source resource.

### Combiner

The runtime component that groups the documents of a transaction by
collection key and reduces documents sharing a key.

### Config

1. The endpoint config: the task-level configuration a user fills in, such as
   credentials and an address.
   It follows the connector's `configSchema`.
2. A resource config: the per-binding configuration, following
   `resourceConfigSchema`.
3. `config.yaml`, the file next to a connector's `test.flow.yaml` holding
   encrypted credentials for a test account.

### Connector

A program, packaged as a Docker image, that implements one of Flow's connector
protocols.
Capture connectors are named `source-*` and materialization connectors
`materialize-*`.

### Connector state

The JSON document a connector persists through checkpoints and receives back
in `Request.Open`.
Its meaning is entirely the connector's, but by convention per-binding state
lives under `bindingStateV1`, keyed by state key.
See [Connector state](capture/README.md#connector-state).

### Constraint

The materialization connector's verdict on each projection of a bound
collection, returned in `Response.Validated`.
`FIELD_REQUIRED` and `LOCATION_REQUIRED` mean the field must be
materialized, `FIELD_OPTIONAL` that it may be, `FIELD_FORBIDDEN` that it
cannot be, e.g. a field whose only type is `null`, and `INCOMPATIBLE` that it
cannot be until the binding is backfilled, e.g. because the existing column
has a type that cannot be migrated.
The field selection is negotiated from these constraints.

### Control plane

The UI, `agent-api`, the agent and the Supabase database that together hold
specifications and run publications, discoveries and controllers.
Contrast with the data plane, which runs tasks.
`Request.Spec`, `Request.Discover` and `Request.Validate` are requested by
the control plane but executed by a connector container in a data plane.

### Controller

A control plane background job, run by the agent, that watches one catalog
entity and reconciles it.

### Cursor

1. A source API's notion of position: a pagination token or a
   modification-time filter.
2. In `estuary-cdk`, a `LogCursor` (the position of incremental capture) or a
   `PageCursor` (the position within a backfill).
3. In `sqlcapture`, the top-level `cursor` of the connector state, which is
   the replication log position shared by every binding.
4. Loosely, any resume point recorded in connector state.

### Data plane

The unit of compute that runs customer pipelines.
A data plane is a set of machines running a reactor, which executes tasks,
Gazette brokers, which store and serve journals, and etcd, which coordinates
them, plus Dekaf where a customer needs it.
Connectors run here, in containers started by the reactor.
Planes come in three kinds: public planes shared by most customers, private
planes dedicated to one customer, and BYOC planes running inside a customer's
own cloud account.

### Dekaf

Estuary's Kafka API compatibility layer, which lets Kafka consumers read
collections as if they were Kafka topics, with no Kafka broker involved.
It is deployed on a data plane on demand, and is one of the consumers that
acts on `_meta/op`.

### Delta updates

A materialization mode in which the runtime skips loading current documents
from the destination and stores the combined change of each key within the
transaction, rather than a fully reduced document.
The alternative, standard updates, loads each key's current document,
reduces the new documents into it, and stores the result.

### Derivation

A task that transforms one or more collections into another collection.

### Discovery

`Request.Discover` and what the control plane does with its result: proposing
one binding per resource and merging the proposals into a draft capture.
See [Request.Discover](capture/README.md#requestdiscover).

### Document

A JSON record in a collection.

### Draft

A set of proposed specification changes awaiting publication.
Discovery produces a draft, and publishing a draft makes its specifications
live.

### Endpoint

1. In Flow, the external system a task connects to, configured by the
   endpoint config: the source of a capture or the destination of a
   materialization.
   A task's `endpoint` stanza names the connector image and its config.
2. An HTTP API endpoint, meaning a URL.
   In this repository, "endpoint" without qualification means the first
   sense.

### [etcd](https://etcd.io/)

The coordination store in a data plane.
Gazette brokers and the reactor use it for leader election and for tracking
which shards and journals are assigned where.

### Evolution

A change to a collection that its readers cannot absorb in place, resolved
by resetting the collection.
A reset is equivalent to deleting and recreating the collection as a single
publication, and gives it a new generation.

### Exactly-once and at-least-once

1. For captures, whether documents can be captured twice after a failure.
   The runtime commits documents and connector state atomically, so a
   session never resumes from state that is ahead of what was published, but
   exactly-once delivery still depends on the source offering a precise
   resume point and the connector recording it.
   Otherwise the capture is at-least-once, which is harmless under
   last-write-wins or `merge` reductions and harmful under `sum` or `append`.
2. For materializations, whether a transaction's documents can be applied to
   the destination twice.
   A connector is exactly-once if it commits each Flow transaction in one
   destination transaction guarded by a fence, or stages the transaction and
   applies it idempotently after the recovery log commit.
   A connector that writes directly to a non-transactional destination is
   at-least-once.
   See the patterns in the [materialization document](materialize/README.md).

### Feature flag

A comma-separated list of flags at `/advanced/feature_flags` in an endpoint
config, used to change a connector's behavior for one task without changing
it for all.
See [feature_flags.md](feature_flags.md).

### Fence

1. Gazette recovery log fencing: the mechanism by which only the current
   instance of a shard can commit, so a zombie instance's transactions are
   rejected.
2. In materializations, a barrier the connector installs in the destination
   so that a zombie instance cannot commit there.
3. In `sqlcapture`, a watermark the connector writes to the database and then
   reads back from the replication stream, proving it has caught up to at
   least that point.
   "Streaming to a fence" means reading replication events until that marker
   appears.

### Field selection

The set of a collection's projections that a materialization binding
materializes as fields, e.g. as the columns of a table.
"Field" is overloaded with a property of a JSON document, which is why the
protocol says "projection" for the document location and "field" for its
materialized form.

### Flow

The backend that runs Estuary's core product.
This name used to refer to the product, but we're now referring to the
product as Estuary.
The name still refers to the backend that runs the product, and it is the
name of the repository holding the runtime and control plane.

### flowctl

1. The command-line tool for working with Flow: inspecting the catalog,
   querying statistics, and sending protocol requests to connectors
   directly through its `raw` subcommands.
2. The same binary running inside a data plane as the reactor, where it
   executes tasks rather than serving a human operator.
3. `flowctl-go`, also called `gazctl`, a legacy Go binary that still covers
   some shard-level operations the current tool does not.

### flow-connector-init

The program the runtime installs as the entrypoint of every connector
container.
It starts the connector as a child process and translates between the
runtime's gRPC and the connector's stdin and stdout.

### [Gazette](https://github.com/estuary/gazette)

The distributed log and storage layer Flow is built on.
A journal is a Gazette append-only log, and collections and recovery logs
are journals.
Gazette brokers, of which a data plane runs several, persist journal data
and serve reads and writes, storing the content as fragment files in cloud
storage.

### Inferred schema

The schema the runtime derives from the documents of a collection, together
with any sourced schemas.
It only ever widens.
It reaches users through a collection's `readSchema` when that schema
references `flow://inferred-schema`, which discovery sets up for schemas
annotated with `x-infer-schema: true`.

### Interval

1. A capture's `interval`, the minimum time between sessions when the
   connector exits after capturing what is available.
   It defaults to five minutes and is measured from the previous session's
   start.
2. In `estuary-cdk`, a resource's `interval`, the time to wait before
   invoking a fetch function again after it returned without new data.
3. The auto-discover interval, by default two hours.

### Key

1. The collection key: the JSON pointers that identify a document, so that
   documents sharing a key are reduced together.
2. A fallback key: a collection key the connector proposed for lack of a
   natural identifier.
3. The state key: the identifier under which a binding's connector state is
   kept, derived from its resource path and backfill counter.
4. A key range: the `keyBegin` and `keyEnd` a shard is responsible for.
5. A primary key or unique index in a source database, which discovery
   proposes as the collection key.
6. A materialization's group-by key: the fields a binding groups documents
   by in the destination, which default to the collection key and may be
   set with `fields.groupBy`.

### Load

`Request.Load`, sent by the runtime for each document key a transaction will
touch, asking the connector for the document currently stored under that
key in the destination.
The connector answers with `Response.Loaded`, or with nothing if the key does
not exist.
`Request.Flush` marks the end of the loads, and `Response.Flushed` confirms
every existing key has been returned.
With delta updates there is no Load phase at all.
See [Request.Load](materialize/README.md#requestload).

### Materialization

1. A task that writes collections into a destination system and keeps them
   up to date there.
   Its protocol is described in the
   [materialization document](materialize/README.md).
2. The single long-lived RPC over which a materialization connector and the
   runtime communicate whose transactions cycle through Acknowledge, Load
   and Store phases.
3. `MaterializationSpec`, the built form of a materialization specification
   handed to the connector in `Request.Apply` and `Request.Open`.

### Merge patch

RFC 7396 JSON merge patch, the way connector state updates combine with prior
state when `mergePatch` is true.
Objects merge recursively, `null` deletes a key, and any other value
replaces what was there.

### `_meta`

1. The object every captured document carries, with the document's `uuid`,
   its operation `op` (`c`, `u` or `d`), connector-specific `source`
   metadata, and for change data capture the row's prior state in `before`.
   See the public [metadata fields guide][metadata-fields].
2. `_meta/path` inside a resource config, where the control plane stores the
   binding's resource path.

### onIncompatibleSchemaChange

A materialization binding setting that decides what happens when the
connector reports an `INCOMPATIBLE` constraint for a field the binding
materializes.

### Ops catalog

The `ops/<tenant>/logs` and `ops/<tenant>/stats` collections, where the
runtime collects task logs, including everything a connector writes to
stderr, and task statistics.

### Priority

1. On a materialization binding, an integer ordering how bindings are
   processed.
   All available documents of a higher-priority binding are processed before
   any of a lower-priority one, so a newly added binding with a long backlog
   need not starve the others.
2. In `sqlcapture` resource configs, an integer ordering which tables are
   backfilled first.

### Projection

A named location within a collection's documents, given as a JSON pointer,
that materializations can map to a field and that can define a logical
partition.
A logical partition stores the documents sharing a projected value in their
own journals.

### Publication

1. In Flow, the control plane operation that validates a draft and makes its
   specifications live.
2. In Postgres, a named set of tables exposed for logical replication.
   `sqlcapture` adds captured tables to the capture's publication.

### Reactor

The process in a data plane that executes tasks.
It is the `flowctl` binary running in its data plane role: it takes shard
assignments, starts connector containers, drives them over the connector
protocols, and writes their output to Gazette journals.
A data plane runs one reactor by default and more on large planes.

### Recovery log

The Gazette journal in which a shard's state is durably recorded, including
the connector state.
Transactions commit to it, and it is fenced against zombie instances.
For a materialization it is the authority on what has been committed unless
the connector keeps the runtime checkpoint in the destination instead, in
which case the destination is.

### Redaction

1. `redact` annotations in a collection schema, which the runtime applies to
   captured documents using a per-task salt.
2. The sanitizers in connector tests that replace volatile values such as
   timestamps and ids so that snapshots compare stably.

### Reduction

How documents sharing a collection key combine, declared with `reduce`
annotations in the schema.
The default is last-write-wins, and others may be used in
discovered schemas.
A materialization with standard updates keeps a fully reduced document per
key in the destination by loading the current one and reducing new
documents into it.
See the public [reduction strategies][reduction-strategies] reference.

### Resource

The thing in the endpoint that a binding is bound to.
For a capture it is what is read: a table, a stream, an object type of an
API, or a file prefix.
For a materialization it is what is written: a table, a topic, or a folder.
Its resource path is the list of components that identify it within the
endpoint.

### Runtime

Flow's machinery for running tasks, and in particular the process that drives
a connector over its protocol, commits transactions, and persists connector
state.

### Schema

1. A collection's schemas: `writeSchema`, which captured documents must
   satisfy, and `readSchema`, which readers see.
   The connector's discovered schema is installed as
   `flow://connector-schema`, and the inferred schema as
   `flow://inferred-schema`.
2. The JSON schemas in `Response.Spec`: `configSchema` for the endpoint
   config and `resourceConfigSchema` for resource configs.
3. A sourced schema, sent by the connector during a session to widen the
   inferred schema.
4. A database schema, meaning a namespace of tables.
   `sqlcapture` calls this `namespace` in its resource configs and the table
   `stream`.

### Session

One run of a connector from `Request.Open` until it exits or is stopped.
`flowctl raw preview-next --sessions` counts these.

### Shard

One running instance of a task, responsible for a key range.
See [Key ranges](capture/README.md#key-ranges).

### Snapshot

1. In `estuary-cdk`, a snapshot resource: one whose entire contents are
   fetched on each invocation, emitted only when they changed, and keyed by
   position with `/_meta/row_id`.
2. A test snapshot: the recorded output a connector test compares against,
   under `.snapshots/` for Go connectors and `tests/snapshots/` for Python
   ones.
3. Reading the current contents of a table or resource in full.

### [sops](https://github.com/getsops/sops)

The tool used to encrypt secret fields of endpoint configs, both in
production and in test `config.yaml` files.
Flow decrypts the config before handing it to the connector.

### Source

1. The system a capture reads from, and the `source-*` prefix of capture
   connector names.
2. On a materialization binding, the `source` stanza naming the collection to
   materialize, optionally with partition selectors and a time range.

### Sourced schema

`Response.SourcedSchema`, a schema the connector sends during a session
describing what the source says a binding's documents look like.
It must be closed and maximally restrictive, and takes effect only once a
checkpoint follows it.
See [Response.SourcedSchema](capture/README.md#responsesourcedschema).

### Spec

1. `Request.Spec` and `Response.Spec`, the exchange in which a connector
   describes its configuration schemas, documentation, and OAuth2 support.
   See [Request.Spec](capture/README.md#requestspec).
2. A catalog specification: the YAML definition of a capture, collection or
   other entity.
3. `CaptureSpec`, the built protobuf form of a capture specification.

### Staging

A durable, temporary place in or near the destination where a materialization
writes a transaction's documents before committing them, such as a staging
table or files in a bucket.
Staging lets a transaction be committed in one step, or applied idempotently
after the recovery log commit, and staging resources are named with the
shard's key range so that shards do not collide.

### StartCommit

`Request.StartCommit`, sent after the last `Request.Store` of a
transaction, carrying the runtime checkpoint.
The connector finishes processing its stores, begins committing to the
destination, and answers `Response.StartedCommit` with a connector state
update, which the runtime then commits to the recovery log along with the
transaction.

### State key

The identifier under which a binding's connector state is kept.
It is derived from the binding's resource path and backfill counter, so
incrementing the counter gives the binding a new state key with no state
under it.
See [Request.Validate](capture/README.md#requestvalidate).

### Store

`Request.Store`, sent by the runtime once per document key in a transaction
during the Store phase, carrying the key, the selected field values, the
full document, whether the key already `exists` in the destination, and
whether it is a `delete`.
The connector persists or stages the document.
See the [materialization document](materialize/README.md).

### Stream

1. The gRPC stream that is the capture RPC or the materialization RPC.
2. A replication stream from a database.
3. In `sqlcapture` resource configs, the `stream` field, which is the table
   name, alongside `namespace` for the schema.
4. A Kinesis stream or Kafka topic, as a source resource.

### Supabase

The control plane's database, a hosted PostgreSQL.
It holds the catalog, task assignments, publications, discoveries,
inferred schemas, connector statuses and data plane metadata.

### Task

A capture, materialization or derivation: a named catalog entity that runs
as one or more shards.

### Tombstone

A document with `_meta/op` set to `"d"`, carrying at least the collection
key, which signals that the document with that key was deleted at the
source.
Whether it becomes a soft or hard delete is decided downstream.
See the public [deletions guide][deletions].

### Transaction

1. A capture transaction: the runtime's atomic commit of some number of
   checkpoint sequences, publishing their documents and persisting the
   reduced connector state together.
2. A materialization transaction: one pass through the Acknowledge, Load and
   Store phases, ending with `Request.StartCommit`, which applies a batch of
   collection documents to the destination.
   Consecutive transactions are pipelined, so the next one's loads begin
   while the previous one commits.
3. A transaction in the source or destination database.

### Watermark

1. A cursor that asserts everything before it has been captured, such as
   "every row with an id up to 1200".
2. In `sqlcapture`, the rows the connector writes to its watermarks table to
   create a fence in the replication stream.
3. In the protocol definition, a checkpoint is described as a watermark in
   the stream of captured documents.

### Zombie

A stale instance of a task that is still running after a replacement has
started.
The recovery log rejects its commits.
For a capture that leaves only source-side effects, e.g. holding a
replication slot.
For a materialization, which writes to the destination itself, the connector
also has to fence the destination, or a zombie can commit stale data there
before the recovery log gets the chance to reject it.

[metadata-fields]: https://docs.estuary.dev/guides/advanced-usage/metadata-fields/
[reduction-strategies]: https://docs.estuary.dev/reference/reduction-strategies/
[deletions]: https://docs.estuary.dev/reference/deletions/
