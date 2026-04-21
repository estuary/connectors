# Materializations

Materialization [connectors] write Flow [collections] into a destination
system, e.g. a database, warehouse, file storage, messaging queue, etc.
Materializations are processed as cooperative transactions between the Flow
runtime and a connector, over a long-lived RPC through which the runtime and
connector exchange messages.

This RPC workflow maintains a materialized view of a Flow collection in an
external system.
It has distinct Acknowledge, Load, and Store phases.
The Flow runtime and connector cooperatively maintain a fully reduced view of
each document by loading current states from the destination, reducing some
number of updates, and then storing updated documents and checkpoints.

This document's aim is to explain in as much detail as possible the
materialization protocol. For example, how the Flow runtime (an overarching
term for all the components and pieces that drive Flow pipelines, including
the data plane) interacts with a materialization connector, and various
constraints and considerations necessary when implementing a connector.

The actual protocol is defined as gRPC/protobuf [here][protobuf].
Here we will reference some of those message types (message types denoted
with prefixes `Request.` and `Response.`) and will explain their role.
This document also references the [materialize-boilerplate][boilerplate]
implementation to create a real connection between the raw protocol and the
actual boilerplate library we use to develop materializations.

# Request.Spec

`Request.Spec` is sent by the runtime to the connector, asking the connector
to provide a bunch of information that tells the runtime and the control plane
what inputs this connector expects, and other metadata this connector has
which are useful for the customer.
The UI uses this information to render connector configuration HTML forms.
The connector responds with `Response.Spec`, which is roughly in this format:

```
{
  "protocol": 3032023, # magic number for the current protocol version
  "documentation_url": "go.estuary.dev/materialize-postgres",
  "configSchema": {
    "type": "object",
    "properties": {
      "address": {
        "type": "string"
      },
      "credentials": {
        "type": "object",
        ...
      }
      ...
    }
  },
  "resourceConfigSchema": {
    "type": "object",
    "properties": {
      "table": {
        "type": "string"
      }
    }
  },
  "oauth2": {
    "provider": "google",
    ...
  }
}
```

Note that here, `configSchema` is the JSONSchema describing the main
configuration that is provided to the connector to set up the connection, etc.,
while `resourceConfigSchema` describes the configuration of each _resource_ in
the destination, usually a table, a folder, a messaging queue topic, etc.
The actual JSON values containing the config and resource configurations
are provided as part of subsequent messages exchanged between runtime and
connector (e.g. see `Request.Open.materialization.config`).

This message is handled by the `materialize-boilerplate`'s
`Materializer.RunSpec` function call, while in `materialize-sql` it is
handled separately by `Driver.Spec`.

A fresh instance of a connector is run in a new container to handle
`Request.Spec`.

For actual specifications of various connectors, look for `TestSpec` or
`TestSpecification` files among their snapshots, e.g.:

```
find materialize-*/.snapshots -iname 'TestSpec*'
```

Config schema rendered in the UI:

[UI rendering of endpoint config](spec.png)

Resource schema rendered in the UI:

[UI rendering of resource config](spec-resource.png)

# Request.Validate

After the connector has been configured by knowing what inputs it takes,
and having the user provide those configurations, the control plane and the
runtime validate the new configuration and the configured resources (e.g.
tables, topics) that the user would like to materialize in the destination.
The control plane asks the runtime to start a connector container, send the
relevant validate RPC, and return the results to the control plane.

Here `Request.Validate` contains the JSON `config` (following
`Response.Spec.configSchema`), along with a list of `bindings` (whose field
`resourceConfig` follows `Response.Spec.resourceConfigSchema`).
Each binding is essentially a Flow collection _bound_ to a materialized
resource, so for example, if I want to materialize my flow collection
`my-lovely-collection`, then a binding binds the collection to a table
`lovely_table` in Postgres.

A binding looks like this in YAML:
```
"bindings": [
    {
      "resource": {
        "_meta": {
          "path": [
            "public",
            "lovely_table"
          ]
        },
        "schema": "public",
        "table": "lovely_table"
      },
      "source": {
        "name": "estuary/source-something/my-lovely-collection",
      },
      "fields": {
        "require": {
          "some-optional-field-i-want-to-include": {}
        },
        "recommended": true
      },
      "backfill": 0,
      "onIncompatibleSchemaChange": "backfill"
    },
    ...
]
```

In this step, the connector should simply validate whether it can
successfully connect to and write data to the destination, without actually
writing anything or making any modifications.
The operation should be read-only on the destination.

The connector should also validate that each of the bindings can be
materialized given their current schema, which is included as
`Request.Validate.bindings.collection.readSchema`.
The connector response is a map of field to
`Response.Validated.Constraint` which tells the runtime whether each field
is required, optional, forbidden, or incompatible.
The user can then decide whether they want to include optional fields
(which are omitted by default).
Forbidden fields are those the connector cannot materialize.
A common example is a field that has only ever had `null` values, so its
known type is `type: null`; most destinations require a non-null type, and
so such fields cannot be materialized (e.g. PostgreSQL).
Incompatible fields are those whose type has changed in a way that the
connector cannot automatically migrate / mitigate, meaning that a backfill is
necessary to materialize the binding.

This is handled by `materialize-boilerplate`'s `Constrainter` and
`Validator` interfaces, and `materialize-sql` implements `Constrainter` and
`Validator`s on behalf of SQL connectors.

A fresh instance of a connector is run in a new container to handle
`Request.Validate`.

# Request.Apply

After the configuration and bindings have been validated, the runtime then
asks the connector to `Request.Apply` the configurations to the destination.
Apply essentially means _set up or alter_ the destination environment.
In most cases the materialization connector creates the necessary
destination resources (e.g. tables, folders, message queues).
When a collection schema has been updated, it means the connector updates the
destination resources to match (e.g. add or drop existing table columns, change
column data types, etc.).

As part of `Request.Apply`, the connector is also given the connector state,
and as part of `Response.Applied`, the connector can respond with a
connector state update.
This is useful for "post-commit apply" materializations (more on this below).

A fresh instance of a connector is run in a new container to handle
`Request.Apply`.

# Request.Open

With the destination set up and prepared by `Request.Apply`, the actual
materialization of data can begin.
To begin this process, the runtime starts an instance of the connector in a new
container, and sends it `Request.Open` to provide information about the
bindings and the connector state, as well as the key range to be covered by
this materialization instance.

The key range bounded by `Request.Open.range.key_begin` and
`Request.Open.range.key_end` notifies the connector of its assigned
collection key subset (possibly including all keys).
This allows the runtime to distribute work among multiple instances of the
connector.
For example, two `materialize-postgres` instances may be spawned with key
ranges `[00000000, 80000000)` and `[80000000, FFFFFFFF]`.
Connectors _do not filter_ the incoming documents, the runtime distributes them
correctly.
However, it is important for the connector to know which key range it is
processing to avoid stepping on another instance's foot.

An example is this:

A connector creates staging tables where it loads data before committing it to
the destination table.
These staging tables are named `flow_stage_<target_table_name>`.
With this naming pattern, if two instances are spawned, they will both try to
create and write to the same staging tables.
Instead, each connector should name its staging tables as
`flow_stage_<keyBegin>_<target_table_name>`.

The connector stays running after handling `Request.Open`.
The runtime sends the following messages in a loop:

## Sequence Diagram

Below is a sequence diagram depicting the various messages exchanged between a
materialization connector and the runtime starting with `Request.Open`.
As a convention and to reduce ambiguity, message request types (sent by the
runtime) are named in the imperative mood (`Load`), while response types (sent
by the connector) are named in the perfect tense (`Loaded`):

See the rendered chart [here][materialization-protocol].

<Mermaid chart={`
  sequenceDiagram
    Runtime->>Connector: Open{MaterializationSpec, connectorCP}
    Note right of Connector: Connect to endpoint.<br/>Optionally fetch last-committed<br/>runtime checkpoint.
    Connector->>Runtime: Opened{runtimeCP}
    Note over Runtime, Connector: One-time initialization ☝️.<br/> 👇 Repeats for each transaction.
    Note left of Runtime: Prior txn commits<br/>to recovery log.
    Note right of Connector: Prior txn commits to DB<br/>(where applicable).
    Runtime->>Connector: Acknowledge
    Note right of Runtime: Acknowledged MAY be sent<br/>before Acknowledge.
    Note right of Connector: MAY perform an idempotent<br/>apply of last txn.
    Note left of Runtime: Runtime does NOT await<br/>Acknowledged before<br/>proceeding to send Load.
    Connector->>Runtime: Acknowledged
    Note left of Runtime: Runtime may now finalize<br/>a pipelined transaction.
    Note over Runtime, Connector: End of Acknowledge phase.
    Runtime->>Connector: Load<A>
    Note left of Runtime: Load keys may<br/> not exist (yet).
    Runtime->>Connector: Load<B>
    Note right of Connector: MAY evaluate Load immediately,<br/>or stage for deferred retrieval.
    Connector->>Runtime: Loaded<A>
    Runtime->>Connector: Load<C>
    Runtime->>Connector: Flush
    Connector->>Runtime: Loaded<C>
    Note right of Connector: Omits Loaded for keys<br/>that don't exist.
    Connector->>Runtime: Flushed
    Note left of Runtime: All existing keys<br/>have been retrieved.
    Note over Runtime, Connector: End of Load phase.
    Runtime->>Connector: Store<X>
    Runtime->>Connector: Store<Y>
    Runtime->>Connector: Store<Z>
    Runtime->>Connector: StartCommit{runtimeCP}
    Note right of Connector: * Completes all Store processing.<br/>* MAY include runtimeCP in DB txn.
    Note right of Connector: Commit to DB<br/>now underway.
    Connector->>Runtime: StartedCommit{connectorCP}
    Note left of Runtime: Begins commit to<br/> recovery log.
    Note over Runtime, Connector: End of Store phase. Loops around<br/>to Acknowledge <=> Acknowledged.
`}/>


The full cycle is:
`Request.Open` -> `Request.Acknowledge` -> `Request.Flush` ->
`Request.Store` -> `Request.StartCommit` -> (loop back)
`Request.Acknowledge`


## Request.Load

Flow creates new documents and applies [reductions][reduction-strategies] to
existing documents.
It sends `Request.Load` which points to a binding index and the document key.
The connector looks up the document in the destination, then returns the full
in `Response.Loaded`.
The runtime later uses this information for later `Request.Store` message,
explained below.
The runtime sends one `Request.Load` message per document key, and the
connector sends one `Response.Loaded` message per document key.
If the document key does not exist in the destination, then the connector does
not send any response for that key.

## Request.Flush

The runtime sends `Request.Flush` after it has sent all `Request.Load` messages.
After the connector receives `Request.Flush` *and* has sent all `Request.Loaded`
messages, it sends `Request.Flushed` with the connector state.
The use case for this message is quite niche, but explained below with a
hypothetical use case:

Consider a destination like Elasticsearch, via `materialize-elastic`.
Elastic is a document DB suited for point lookup and point update.
It doesn’t provide a meaningful bulk query API for reads or writes (its "batch"
operations don't scale to Flow's requirements), so the strategy is to stream
out `Load`'d documents (`Response.Loaded` messages), and stream in `Store`'d
documents (`Request.Store` messages).
We throw our hands up and say "this connector is at-least-once" (in an
at-least-once connector, a transaction may fail part-way through and be
restarted, causing its effects to be partially or fully replayed/re-applied).

However, suppose we add an external data container, like an S3 bucket.
Then the connector could:

- **Current**: Evaluate Load keys and stream out point lookups of Loaded
  documents
- **New**: Append those Loaded documents, as they happen, to short-lived log
  segments in the external data container
- **New**: Send `Response.Flushed.state` updates which name those log segment
  file(s)
  - This connector state update is committed to the Recovery Log before the first
    `Request.Store` document is seen
- **Current**: Evaluate Store documents and stream out point upserts

Suppose we fail partway through the Store phase.
The Elastic destination will contain a mix of old and updated document keys.
This mix is what makes it at-least-once today: a subsequent transaction would
update some keys a second time, as it was already updated in the first attempt,
and we would lose track of it.

However, now we have a new option:

- **New**: On restart, the connector sees that log segments from a prior
  `Response.Flushed` are still in its connector state.
- **New**: Knowing that the first transaction will be idempotent, it reads
  those log segments, instead of Elastic, in `Response.Loaded` messages.
  - The runtime correctly reduces with pre-modification document states, so the
    result is idempotent.
- **Current**: The connector evaluates upserts from the Store iterator
  - … but this time, they’re idempotent upserts — the connector only needs
    to handle the unexpected existence of a doc marked as "insert".

**Upsides**:
This makes `materialize-elastic` exactly-once on a per-key basis (though not
atomic — we can't fix that).
The structure of documents in the connector is unchanged with respect to today.

**Downsides**:
It requires a new, external, data container.
Costs increase due to frequent S3 file writes / deletes.
More configuration, more complexity.

**Alternative model**: Client-facing Multi-Version Concurrency Control (MVCC)

Instead of using an external data container, a connector could retain multiple
versions of a document under its given key, distinguished by a version
(e.g. a monotonic transaction counter T).

- Load reads the document version with the largest _committed_ T.
- Store creates a new version, the T of the transaction being committed.
  - Prune older versions, keeping the current version and one older version.

At query time, the client needs to know a committed-through T.
It then scans the destination and takes the largest version of each key <= T.

**Upsides**:
No external data container is required.
This write pattern is well suited to some column-family architectures (thinking
about `materialize-bigtable` in particular).

**Downsides**:
Much more complexity for the client.
Aside from internal usage, only very sophisticated users would successfully use
this.

## Request.Store + Request.StartCommit + Request.Acknowledge

These three messages are highly related, and depending on the various patterns
for writing a materialization connector, they will be used very differently.
Here we explain some of the various patterns, use cases, scale-out strategies,
limitations, and nuances for each.

The general constraint that largely dictates the patterns must be explained:
transactionality.
Flow sends documents to materializations, batched in transactions.
The documents in a transaction must all be committed, or all not be committed.

Flow transactions can be thought of like this:
1. Flow reads the [Recovery Log][recovery-log] for a materialization task
   to know which documents are pending to be materialized (where did the
   materialization leave off last time? continue from there).
2. A transaction starts with a set of documents sent to the connector.
3. These documents must then be _committed_ to the destination, meaning
   they are all persisted in the destination.
4. The Flow Recovery Log persists a checkpoint, marking the end of the
   transaction, asserting that all the documents sent in this transaction
   were committed.
   This checkpoint includes the connector state (a JSON object emitted by
   the connector for the connector's bookkeeping)

The Recovery Log is a durable commit log where Flow keeps track of transactions:
which documents have been materialized.

Ideally, each Flow transaction maps to exactly one transaction in the
destination system; this is sometimes possible, but often it is not.
This is where patterns come into play that fit various systems and their
constraints.

Zombie instances are a critical constraint.
It is possible for a duplicate _zombie_ instance of a connector to be running
alongside a new instance of the connector.
At every point during a transaction, we must guard against a zombie
trying to race our instance, writing and committing a transaction with the same
checkpoint, but not having the same set of documents.
Consider the scenario below:

An instance of `materialize-example` is running, we call it `A`.
It writes documents to ExampleDB.

During the task assignment process, for some reason, we end up with a second
instance of `materialize-example` `B` before `A` can be shut down.

Now both `A` and `B` are processing transactions to table T, but their
transactions are not identical.
Since the transaction being processed by `A` has not yet committed, the same
documents are passed to `B` as well.

In this case, `A` and `B` are both racing to process and commit transactions
which have overlap, and moreover the ordering of operations may be incorrect:
it is possible that `B` is given a smaller transaction, and commits before `A`,
even though the documents received by `B` are potentially newer versions of
documents already passed to `A`.

So for example, this document is passed to `A` before `B` is spawned:

```
{"id": 1, "counter": 1}
```

By the time `B` is spawned, the document has already been updated in the
collection, and it is now:

```
{"id": 1, "counter": 2}
```

Now, `B` is given a smaller transaction than `A`, and commits before `A`, so
the table in ExampleDB now has:

```
{"id": 1, "counter": 2}
```

After this, `A` finally finishes processing its transaction and commits, but it
commits `counter: 1`, which is wrong.

It is also possible that `A` commits before `B`, but `B` is committing the same
documents that have already been committed, and while in some scenarios this
may be okay, there are scenarios using [reduction strategies][reduction-strategies]
such as `sum` which require exactly-once delivery, otherwise they end up with
wrong values.

So connectors must be able to "fence off" other instances from racing with them
to commit to the same table.
This is yet another consideration that must be made when deciding which pattern
to use.
The Recovery Log can sometimes act as the fence itself, but that brings its own
nuances, explained below.

### Destination is Authoritative

One option for establishing fenced-off transactionality is to let the
destination be authoritative about the checkpoints: i.e. instead of using the
Flow Recovery Log, the destination itself does transaction bookkeeping.
This requires that the destination support full ACID transactions which
can apply across resources (e.g. one transaction for updates to multiple
tables in Postgres) throughout the entire process of materializing
documents, including the staging of documents for commit.

This pattern is hard to come by in the real world.
One example of a connector that has this pattern is `materialize-postgres`,
where we can open a transaction at the beginning of `Request.Store`, then add
all the documents to batches which are kept staged by Postgres, then as part of
`Request.StartCommit` the connector also stages an update to a "fence" table,
which holds record of the runtime checkpoint as well as a _nonce_ value (an
integer counter for example), where it upserts a checkpoint value given by the
runtime (it's a string that the connector needs to return back to the runtime
on the next startup as `Response.Opened.runtime_checkpoint` to let the runtime
know where it would like its next transaction to begin).

The nonce value ensures that a specific instance is committing to the fence it
wrote, instead of writing to a fence that has been written to / updated by
another instance.
This way, each instance increments the nonce value when it starts up, and this
new value becomes its fence.
See the queries below:

At startup:
```
update {{ Identifier $.TablePath }}
  set   fence = fence + 1
  where materialization = {{ Literal $.Materialization.String }}
  and   key_end >= {{ $.KeyBegin }}
  and   key_begin <= {{ $.KeyEnd }}
returning *
```

In the `Request.StartCommit` handler:
```
DO $$
BEGIN
	UPDATE {{ Identifier $.TablePath }}
		SET   checkpoint = {{ Literal (Base64Std $.Checkpoint) }}
		WHERE materialization = {{ Literal $.Materialization.String }}
		AND   key_begin = {{ $.KeyBegin }}
		AND   key_end   = {{ $.KeyEnd }}
		AND   fence     = {{ $.Fence }};

	IF NOT FOUND THEN
		RAISE 'This instance was fenced off by another';
	END IF;
END $$;
{{ end }}
```

Note that this query requires that the fence be equal to the value that the
connector has itself inserted at its startup.
If another instance has updated the nonce since then, or if a zombie process
tries to commit, the query above will fail to find the fence with the expected
value; the nonce has been updated by the new instance.

With this pattern, the `Request.Acknowledge` handler is implemented as a no-op.

The Recovery Log itself has a fencing mechanism, however this fencing mechanism
is not available to Destination Authoritative connectors (connectors which
commit to destination in the `Request.StartCommit` handler).
This is because the `Request.StartCommit` handler must do one of:

- The connector first commits to the destination, then the runtime updates the
  Recovery Log.
- The runtime first updates the Recovery Log, then the connector commits to the
  destination.

In the case of Destination Authoritative (committing data to the destination in
the `Request.StartCommit` handler), we must be able to finish committing to the
destination before letting runtime update the Recovery Log.
This means the data is already committed to the destination before the Recovery
Log can validate fencing.
So the zombie process commits data to the destination (e.g. writes to a table),
then requests an update to the Recovery Log, which will fail because of fencing,
but it is too late... data has already landed.

### Recovery Log with Idempotent Apply

Many materialization destinations cannot durably stage transactions, do not
support large transactions, do not support cross-resource transactions, or have
other limitations in their ACID guarantees such that we cannot reliably map
Flow transactions 1:1 with a fenced-off destination transaction.
In these scenarios, instead of having the destination act as the authority in
checkpoints and bookkeeping, the Flow Recovery Log is our authoritative
bookkeeper.
This, however, introduces its own nuances.

In order for the Recovery Log fencing to apply, we will need to first commit to
the Recovery Log, so that fencing is checked as part of this Recovery Log
commit, and only after that we can safely commit data to the destination
resources (e.g. tables).
If a zombie instance and a new instance both try to commit, the runtime will
reject the zombie process' commit because the Recovery Log is now fenced by the
new instance.
(This happens at the Gazette recovery log level.)
When in the `Request.StartCommit` handler we return the `ConnectorState` update
to the runtime in Recovery Log with Idempotent Apply connectors (e.g.
`materialize-databricks`), the runtime checks the fence to make sure the new
state originated from the latest valid instance, and not a zombie.
Once that check is complete, the Recovery Log is updated, and the connector is
responsible for committing data to the destination.
This final commit is executed by the `Request.Acknowledge` handler, and must be
idempotently retriable.

This means that the Recovery Log commits a transaction and assumes the
transaction is _done_ before data is actually committed to the destination.
The `Request.Acknowledge` step ensures data has been committed to the
destination before the next transaction starts.
However, since this is run after the Recovery Log commit, and because it is
possible that the destination commit fails, the `Request.Acknowledge` handler
implementation must be idempotent.
If a destination commit fails for any reason, the runtime must be able to retry
it as many times as necessary (by invoking `Request.Acknowledge`) until the
last transaction has been entirely committed.

In this pattern the `Request.Store` handler should durably stage the
transaction, without changing the state of the bound destination resource
(e.g. a table).
This can be a staging directory, a staging table, or any durable data container
the destination system can use for staging.

The `Request.StartCommit` handler emits a connector state update with pointers
to the staged data.
Then the runtime commits this connector state update, along with the transaction
checkpoint, to the Recovery Log.

Finally, the connector receives the connector state via the
`Request.Acknowledge` handler, which it can use to apply the staged changes to
the destination.
This step must be idempotent, so that it can be retried as many times as
necessary until the changes are successfully committed to the destination.

### Recovery Log with Non-Transactional Destination

In this pattern, the runtime's Recovery Log persists the Flow checkpoint and
handles fencing semantics.
During the Load and Store phases, the connector directly manipulates a
non-transactional destination container or API, such as a key/value database.

Note that this pattern is at-least-once.
A transaction that fails part-way through is restarted, causing its effects to
be partially or fully replayed.

Care must be taken if the collection's schema has reduction annotations such as
sum, as those reductions may be applied more than once due to a partially
completed, but ultimately failed transaction.

If the collection's schema is last-write-wins, this mode effectively provides
exactly-once delivery.
Collections which aren't last-write-wins can be turned into last-write-wins
through the use of derivations.

### Push-only Endpoints & Delta Updates

Some systems, such as APIs, Webhooks, and messages brokers, are push-only in
nature.
Flow's materializations can run in a "delta updates" mode, where loads are
skipped, and Flow does not emit fully reduced documents.
Instead, the runtime sends delta updates during the Store phase, which reflect
the combined roll-up of collection documents processed only within this
transaction.

To illustrate the meaning of a delta update, consider documents which are
simple counters, having a collection schema that uses a `sum` reduction
strategy.

Without delta updates, Flow would reduce documents `-1`, `3`, and `2` by `sum`
to arrive at document `4`, which is delivered to the destination.
In the next transaction, document `4` is loaded and reduced with `6`, `-7`, and
`-1` to arrive at a new document `2`, delivered to the destination.
`2` represents the full reduction of the documents materialized thus far.

Compare this to delta updates mode: Flow reduces documents `-1`, `3`, and `2`
by `sum` to arrive at document `4`, which is delivered to the destination.
The next transaction combines with `6`, `-7`, and `-1` to arrive at a new
document `-2`, delivered to the destination.
These delta updates are a windowed combine over documents seen in the
current transaction only, and unlike before are not a full reduction of the
document.
If delta updates were written to a message queue, note that a subscriber could
further reduce over each delta update to recover fully reduced document `2`.

Note that many reductions require only last-write-wins delivery, and that for
these use cases, delta updates do the "right thing" by trivially replacing
each document with its most recent version.
This matches the behavior of Kafka Connect, for example.

[connectors]: https://docs.estuary.dev/concepts/connectors/
[collections]: https://docs.estuary.dev/concepts/collections/
[protobuf]: https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto
[boilerplate]: ../../materialize-boilerplate
[materialization-protocol]: https://docs.estuary.dev/reference/Connectors/materialization-protocol/
[reduction-strategies]: https://docs.estuary.dev/reference/reduction-strategies/
[recovery-log]: https://gazette.readthedocs.io/en/latest/consumers-concepts.html#recovery-logs
