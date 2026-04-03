# Materializations

Materialization connectors write Flow collections into a destination system, e.g. a database, warehouse, file storage, messaging queue, etc.

This document's aim is to explain in as much detail as possible the materialization protocol (i.e. how does Flow runtime interact with a materialization connector), and explain the various constraints and considerations necessary when implementing a connector.

The actual protocol is defined as protobuf messages [here](https://github.com/estuary/flow/blob/master/go/protocols/materialize/materialize.proto). Here we will reference some of those message types (in this doc these message types are denoted with a `Request.` or `Response.` prefix) and will explain their role. This document also references the [materialize-boilerplate](./materialize-boilerplate) implementation to create a real connection between the raw protocol and the actual boilerplate library we use to develop materializations.

# Request.Spec

`Request.Spec` is sent by the runtime to the connector, asking the connector to provide a bunch of information that tells the runtime and the control plane what inputs does this connector expect, and what other metadata this connector has which are useful for the customer. This information is used by the UI to render the forms which ask for configuration inputs for the connector. The connector has to respond with `Response.Spec`, which is roughly in this format:

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

Note that here, `configSchema` is the main configuration that is provided to the connector in order to set up the connection, etc., while `resourceConfigSchema` describes the configuration of each _resource_ in the destination, this is usually a table, a folder, a messaging queue topic, etc.

This message is handled by the `materialize-boilerplate`'s `Materializer.RunSpec` function call, while in `materialize-sql` it is handled separately by `Driver.Spec`.

For actual specifications of various connectors, look for `TestSpec` or `TestSpecification` files among their snapshots, e.g.:

```
find materialize-*/.snapshots -iname 'TestSpec*'
```

Config schema rendered in the UI:

[UI rendering of endpoint config](spec.png)

Resource schema rendered in the UI:

[UI rendering of resource config](spec-resource.png)


# Request.Validate

After the connector has been configured by knowing what inputs it takes, and having the user provide those configurations, the control plane and the runtime validate the new configuration, and the configured resources (e.g. tables, topics) that the user would like to materialize in their destination.

Here `Request.Validate` contains the JSON `config`, along with a list of `bindings`. Each binding is essentially a Flow collection _bound_ to a materialized resource, so for example, if I want to materialize my flow collection `my-lovely-collection`, there is going to be a binding that binds this collection to a table `lovely_table` in postgres.

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

In this step, the connector is supposed only to validate whether it can now successfully connect to and write data to the destination, without actually writing anything or making any modifications. The operation should be read-only on the destination.

The connector should also validate that each of the bindings can be materialized given their current schema, which is included as `Request.Validate.bindings.collection.readSchema`. The connector will respond with a map of field to `Response.Validated.Constraint` which tells the runtime whether each field is required, optional, forbidden or incompatible. The user can then decide whether they want to include optional fields (which are omitted by default). For forbidden fields, they cannot be materialized by this connector. Incompatible fields mean the type of a field has changed in a way that the connector cannot automatically migrate / mitigate, and so a backfill is necessary to be able to materialize this binding.

This is handled by `materialize-boilerplate`'s `Constrainter` and `Validator` interfaces, and `materialize-sql` implements `Constrainter` and `Validator`s on behalf of sql connectors.

# Request.Apply

Once the configuration and the various bindings have been validated, the runtime then asks the connector to `Request.Apply` the configurations to the destination. Apply essentially means _set up_ the environment, in most cases this means something like creating the necessary resources (schemas, tables, topics, folders) in the destination, or in cases where the schema of a collection has been updated, it means updating the resources to match the new schema (e.g. altering existing tables to add a new column or drop a colum or change a column's data type, etc.)

As part of `Request.Apply`, the connector is also given the connector state, and as part of `Response.Applied`, the connector can respond with a connector state update. This is useful for "post-commit apply" materializations (more on this below).

# Request.Open

With the destination set up and prepared by `Request.Apply`, then the actual materialization of data can begin. To begin this process, the runtime starts an instance of the  connector, and sends it an `Request.Open` first, providing information about the bindings and the connector state to the connector, as well as the key range to be covered by this materialization instance.

The key range provided in `Request.Open.range.key_begin` and `Request.Open.range.key_end` let the connector know that it is being provided part (or all) of the key range for collections that it is materializing. The reason for this is that the runtime can distribute work among multiple instances of the connector, so for example two materialize-postgres instances may be spawned with key ranges `[00000000, 80000000)` and `[80000000, FFFFFFFF]`. The connectors do not need to _filter_ the incoming documents, they are already distributed by the runtime correctly, however it is important for the connector to know which key range it is processing, in order to avoid stomping over another instance's foot. An example is this:

A connector creates staging tables where the data is loaded into before being commited to the destination table. These staging tables are named `flow_stage_<target_table_name>`. With this naming pattern, if two instances are spawned, they will both try to create, and write to the same table, resulting in a mixing of data between them, so instead the connector should create its staging tables as `flow_stage_<keyBegin>_<target_table_name>`.

Once the materialization has been "Opened", the connector stays running and it receives the following messages from the runtime in a loop:

## Request.Load

Flow wants to apply [reductions](https://docs.estuary.dev/reference/reduction-strategies/) on top of existing documents, as well as gather information on whether certain keys already exist in the destination or not. In order to achieve this, it sends `Request.Load` messages which point to a binding index as well as the key of the document. The connector then must look for this document in the destination and return the full document as part of `Response.Loaded`. This process gathers information that is later provided back to the connector during `Request.Store`, explained below.

## Request.Flush

Once all load requests are sent, the runtime sends `Request.Flush`, to which the connector responds with `Request.Flushed` with a connector state. The use case for this message is quite niche, but explained below.

Consider a store like materialize-elastic. Elastic is a document DB suited for point lookup and point update. It doesn’t provide a meaningful bulk query API for reads or write (its "batch" operations don’t scale to the degree we’d need), so the basic strategy is to stream out Load’d documents, and stream in Store’d documents. We throw our hands up and say "this connector is at-least-once".

However, suppose you add in external stable storage, like an S3 bucket. Then the connector could:

- **Current**: Evaluate Load keys and stream out point lookups of Loaded documents
- **New**: append those Loaded documents, as they happen, to short-lived log segments in stable storage
- **New**: send Response.Flushed.state updates which name those log segment file(s)
  - This connector state update is stored durably before the first Request.Store document is seen
- **Current**: Evaluate Store documents and stream out point upserts


Suppose we fail partway through the Store phase. Elastic’s documents will contain a mix of old vs updated document keys. This mix is what makes it at-least-once today: a next transaction would update a key a second time, as it was already updated in the first attempt and we lost track of it.

However, now we have a new option:

- **New**: On restart, the connector sees that log segments from a prior Flushed are still in its state.
- **New**: Knowing that the first transaction will be idempotent, it reads those log segments instead of Elastic to return Response.Loaded
  - As these pre-modification document states, the runtime will reduce idempotent outputs
- **Current**: The connector evaluates upserts from the Store iterator
  - … but this time, they’re idempotent upserts — the connector only needs to handle the unexpected existence of a doc marked as “insert”.

**Upsides**: This makes Elastic exactly-once on a per-key basis (though not atomic — we can’t fix that for it). The structure of documents in the connector is unchanged with respect to today.
**Downsides**: It requires new, external, stable storage. More cost due to frequent S3 file writes / deletes. More configuration, more complexity.

**Alternative model**: Client-facing Multi-Version Concurrency Control (MVCC)

Instead of using external stable storage, a connector could store multiple versions of a document under its given key, distinguished by a version (e.x. a monotonic transaction counter T).

- During Load, take the version with largest _committed_ T.
- During Store, create a new version at the T of the transaction being committed.
  - Prune older versions, but minimally retain one older version.

At query time, the client needs to know a committed-through T. It then scans the store and takes the largest version of each key <= T.

**Upsides**: No external storage, and this write pattern is well suited to some column-family storage architectures (thinking about a materialize-bigtable in particular).
**Downsides**: Much more complexity for the client. Aside from internal usage, only very sophisticated users would be successful using this. (edited)

## Request.Store + Request.StartCommit + Request.Acknowledge

These three messages are highly related, and depending on the various patterns for writing a materialization connector, they will be used very differently. So here we explain some of the various patterns, use cases, scale-out strategies, limitations and nuances for each.

Before diving into the patterns, one general constraint that is largely what dictates the patterns must be explained: transactionality. Flow sends documents to materializations as part of a transaction, which must either all be committed, or none at all.

Flow transactions can be thought of like so:
1. Flow reads the recovery log for a materialization task to know which documents are pending to be materialized (where did the materialization leave off last time? continue from there)
2. A transaction starts with a set of documents sent to the connector
3. These documents must then be _committed_ to the destination, meaning all these documents are persisted and updated in the destination
4. The Flow Recovery Log persists a checkpoint, marking the end of the transaction, asserting that all the documents sent in this transaction were committed. This checkpoint includes the connector state (a JSON object emitted by the connector for the connector's bookkeeping)

The recovery log is basically a bookkeeping store where Flow keeps track of transactions: which documents have been materialized.

Ideally we want to map Flow transactions exactly into transactions in the destination system, and that is sometimes possible, but a lot of the times it may not be possible to achieve a 1:1 mapping between these transactions. This is where various patterns come into play that fit various systems and their constraints.

One important constraint that we must consider when implementing transactions in materializations is that it is possible for a duplicate _zombie_ instance of a connector to be running alongside a new instance of the connector. So essentially at any point during a transaction, we must be aware that another zombie instance may be trying to race our instance in writing and committing a transaction starting at the same checkpoint (but not necessarily having the exact same documents, only the starting point of these transactions is the same, but they may have a different group of documents each). Consider the scenario below:

We have an instance of materialize-example running, we call it `A`.

During the task assignment process, for some reason, we end up with a second instance of materialize-example `B`, before `A` can be shut down.

Now both `A` and `B` are trying to processed transactions to table T, but their transactions are not identical. Since the transaction being processed by `A` was still not committed, the same documents may now be passed to `B` as well.

So in this case, `A` and `B` are both racing to process and commit transactions which have overlap, and moreover the ordering of operations may be incorrect: it is possible that `B` is given a smaller transaction, and commits before `A`, even though the documents received by `B` are potentially newer versions of documents already passed to `A`.

So for example, this document is passed to `A` before `B` is spawned:

```
{"id": 1, "counter": 1}
```

By the time `B` is spawned, the document has already been updated in the collection, and it is now:

```
{"id": 1, "counter": 2}
```

Now, `B` is given a smaller transaction than `A`, and commits before `A`, so the table in Clickhouse now has:

```
{"id": 1, "counter": 2}
```

After this, `A` finally finishes processing its transaction and commits, but it is committing `counter: 1`, which is wrong.

It is also possible that `A` commits before `B`, but `B` is committing the same documents that have already been committed, and while in some scenarios this may be okay, there are scenarios such as using [reduction strategies](https://docs.estuary.dev/reference/reduction-strategies/) such as `sum` which require exactly-once delivery, otherwise they end up with wrong values.

So connectors must be able to "fence off" other instances from racing with them to commit to the same table. This is yet another consideration that must be made when deciding which pattern to use. Recovery Log can sometimes act as the fence itself, but that brings its own nuances, explained below.

### Destination is Authoritative

One option for establishing fenced-off transactionality is to let the destination be authoritative about the checkpoints: i.e. instead of using the Flow recovery log, the destination itself does bookkeeping of transactions. This requires that the destination support full ACID transactions which can apply across resources (e.g. one transaction for updates to multiple tables in postgres) throughout the entire process of materializing documents, including the staging of documents for commit.

This pattern is hard to come by in the real world. One example of a connector that has this pattern is materialize-postgres, where we can open a transaction at the beginning of `Request.Store`, then add all the documents to batches which are kept staged by Postgres, then as part of `Request.StartCommit` the connector also stages an update to a "fence" table, which holds record of the runtime checkpoint as well as a _nonce_ value (an integer counter for example), where it upserts a checkpoint value given by the runtime (it's a string that the connector needs to return back to the runtime on the next startup as `Response.Opened.runtime_checkpoint` to let the runtime know where it would like its next transaction to begin from).

The nonce value is what ensures that a specific instance is committing to the fence it wrote, instead of writing to a fence that has been written to / updated by another instance. So each instance, when it starts up, it increments the nonce value, and this new value becomes its fence. See the queries below:

At startup:
```
update {{ Identifier $.TablePath }}
  set   fence = fence + 1
  where materialization = {{ Literal $.Materialization.String }}
  and   key_end >= {{ $.KeyBegin }}
  and   key_begin <= {{ $.KeyEnd }}
returning *
```

In StartCommit:
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

Note that this query requires that the fence be equal to the value that the connector has itself inserted at its startup. If another instance has updated the nonce since, or if another old zombie process tries to commit, because the nonce has been updated by the new instance, the query above will fail by not being able to find the fence with the expected value.

When using this pattern, `Request.Acknowledge` is a no-op implementation which is not necessary.

The Recovery Log itself has fencing mechanism, however this fencing mechanism is not available to Destination Authoritative connectors (connectors which commit the data to destination in StartCommit), this is because in StartCommit, we either need to:

- First commit the data to the destination, then tell runtime that the commit has been done so the recovery log can be updated
- First commit to recovery log by runtime, then commit data to destination

In case of Destinatio Authoritative (committing data to destination in StartCommit), we must be able to finish committing to store before telling runtime to update recovery log -- this means the data is already commited to destination before recovery log can validate fencing. So the zombie process will commit data to destination (write to tables), then ask for update to recovery log, which will fail because of fencing, but it is too late... data has already landed.

### Recovery Log with Idempotent Apply

Many destinations we materialize to do not support transactions that include staging of documents in large quantities, cross-resource transactions, or have general limitations in their ACID guarantees that mean we cannot reliably have a fenced-off transaction mapping 1:1 to a Flow transaction. In these scenarios, instead of having the destination act as the authority in checkpoints and bookkeeping, we let the Flow Recovery Log be our authoritative bookkeeper. This, however, introduces its own nuances.

In order for the Recovery Log fencing to apply, we will need to first commit to the Recovery Log, so that fencing is checked as part of this Recovery Log commit, and only after that we can safely commit data to the destination resources (e.g. tables). If a zombie process and a new assignment try to both commit, the runtime will reject the zombie process' commit because the recovery log is now fenced by the new assignment (this is at the Gazette recovery log level), so when in StartCommit we return the ConnectorState update to runtime in Recovery Log with Idempotent Apply connectors (e.g. materialize-databricks), the runtime will check for the fence to make sure this is coming from the latest valid assignment, and not a zombie one. Once that check is done, recovery log is updated, and then the connector has the responsibility to actually commit data to destination: this is done during Acknowledge, and must be idempotently retriable.

This means that the Recovery Log is going to commit a transaction, and assume it as _done_ before any data has actually landed in the destination resources. The way to ensure that the data has actually fully landed in the destination before a next transaction starts is to use the `Request.Acknowledge` step to land the data. However, since this is run after the commit, and it is possible for failures as part of this committing of data to destination, the `Request.Acknowledge` handling must be idempotent: if the process of landing data fails for any reason, we must be able to retry it as much as necessary until all data has landed, before we can continue with the next transaction.

In this pattern, during `Request.Store` the connector should stage data for landing, but should not land anything in the destination. This is usually in the form of a staging directory, a staging table or some form of data staging supported by the destination system.

Then, during `Request.StartCommit` the connector emits a state update with pointers to the staged data. At this point the runtime is going to commit this state update along with the transaction checkpoint to the Recovery Log.

Finally, in `Request.Acknowledge` the connector state is sent to the connector, which it should use to actually land the data in the destination. This step must be idempotent, so that it can be retried as many times as necessary until the transaction has landed.

### Recovery Log with Non-Transactional Destination

In this pattern, the runtime's recovery log persists the Estuary checkpoint and handles fencing semantics. During the Load and Store phases, the driver directly manipulates a non-transactional store or API, such as a key/value store.

Note that this pattern is at-least-once. A transaction may fail part-way through and be restarted, causing its effects to be partially or fully replayed.

Care must be taken if the collection's schema has reduction annotations such as sum, as those reductions may be applied more than once due to a partially completed, but ultimately failed transaction.

If the collection's schema is last-write-wins, this mode still provides effectively-once behavior. Collections which aren't last-write-wins can be turned into last-write-wins through the use of derivations.

### Push-only Endpoints & Delta Updates

Some systems, such as APIs, Webhooks, and Pub/Sub, are push-only in nature. Estuary's materializations can run in a "delta updates" mode, where loads are always skipped and Estuary does not attempt to store fully-reduced documents. Instead, during the store phase, the runtime sends delta updates which reflect the combined roll-up of collection documents processed only within this transaction.

To illustrate the meaning of a delta update, consider documents which are simple counters, having a collection schema that uses a `sum` reduction strategy.

Without delta updates, Estuary would reduce documents -1, 3, and 2 by `sum` to arrive at document 4, which is stored. The next transaction, document 4 is loaded and reduced with 6, -7, and -1 to arrive at a new stored document 2. This document, 2, represents the full reduction of the collection documents materialized thus far.

Compare to delta updates mode: collection documents -1, 3, and 2 are combined to store a delta-update document of 4. The next transaction starts anew, and 6, -7, and -1 combine to arrive at a delta-update document of -2. These delta updates are a windowed combine over documents seen in the current transaction only, and unlike before are not a full reduction of the document. If delta updates were written to pub/sub, note that a subscriber could further reduce over each delta update to recover the fully reduced document of 2.

Note that many use cases require only `lastWriteWins` reduction behavior, and for these use cases delta updates does the "right thing" by trivially re-writing each document with its most recent version. This matches the behavior of Kafka Connect, for example.
