# Materialiation Connectors

This document describes Flow materialization connectors, how they work, and how to create your own. 

## Process Flow

A [materialization](https://docs.estuary.dev/reference/catalog-reference/materialization) is implemented as a gRPC-based server. 
If you've never worked with gRPC, this basically means to create a connector you implement a handful of functions in whatever language you desire. As long as
they fulfill the function contracts and take or provide the correct data, they'll interoperate with Flow. 
The gRPC specification for a Materialzation RPC is in the [proto definition file](https://github.com/estuary/protocols/blob/main/materialize/materialize.proto)
The service that it must fulfill is the **driver** service. 

There are few common elements used across the RPC functions in the connectors.

### Endpoint specification
This specification defines the information required to connect to and authenticate the endpoint. 
It will include things like a hostname, credentials, and service accounts. You can think of this the global 
configuration that will be used for every connection. The connector uses this to tell
Flow what information is required to configure the connector.

### Endpoint configuration
This is the information Flow provides the connector to tell it how to configure itself.

### Resource specification
This is the specification for the configuration of a data [binding](#binding). For example, for a single database 
you might be storing multiple types of information. Each type of information will have its own 
resource specification. This might include the table name for which you want to store that 
specific type of information. Generally, a given connector will have one endpoint specification 
and one or more resource specifications. The connector uses this to tell Flow, per binding, what information
is required to configure that binding.

### Resource configuration
This is the information provided by Flow, per binding, to tell it how to configure the connector to
store that binding.

### Binding
A binding is a data storage and format configuration. It generally describes where the data comes 
from (the [**collection**](https://docs.estuary.dev/reference/catalog-reference/collections)) and the format in which it is expected to be stored. The field names and types
are provided as well as any constraints on the fields. 

### Delta updates
The delta updates flag tells the Flow runtime if the connector only supports delta changes. In this mode,
the Flow runtime will not attempt to load any data from the connector and instead only provides the most
recent version of the document. Essentially, the connector works as an append-only [journal](https://docs.estuary.dev/architecture/concepts-1#how-brokers-connect-collections-to-the-runtime) when it runs
in delta updates mode.

### Sharding: KeyBegin and KeyEnd
In order to facilitate scaling of the Flow ecosystem, document processing is broken into [shards](https://docs.estuary.dev/architecture/scaling#processing-with-shards). Chunks of
documents might be broken into smaller groups for speed of processing, volume of data or both. This is done
with some sort of sharding function. Essentially, if you can take your documents and break them into 
groups such that all related documents end up in the same group, Flow is able to horizontally scale the
processing of work. This is done with the key fields. You might choose to use a date, IP address, region
or combinations of these to shard the documents. A document is assigned a key with a sharding 
function, and then Flow uses this key value to route documents to ensure documents with the same
key are all handled by the same pipeline. 

Processing is done using key ranges, where a particular range of keys are associated with an end to end
pipeline for processing. KeyBegin and KeyEnd determine the range of keys handled by a particular pipeline.

### Checkpoints
To ensure that all data is fully processed, the Flow pipeline uses checkpoints. 

There are two checkpoint types: driver checkpoints and Flow checkpoints. 

**Flow checkpoints** are used to delineate pieces of work. 
Flow indicates to a connector when it is starting a particular checkpoint. Once the connector confirms the
checkpoint is complete or committed, Flow registers this and considers it a completed piece of work. 
Operations are completed using these delineations
such that if there is a failure, it will start processing again at the last completed checkpoint of data. 

**Driver Checkpoints** are optional values that the connector (or driver) can register with Flow for tracking state
information. Flow can provide this information back to the connector if there is a failure once it is restarted.
Read below for more information about the driver checkpoint values.

## RPCs
The connector lifecycle is broken down into several RPCs that provide the specification, check and verify, set up, and finally
transmit and receive data. 

These RPCs are outlined below in the order they are called by Flow:

### Spec
The **Spec** RPC is called by the Flow runtime to return the specification definition of the connector. 
This information is essentially what the driver requires in order to configure itself. 

The request includes an optional, partial endpoint configuration 
to allow the user to pre-configure the connector if needed. This might not be a full configuration. It could just be bootstrap
information to run the connector.

The response provided by the connector should contain:
- **Endpoint specification schema**: Describing the schema required to fully configure the connector.
- **Resource specification schema**: Describing the schema required for each data binding.
- **Documentation URL**: Link to documentation for the connector.

### Validate
The **Validate** RPC is responsible for validating the configuration provided by Flow. These checks typically include:
- That you can connect to the location into which you are materializing
- That the data bindings are all valid and work for the database type
- Whether there is any existing data at the endpoint
- What changes have occurred, if any
- That the configuration is valid in light of any existing data or structure

For example, if the bindings don't match previous bindings that were used and the database cannot update
the field schema, the validate request should fail.

The request contains:
- **Endpoint specification**: Connection information, credentials and config
- **Bindings**: Each configured record or document type to be stored, including:
  - **Resource specification**: Configuration information such as table name
  - **Collection**: Definition of the source of the data
  - **Fields**: Field names and types

The response contains:
- **Data bindings** Validated data binding information with:
  - **Field Constraints**: Any requirements or validation errors for fields
  - **Resource path**: Describes the path of the data materialization, such as table name
  - **Delta updates** (T/F): Whether the connector will accept delta updates only

For example, if you include a new field in a binding and the connector cannot update the database
schema, the field constraint for the new field in the relevant binding might indicate an error
with that field. 

### Apply
The **Apply** RPC is the setup request and last step before the connector actually handles data. During this request, the
connector should configure the materialization to store the data. This might include setting up
database tables and creating indexes. 

The request contains:
- **Materialization**: Contains all the information for the connector
  - **Materialization name**: Identifier describing this materialziation
  - **Endpoint type**: Identifier describing this materialization type
  - **Endpoint specification**: Connection information, credentials and config
  - **Bindings**: Each configured record or document type to be stored, including:
    - **Resource specification**: Configuration information such as table name
    - **Collection**: Definition of the source of the data
    - **Fields**: Field names and types
    - **Delta updates**: Whether this connector is operating in delta updates mode
- **Version**: The version of the materialization being applied
- **Dry run**: A flag indicating whether the connector should perform the apply or just return what it would do

The response contains:
- **Action description**: What is or would have been done during the apply request

### Transactions
After the **Spec**, **Validate** and **Apply** RPCs have all been called, the configuration has been 
checked and confirmed and any necessary setup has been applied to the database. The connector is now
ready to process data. This is done via a bi-directional streaming RPC called Transactions. 

## Transactions RPC
The **Transactions** RPC essentially follows a well-defined loop of loading, preparing, storing and 
committing data into the connector materialization. This is a bidirectional stream of messages
that must follow the strict protocol outlined below.

### Open -> Opened
Flow will initiate the stream by opening the Transactions RPC and start the 
process by sending an `Open` message.

The `Open` message contains:
- **Materialization**: This is the same materialization as provided in the Apply request.
- **Version**: This is the same version as provided in the Apply request
- **KeyBegin/KeyEnd**: Range of [key values](#sharding-keybegin-and-keyend) this materialization will handle.
- **Driver checkpoint**: Last provided [driver checkpoint](#checkpoints) (if any) that was provided by the connector.

The connector must reply with the `Opened` response message that contains:
- **Driver Checkpoint**: The last processed checkpoint that this materialization has processed.
    This checkpoint value instructs Flow where in history to start sending data.

### Load
Flow will start to stream zero or more load messages to the materialization. This instructs the materialization
to load records from the underlying data store and return them to Flow.

The load request contains:
- **Binding number**: The binding number from the list of bindings (zero indexed)
- **Arena**: A contiguous array of bytes that contains packed keys
- **Packed keys**: An array of start/end OFFSETS inside the **arena** byte array for the packed tuples of key values. 

The **packed keys** are tuples of values which make up the primary keys for the materialization store. It is expected
that the connector will load the documents referenced by these primary keys and send them to Flow. See the next section
for information about the Tuple format. 

#### FoundationDB tuples
Data (both keys and values) are stored in Flow messages in the FoundationDB Tuple format. This format allows the
efficient packing of multiple values into an array of bytes. You can read more about the format [here](https://apple.github.io/foundationdb/data-modeling.html).

For example, in the load request above, the arena is the array of bytes and the packed keys are the stard/end
offsets of the tuples of values in that arena. For each offset you get the subset of bytes within the arena and then 
use the FoundationDB tuple library to unpack the values within the tuple. 

### Loaded
At this point, it is up to the connector whether to load records one at a time or pool the load requests
into one large request, allowing the endpoint to load all of the records at once. The latter is usually far more efficient, but this
depends on the underlying datastore. After Flow sends zero or more `Load` messages with keys for records to load, 
it sends a `Prepare` message indicating that the load process is complete. A connector can use the `Prepare` 
message as a trigger, since no more `Load` messages are coming, to go fetch all the documents and send them 
to Flow in a `Loaded` message.

Each `Loaded` message contains:
- **Binding number**: The binding number, which basically tells Flow which type or table of data it is
- **Arena**: Contiguous byte slice containing all of the documents in JSON format
- **Docs JSON**: Start and stop offsets within the arena indicating where each document JSON exists

### Prepare -> Prepared
The Prepare message from Flow to the connector signals that it is done sending `Load` messages and that it is ready to start
a checkpoint. 

The request contains:
- **Flow Checkpoint JSON**: The checkpoint information that Flow is about to register

At this point in time, Flow is waiting for the connector to send all the `Loaded` messages (zero or more) that it possibly can. 
The connector can wait until it receives the `Prepare` message before it sends `Loaded` messages or stream them as it 
gets them. It's up to the connector implementer to decide, though it's probably more efficient to wait and send all at once.

Once the connector has finished sending all the `Loaded` message back to Flow, it will finally send the `Prepared` message.
This signals that the connector is ready to receive data to store. 

The response contains:
- **Driver checkpoint JSON**: Optional driver-provided [checkpoint](#checkpoints) information to be persisted in Flow.

### Store
The `Store` message from Flow to the connector contains the documents that need to be persisted. Flow will send zero or more of
these message.

The request contains:
- **Binding number**: The binding number, which basically tells Flow which type or table of data it is
- **Arena**: Contiguous byte slice containing all of the documents in JSON format
- **Package keys**: An array of start and end OFFSETS inside the arena byte array for the packed tuples of key values 
- **Packed values**: An array of start and end OFFSETS inside the arena byte array for the packed tuples of non-key values 
- **JSON documents**: An array of start and end OFFSETS inside the arena byte array for the Flow document JSON values
- **Exists**: An array of boolean values corresponding to the index of values in the packed keys, values, and documents indicating
whether the document already exists in the datastore

For each `Store` message, it's the job of the connector to insert or update the provided values into the store. The keys 
and values are packed tuples in the [FoundationDB](#foundationdb-tuples) format. The Flow document is the full JSON value of 
the document, which is usually persisted in the table with the keys and values. The key and field values are in the same order 
as in the bindings. Based on the boolean exists value, you know whether the document needs to be inserted or updated, which can help you optimize for performance.

### Commit -> Committed
Once Flow has sent all the `Store` messages it would like to persist, it sends the `Commit` message to indicate 
to the connector it should complete storing all the data and begin committing the data. 

After all the data is committed to the endpoint, the connector should respond with the `Committed` message. 

The `Commit` and `Committed` messages don't have any information inside of them.

### Loading again
The next step in the process is to loop around to the `Loading` stage again and repeat for the next checkpoint of data.

NOTE: As soon as Flow sends the `Commit` message it will not wait for the `Committed` message before it starts sending `Load`
messages from the next cycle. It does, however, provide the guarantee that the next section of documents will not have
the same keys as any document currently being committed.

That it! From here on out, this loop continues indefinitely loading, preparing, storing, and committing. 


## Fencing
Some of the connectors implement a feature called **fencing**. This is a connector managed "fence" around a unit of work.
In a distributed system, this ensures that two connector instances aren't attempting to update overlapping pieces of 
data. This is typically done in ACID compliant databases that support transactions. 

### Setup
Flow creates a checkpoints table with the following fields:
  - **Fence**: integer
  - **Materialization**: The name of the materialization
  - **KeyBegin**: Key range start value
  - **KeyEnd**: Key range end value
  - **Checkpoint** JSON 

### Implementation
During an `Open` request, the connector will increment the fence value by 1 for any checkpoint table value where:
  - It has the same materialization name
  - It has overlapping KeyBegin/KeyEnd ranges (if any part of the range overlaps)

It then checks to see if there are any matching rows in the checkpoint table where:
  - It has the same materialization Name
  - It has exactly the same KeyBegin/KeyEnd values
If there IS such a value, it takes note of the fence integer value
If there IS NO value, it inserts a new record with the materialization name, KeyBegin/KeyEnd values, the "no checkpoint"
checkpoint value, and a fence with value 1.

The connector maintains a reference to the values that are stored in this step throughout the lifetime of the connector.
This fence is used to mark in the database that this collector owns that materialization and key range of data. The fence
number is basically the reference this connector maintains as the serial number for the range it owns. Any change to these
values indicates that another connector has taken ownership of this key range and the connector should stop. 

### Fencing of materialization
The way in which the fence is leveraged is that during a commit operation, as part of the same transaction, 
the connector checks that the fence value still exists with the same name, KeyBegin/KeyEnd and fence values. 
If any of these values has changed, the connector should fail the transaction and exit as another superseding
connector has taken over this range of values. 

