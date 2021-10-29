# Materialiation Connectors

This document describes Materialization Connectors, how they work and how to create your own. 

## Process Flow

A Materialzation is implemented as a gRPC based server. If you've never worked with gRPC it just basically 
means to create a connector you implement a handful of functions in whatever language you desire. As long as
they fulfill the function contracts and take/provide the correct data, it will interoperate with Flow. 
The gRPC specification for a Materialzation RPC is in the proto definition file here: 
https://github.com/estuary/protocols/blob/main/materialize/materialize.proto
The Service that it must fulfill is the Driver service. 

There's a few common elements used across the RPC functions in the connectors.

### Endpoint Specification
This is the specification for the information required to connect and authenticate to the endpoint. 
It will include things like a hostname, credentials, service accounts, etc. This is the global 
configuration so to speak that will be used for every connection. The connector uses this to tell
Flow what information is required to configure the connector.

### Endpoint Configuration
This is the information provided by Flow to the connector to tell it how to configure itself.

### Resource Specification
This is the specification for the configuration of a data binding. For example for a single database 
you might be storing multiple types of information. Each type of information will have it's own 
resource specification. This might include the table name for which you want to store that 
specific type of information. Generally for a connector you will have one Endpoint Specification 
and one or more Resource Specifications. The connector uses this to tell Flow, per binding, what information
is required to configure that binding.

### Resource Configuration
This is the information provided by Flow, per binding, to tell it how to configure the connector to
store that binding.

### Binding
A binding is a data storage and format configuration. It generally describes where the data comes 
from (the Collection) and the format in which it is expected to be stored. The field names and types
are provided as well as any constraints on the fields. 

### Delta Updates
The Delta Updates flag tells the Flow Runtime if the connector only supports delta changes. In this mode
the Flow Runtime will not attempt to load any data from the connector and instead just provides the most
recent version of the document. Essentially the connector works as an append only journal when it runs
in Delta Updates mode.

### Sharding - KeyBegin/KeyEnd
In order to facilitate scaling of the Flow ecosystem, document processing is broken into Shards. Chunks of
documents might be broken into smaller groups for speed of processing, volume of data or both. This is done
with some sort of sharding function. Essentially if you can take your documents and break them into 
groups such that all related documents end up in the same group, Flow is able to horizontally scale the
processing of work. This is done with the Key fields. You might choose to use a date field, IP address, region
or combinations of all of these for sharding the documents. A document is assigned a Key with a Sharding 
function and then Flow will use this key value to route documents to ensure documents with the same
key are all handled by the same pipeline. 

Processing is done using Key ranges where a particular range of keys are associated with an end to end
pipeline for processing. KeyBegin and KeyEnd determine the range of Keys Handled by a particular pipeline.

### Checkpoints
To ensure that all data is fully processed, the Flow pipeline uses checkpoints. 

There are two checkpoint types, Driver Checkpoints and Flow Checkpoints. 

Flow Checkpoints are used to delineate pieces of work. Operations are completed using these delineations
such that if there is a failure, it will start processing again at the last completed checkpoint of data. 
Flow indicates to a connector that it is starting a particular checkpoint and once the connector confirms the
checkpoint is complete/committed, Flow will register and consider that a completed piece of work.

Driver Checkpoints are optional values that the connector/driver can register with Flow for tracking state
information. Flow can provide this information back to the connector if there is a failure once it is restarted.
Read below for more information about the Driver Checkpoint values.

## RPCs
The connector lifecycle is broken down into 3 RPCs for checks, verifications and setup and finally
a Streaming RPC for transmitting and receiving data. 

The RPCs are as follows (in the order they are called by Flow):

### Spec
The Spec RPC is called by the Flow Runtime to return the specification definition of the connector. 
This information is essentially what the driver requires in order to configure itself. 

The Request includes an optional/partial Endpoint Configuration (as provided by the user)
to pre-configure the connector if needed. This may not be a full configuration. It could just be bootstrap
information to run the connector.

The response provided by the connector should contain:
- The Endpoint Specification Schema - Describing the schema required to fully configure the connector.
- The Resource Specification Schema - Describing the schema required for each data binding.
- Documentation URL - Link to documentation for the connector.

### Validate
The Validate RPC is responsible for validating the configuration provided by Flow. It does things like
checking that you can connect to whatever you are materializing data into as well as the data bindings 
are all valid and work for the database type. It also should check if there is any existing data and
if anything has changed as well as if  the configuration is valid in light of any existing data or structure. 
For example, if the bindings don't match previous binding that were used and the database cannot update
the field schema, the validate request would fail.

The request contains:
- Endpoint Specification - Connection information, credentials and config
- Bindings - Each configured "record/document type" to be stored. It will include for each:
  - Resource Specification - Configuration information such as table name
  - Collection - Definition of the source of the data
  - Fields - Field names and types

The response contains:
- Data Bindings - Validated data binding information with:
  - Field Constraints - Any requirements or validation errors for fields
  - Resource Path - Describing the path of the data materialization (such as table name)
  - Delta Updates - T/F - Will the connector be accepting delta updates only

For example, if you include a new field in a binding and the connector cannot update the database
schema, the field constraint for the new field in the relevant binding might indicate there's an error
with that field. 

### Apply
The Apply RPC is the setup request and last step before actually handling data. During this request the
connector should configure the materialization for storing the data. This might include setting up
database tables and creating indexes. 

The request contains:
- Materialization - Contains all the information for the connector
  - Materialization Name - Identifier describing this materialziation
  - Endpoint Type - Identifier describing this Materialization type.
  - Endpoint Specification - Connection information, credentials and config
  - Bindings - Each configured "record/document type" to be stored. It will include for each:
    - Resource Specification - Configuration information such as table name
    - Collection - Definition of the source of the data
    - Fields - Field names and types
    - Delta Updates - If this connector is operating in Delta Updates mode
- Version - A version of the materialization being applied.
- Dry Run - A flag if the connector should perform the apply or just return what it would do.

The response contains:
- Action Description - What is or would have been done during the apply request.

### Transactions
After the Spec, Validate and Apply RPCs have all been called, the configuration has been 
checked and confirmed and any setup has been applied to the database. The connector is now
ready to process data. This is done via a bi-directional Streaming RPC called Transactions. 

## Transactions RPC
The transactions RPC essentially follows a well defined loop of loading, preparing, storing and 
committing data into the connector materialization. This is a bidirectional stream of messages
that must follow the strict protocol outlined below.

### Open -> Opened
Flow will initiate the stream by opening the Transactions RPC and start the 
process by sending an `open` message to initiate the process.

The `Open` message contains:
- Materialization - This is the same Materialization as provided in the Apply Request.
- Version - This is the same version as provided in the Apply Request.
- KeyBegin/KeyEnd - Range of Key values this materialization will handle. (See Above)
- Driver Checkpoint - Last provided driver checkpoint (if any) that was provided by the connector. (See Above)

The connector must reply with the `Opened` response message that contains:
- Driver Checkpoint - The last processed checkpoint that this materialization has processed.
    This checkpoint value instructs Flow where in history to start sending data.

### Load
Flow will start to stream zero or more Load messages to the materialization. This instructs the materialization
to load records from the underlying data store and return them to Flow.

The load request contains:
- Binding Number - This is the binding number from the list of bindings. (zero indexed)
- Arena - This is a contiguous array of bytes that contains packed keys.
- Packed Keys - This is an array of start/end OFFSETS inside the Arena byte array for the packed tuples of key values. 

The Packed Keys are tuples of values which make up the primary keys for the materialization store. It is expected
that the connector will load the documents referenced by these primary keys and send them to Flow. See the next section
for information about the Tuple format. 

#### FoundatioDB Tuples
Data (both keys and values) are stored in Flow messages in the FoundationDB Tuple format. This format allows
efficiently packing multiple values into an array of bytes. You can read more about the format here:
https://apple.github.io/foundationdb/data-modeling.html 

For example, in the load request above, the Arena is the array of bytes and the Packed Keys are the stard/end
offsets of the tuples of values in that Arena. For each offset you get the subset of bytes within the arena and then 
use the FoundationDB tuple library to unpack the values within the Tuple. 

### Loaded
At this point it is up to the connector if it wishes to load a record at a time or pool the load requests
into one large request to the database to load all of the records at once. (Which is usually far more efficient
depending on the underlying datastore) After Flow sends zero or more `Load` messages with keys for records to load, 
it will finally send a `Prepare` message indicating that the Load process is complete. A connector can use the `Prepare` 
message as a trigger that no more `Load` messages are coming and to actually go fetch all the documents and send them 
to Flow in a `Loaded` message(s).

Each `Loaded` message contains:
- Binding Number - This is the binding number which basically tells Flow which type/table of data it is.
- Arena - Contiguous Byte Slice containing all of the documents in JSON format
- Docs JSON - Start/Stop offsets within the Arena of where each document JSON exists.

### Prepare -> Prepared
The Prepare message from Flow to the connector signals that it is done sending `Load` messages and that it is ready to start
a checkpoint. 

The request contains:
- Flow Checkpoint JSON - This is the checkpoint information that Flow is about to register.

At this point in time, Flow is waiting for the connector to send all the `Loaded` messages (zero or more) that it possibly can. 
The connector can wait until it receives the `Prepare` message until it sends and `Loaded` messages or stream them as it 
gets them. It's up to the connector implementer to decide. (It's probably more efficient to wait and send all at once)

Once the connector has finished sending all the `Loaded` message back to Flow, it will finally send the `Prepared` message.
This signals that the connector is ready to receive data to Store. 

The response contains:
- Driver Checkpoint JSON - Optional driver provided checkpoint information to be persisted in Flow. (See Above) 

### Store
The Store message from Flow to the connector contains the documents that need to be persisted. Flow will send zero or more of
these message.

The request contains:
- Binding Number - This is the binding number which basically tells Flow which type/table of data it is.
- Arena - Contiguous Byte Slice containing all of the documents in JSON format
- Package Keys - This is an array of start/end OFFSETS inside the Arena byte array for the packed tuples of key values. 
- Packed Values - This is an array of start/end OFFSETS inside the Arena byte array for the packed tuples of non-key values. 
- JSON Documents - This is an array of start/end OFFSETS inside the Arena byte array for the Flow Document JSON values.
- Exists - This is an array of boolean values corresponding to the index of values in the Packed Key/Values/Documents saying
if the document already exists in the datastore.

For each `Store` message it's the job of the connector to insert or update the provided values into the store. The keys 
and values are packed tuples in the FoundationDB format. (See above) The Flow Document is the full JSON value of 
the document that is usually persisted in the table with the keys/values. The key/field values are in the same order 
as in the bindings. Based on the boolean exists value, you know if the document needs to be inserted or updated if 
this helps you optimize for performance.

### Commit -> Committed
Once Flow has sent all the `Store` messages it would like to persist, it sends the `Commit` message to indicate 
to the connector it should complete storing all the data and begin committing the data. 

After all the data is committed to the connector datastore, the connector should respond with the `Committed` message. 

The `Commit` and `Committed` messages don't have any information inside of them.

### Loading Again
The next step in the process is to loop around to the `Loading` stage again and repeat for the next checkpoint of data.

NOTE: As soon as Flow sends the `Commit` message it will not wait for the `Committed` message before it starts sending `Load`
messages from the next cycle. It does however provide the guarantee that the next section of documents will not have
the same keys as any document currently being committed.

That it! From here on out, this loop continues indefinitely Loading, Preparing, Storing and Committing. 


## Fencing
Some of the connectors implement a feature called Fencing. This is a connector managed "fence" around a unit of work.
This ensures in a distributed system that two connector instances aren't attempting to update overlapping pieces of 
data. This is typically done in ACID compliant databases that support transactions. 

### Setup
Flow creates a checkpoints table with the following fields
  - Fence - integer
  - Materialization - The name of the materialization
  - KeyBegin - Key range start value
  - KeyEnd - Key range end value
  - Checkpoint JSON - 

### Implementation
During an Open Request the connector will increment the fence value by 1 for any checkpoint table value where:
  - It has the same Materialization Name
  - It has overlapping KeyBegin/KeyEnd ranges (if any part of the range overlaps)

It then checks to see if there is any matching rows in the checkpoint table where:
  - It has the same Materialization Name
  - It has exactly the same KeyBegin/KeyEnd values
If there IS such a value, it takes note of the fence integer value
If there IS NO value, it inserts a new record with the Materialization name, KeyBegin/KeyEnd values, the "no checkpoint"
checkpoint value and a fence with value 1.

The connector maintains a reference to the values that are stored in this step throughout the lifetime of the connector.
This fence is used to mark in the database that this collector owns that materialization + key range of data. The Fence
number is basically the reference this connector maintains as it's serial number for the range it owns. Any change to these
values indicates another connector has taken ownership of this key range and the connector should stop. 

### Fencing of Materialization
The way in which the fence is leveraged is that during a commit operation, as part of the same transaction, 
the connector checks that the fence value still exists with the same name, KeyBegin/KeyEnd and fence values. 
If any of these values has changed, the connector should fail the transaction and exit as another superseding
connector has taken over this range of values. 

