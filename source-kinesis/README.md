# source-kinesis

This is a capture connector that implements the Airbyte specification and captures data from
AWS Kinesis.

### Configuration

The endpoint configuration accepts the following properties:

- `region`: Required. Name of the AWS region where the Kinesis stream is located (e.g. "us-east-1").
- `endpoint`: Optional endpoint URI for the Kinesis service.
- `awsAccessKeyId`: Required. Credential for accessing Kinesis.
- `awsSecretAccessKey`: Required. Credential for accessing Kinesis.

The bindings configuration only names each Kinesis Stream to be bound to a Flow collection. The
bindings must all reference Kinesis Streams that are in the same AWS region.

### Limitations

This connector currently only supports JSON data. All Records in all Shards of the Stream must be
JSON documents that conform to the target collection's schema. Handling of most other data formats
would require additional work in `flow-parser` to handle framed inputs and outputs.

### Scaling

The Kinesis connector automatically discovers all Kinesis Shards within the named Kinesis Stream and
reads data from all of them. The Flow Capture Shards may also be split, in which case each Flow
shard will read from a (possibly overlapping) subset of the Kinesis shards. It is detrimental to
efficiency to have more Flow shards than there are Kinesis Stream Shards. A good general guideline
is to have a number of Flow shards that is roughly half the number of Kinesis shards.

### State

The Kinesis connector stores the current offset within each Kinesis Shard in its state. It prunes
old Kinesis Shards only on startup. Kinesis shards can be created and deleted at any time, but the
overall rate of change is relatively slow, as Kinesis limits the number of scaling events that you
can perform each day.

