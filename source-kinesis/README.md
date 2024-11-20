# source-kinesis

This is a flow capture connector that captures data from AWS Kinesis.

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

### State

The Kinesis connector stores the current offset within each Kinesis Shard in its state. It prunes
old Kinesis Shards only on startup. Kinesis shards can be created and deleted at any time, but the
overall rate of change is relatively slow, as Kinesis limits the number of scaling events that you
can perform each day.

