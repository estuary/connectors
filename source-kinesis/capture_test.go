package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	glueTypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// useRealAWS returns true if AWS credentials are provided via environment variables
func useRealAWS() bool {
	return os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != ""
}

func testConfig(t *testing.T) Config {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	if useRealAWS() {
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
		return Config{
			Region:             region,
			AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			AWSSessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		}
	}

	return Config{
		Region:             "local",
		AWSAccessKeyID:     "x",
		AWSSecretAccessKey: "x",
	}
}

func TestDiscover(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client, err := connect(ctx, &conf)
	require.NoError(t, err)

	testStreams := []string{"test-stream-1", "test-stream-2"}

	cleanup := func() {
		for _, s := range testStreams {
			_, _ = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(s)})
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, s := range testStreams {
		s := s
		_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(s),
			ShardCount: aws.Int32(2),
		})
		require.NoError(t, err, "failed to create stream")
		awaitStreamActive(t, ctx, client, s)
	}

	cs := &st.CaptureSpec{
		Driver:       &driver{},
		EndpointSpec: &conf,
	}

	cs.VerifyDiscover(ctx, t)
}

func TestCapture(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client, err := connect(ctx, &conf)
	require.NoError(t, err)

	testStreams := []string{"test-stream-1", "test-stream-2"}

	cleanup := func() {
		for _, s := range testStreams {
			_, _ = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(s)})
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	for _, s := range testStreams {
		s := s
		_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(s),
			ShardCount: aws.Int32(2),
		})
		require.NoError(t, err, "failed to create stream")
		awaitStreamActive(t, ctx, client, s)
	}

	bindings := []*pf.CaptureSpec_Binding{}
	for _, s := range testStreams {
		resourceJson, err := json.Marshal(resource{
			Stream: s,
		})
		require.NoError(t, err)
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: resourceJson,
			ResourcePath:       []string{s},
			StateKey:           fmt.Sprintf("%s.v0", s),
		})
	}

	capture := st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     bindings,
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers: map[string]*regexp.Regexp{
			"<SEQUENCE_NUM>": regexp.MustCompile(`\d{56}`),
		},
	}

	addData := func(t *testing.T, start, end int) {
		for i := start; i < end; i++ {
			var input = kinesis.PutRecordInput{
				StreamName:   aws.String(testStreams[i%len(testStreams)]),
				Data:         []byte(fmt.Sprintf(`{"i":"%02d"}`, i)),
				PartitionKey: aws.String(fmt.Sprintf("partitionKey-%02d", i)),
			}
			_, err := client.PutRecord(ctx, &input)
			require.NoError(t, err)
		}
	}

	// Capture some initial data.
	addData(t, 0, 10)
	advanceCapture(t, &capture)

	// Running the capture again does not re-capture anything.
	advanceCapture(t, &capture)

	// Running capture after splitting and merging shards still doesn't
	// re-capture anything.
	_, err = client.MergeShards(ctx, &kinesis.MergeShardsInput{
		StreamName:           aws.String(testStreams[0]),
		ShardToMerge:         aws.String("shardId-000000000000"),
		AdjacentShardToMerge: aws.String("shardId-000000000001"),
	})
	require.NoError(t, err)
	awaitStreamActive(t, ctx, client, testStreams[0])
	_, err = client.SplitShard(ctx, &kinesis.SplitShardInput{
		StreamName:         aws.String(testStreams[1]),
		ShardToSplit:       aws.String("shardId-000000000001"),
		NewStartingHashKey: aws.String("255000000000000000000000000000000000000"),
	})
	require.NoError(t, err)
	awaitStreamActive(t, ctx, client, testStreams[1])
	advanceCapture(t, &capture)

	// New data added after the shard changes is captured.
	addData(t, 10, 20)
	advanceCapture(t, &capture)

	cupaloy.SnapshotT(t, capture.Summary())
}

func TestCaptureParsing(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client, err := connect(ctx, &conf)
	require.NoError(t, err)

	testStream := "test-stream-parsing"
	cleanup := func() {
		_, _ = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(testStream)})
	}
	cleanup()
	t.Cleanup(cleanup)

	_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(testStream),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err, "failed to create stream")
	awaitStreamActive(t, ctx, client, testStream)

	resourceJson, err := json.Marshal(resource{
		Stream: testStream,
	})
	binding := &pf.CaptureSpec_Binding{
		ResourceConfigJson: resourceJson,
		ResourcePath:       []string{testStream},
		StateKey:           fmt.Sprintf("%s.v0", testStream),
	}

	capture := st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     []*pf.CaptureSpec_Binding{binding},
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers: map[string]*regexp.Regexp{
			"<SEQUENCE_NUM>": regexp.MustCompile(`\d{56}`),
		},
	}

	data := [][]byte{
		[]byte(`{"int1":-4974660706199429123,"int1AsStr":"-4974660706199429123","int2":9223372036854775807,"int2AsStr":"9223372036854775807"}`),
		[]byte(`{"_meta":{"userMetaField1":"hello1","userMetaField2":"world1"},"other":"value"}`),
		[]byte(`{"_meta":{"userMetaField1":"hello2","userMetaField2":"world2","source":{"userSourceField1":"clobbered"}},"other":"value"}`),
	}

	for _, d := range data {
		var input = kinesis.PutRecordInput{
			StreamName:   aws.String(testStream),
			Data:         d,
			PartitionKey: aws.String(fmt.Sprintf("anyPartitionKey")),
		}
		_, err = client.PutRecord(ctx, &input)
		require.NoError(t, err)
	}

	advanceCapture(t, &capture)
	cupaloy.SnapshotT(t, capture.Summary())
}

// testGlueConfig returns a config for Glue tests.
// If AWS credentials are provided, it uses real AWS.
// Otherwise, it attempts to use localstack and skips if Glue is not available.
func testGlueConfig(t *testing.T) Config {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	if useRealAWS() {
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
		return Config{
			Region:             region,
			AWSAccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
			AWSSecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
			AWSSessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		}
	}

	// Using localstack - check if Glue is available (requires Pro)
	conf := Config{
		Region:             "local",
		AWSAccessKeyID:     "x",
		AWSSecretAccessKey: "x",
	}

	// Try to connect to Glue to check if it's available
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	glueClient, err := connectGlue(ctx, &conf)
	if err != nil {
		t.Skipf("skipping Glue test: unable to connect to Glue (localstack Pro required): %v", err)
	}

	// Try a simple Glue operation to verify it's actually working
	_, err = glueClient.ListRegistries(ctx, &glue.ListRegistriesInput{})
	if err != nil {
		t.Skipf("skipping Glue test: Glue not available (localstack Pro required): %v", err)
	}

	return conf
}

func TestCaptureGlueSchema(t *testing.T) {
	ctx := context.Background()

	conf := testGlueConfig(t)
	client, err := connect(ctx, &conf)
	require.NoError(t, err)

	glueClient, err := connectGlue(ctx, &conf)
	require.NoError(t, err)

	// Use unique names when testing against real AWS to avoid conflicts
	suffix := ""
	if useRealAWS() {
		suffix = fmt.Sprintf("-%d", time.Now().UnixNano())
	}
	testStream := "test-stream-glue" + suffix
	registryName := "test-registry" + suffix
	schemaName := "test-schema" + suffix

	cleanup := func() {
		_, _ = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(testStream)})
		_, _ = glueClient.DeleteSchema(ctx, &glue.DeleteSchemaInput{
			SchemaId: &glueTypes.SchemaId{
				RegistryName: aws.String(registryName),
				SchemaName:   aws.String(schemaName),
			},
		})
		_, _ = glueClient.DeleteRegistry(ctx, &glue.DeleteRegistryInput{
			RegistryId: &glueTypes.RegistryId{
				RegistryName: aws.String(registryName),
			},
		})
	}
	cleanup()
	t.Cleanup(cleanup)

	// Wait for stream to be fully deleted if it existed
	awaitStreamDeleted(t, ctx, client, testStream)

	// Create Glue schema registry
	_, err = glueClient.CreateRegistry(ctx, &glue.CreateRegistryInput{
		RegistryName: aws.String(registryName),
	})
	require.NoError(t, err, "failed to create glue registry")

	// Create an Avro schema in Glue
	avroSchema := `{
		"type": "record",
		"name": "TestRecord",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "value", "type": "double"}
		]
	}`

	createSchemaRes, err := glueClient.CreateSchema(ctx, &glue.CreateSchemaInput{
		RegistryId: &glueTypes.RegistryId{
			RegistryName: aws.String(registryName),
		},
		SchemaName:       aws.String(schemaName),
		DataFormat:       glueTypes.DataFormatAvro,
		Compatibility:    glueTypes.CompatibilityNone,
		SchemaDefinition: aws.String(avroSchema),
	})
	require.NoError(t, err, "failed to create glue schema")

	schemaVersionId := aws.ToString(createSchemaRes.SchemaVersionId)

	// Create Kinesis stream
	_, err = client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(testStream),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err, "failed to create stream")
	awaitStreamActive(t, ctx, client, testStream)

	// Small delay to ensure stream is fully propagated
	time.Sleep(2 * time.Second)

	resourceJson, err := json.Marshal(resource{
		Stream: testStream,
	})
	require.NoError(t, err)

	binding := &pf.CaptureSpec_Binding{
		ResourceConfigJson: resourceJson,
		ResourcePath:       []string{testStream},
		StateKey:           fmt.Sprintf("%s.v0", testStream),
	}

	capture := st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     []*pf.CaptureSpec_Binding{binding},
		Validator:    &st.SortedCaptureValidator{IncludeSourcedSchemas: true},
		Sanitizers: map[string]*regexp.Regexp{
			"<SEQUENCE_NUM>":    regexp.MustCompile(`\d{56}`),
			"test-stream-glue":  regexp.MustCompile(`test-stream-glue-\d+`),
		},
	}

	// Create records with Glue schema header
	// Glue header format:
	// - Byte 0: Header version (3)
	// - Byte 1: Compression (0 = none)
	// - Bytes 2-17: Schema version UUID (16 bytes)
	// - Rest: JSON payload

	testDocs := []map[string]any{
		{"id": 1, "name": "first", "value": 1.5},
		{"id": 2, "name": "second", "value": 2.5},
		{"id": 3, "name": "third", "value": 3.5},
	}

	for i, doc := range testDocs {
		payload, err := json.Marshal(doc)
		require.NoError(t, err)

		// Build Glue header
		header := buildGlueHeader(t, schemaVersionId)
		data := append(header, payload...)

		_, err = client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   aws.String(testStream),
			Data:         data,
			PartitionKey: aws.String(fmt.Sprintf("partition-%d", i)),
		})
		require.NoError(t, err, "failed to put record %d", i)
	}

	// Wait for records to be available in Kinesis
	time.Sleep(5 * time.Second)

	advanceCaptureWithTimeout(t, &capture, 10*time.Second)
	cupaloy.SnapshotT(t, capture.Summary())
}

// buildGlueHeader creates a Glue schema registry header from a schema version UUID string.
// The header format is:
// - Byte 0: Header version (3)
// - Byte 1: Compression (0 = none)
// - Bytes 2-17: Schema version UUID (16 bytes in binary form)
func buildGlueHeader(t *testing.T, schemaVersionId string) []byte {
	t.Helper()

	header := make([]byte, 18)
	header[0] = 3 // Header version
	header[1] = 0 // No compression

	// Parse UUID string (format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) to binary
	uuidStr := strings.ReplaceAll(schemaVersionId, "-", "")
	uuidBytes, err := hex.DecodeString(uuidStr)
	require.NoError(t, err, "failed to decode UUID")
	require.Len(t, uuidBytes, 16, "UUID should be 16 bytes")

	copy(header[2:18], uuidBytes)

	return header
}

func advanceCapture(t testing.TB, cs *st.CaptureSpec) {
	advanceCaptureWithTimeout(t, cs, 100*time.Millisecond)
}

func advanceCaptureWithTimeout(t testing.TB, cs *st.CaptureSpec, shutdownDelay time.Duration) {
	t.Helper()

	captureCtx, cancelCapture := context.WithCancel(context.Background())

	// Start an initial watchdog that cancels if no data is received
	shutdownWatchdog := time.AfterFunc(60*time.Second, func() {
		log.WithField("delay", "60s").Debug("capture initial timeout expired with no data")
		cancelCapture()
	})

	var count int

	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		count += 1
		if count > 100 {
			// The Kinesis localstack emulator is kind of buggy and will
			// sometimes continue returning the same record over and over for
			// subsequent shard iterators. Bail out if that seems to be
			// happening.
			cancelCapture()
		}

		// Reset the watchdog with the shorter shutdown delay once we start receiving data
		shutdownWatchdog.Reset(shutdownDelay)
	})
}

func awaitStreamActive(t *testing.T, ctx context.Context, client *kinesis.Client, stream string) {
	t.Helper()

	for {
		res, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: &stream,
		})
		require.NoError(t, err)

		if res.StreamDescription.StreamStatus == "ACTIVE" {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func awaitStreamDeleted(t *testing.T, ctx context.Context, client *kinesis.Client, stream string) {
	t.Helper()

	for i := 0; i < 60; i++ {
		_, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: &stream,
		})
		if err != nil {
			// Stream doesn't exist, we're done
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Timed out waiting for stream %s to be deleted", stream)
}
