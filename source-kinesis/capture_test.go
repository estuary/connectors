package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	return Config{
		Region:             "local",
		AWSAccessKeyID:     "x",
		AWSSecretAccessKey: "x",
		Advanced: advancedConfig{
			Endpoint: "http://localhost:4566",
		},
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

func advanceCapture(t testing.TB, cs *st.CaptureSpec) {
	t.Helper()

	captureCtx, cancelCapture := context.WithCancel(context.Background())

	const shutdownDelay = 100 * time.Millisecond
	var shutdownWatchdog *time.Timer
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

		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
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
