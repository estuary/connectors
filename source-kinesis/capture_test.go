package main

// This integration test requires that kinesis is running and that the configuration in
// testdata/kinesis-config.json points to the correct instance. The config that's there is
// intended for use with a localstack container.
// You can run such a container using:
// docker run --rm -it -p 4566:4566 -p 4571:4571 -e 'SERVICES=kinesis' -e 'KINESIS_ERROR_PROBABILITY=0.2' localstack/localstack
// The KINESIS_ERROR_PROBABILITY simulates ProvisionedThroughputExceededExceptions in order to
// exercise the retry logic.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestIsRecordWithinRange(t *testing.T) {
	var flowRange = &pf.RangeSpec{
		KeyBegin: 5,
		KeyEnd:   10,
	}
	var kinesisRange = &pf.RangeSpec{
		KeyBegin: 7,
		KeyEnd:   15,
	}
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 5))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 7))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 10))
	// Record is outside of the kinesis range, but is claimed by the flow shard because it overlaps
	// the low end of the kinesis range
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 4))

	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 11))
	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 17))

	flowRange.KeyBegin = 8
	flowRange.KeyEnd = 20
	// The record is no longer claimed by the flow shard because its range does not overlap the low
	// end of the kinesis range
	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 4))
	// These should now be claimed because the flow range overlaps the high end of the kinesis range
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 17))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 9999))
}

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

func TestKinesisCaptureWithShardOverlap(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client, err := connect(&conf)
	require.NoError(t, err)

	var stream = "test-" + randAlpha(6)
	var testShards int64 = 3
	var createStreamReq = &kinesis.CreateStreamInput{
		StreamName: &stream,
		ShardCount: &testShards,
	}
	_, err = client.CreateStream(createStreamReq)
	require.NoError(t, err, "failed to create stream")

	defer func() {
		var deleteStreamReq = kinesis.DeleteStreamInput{
			StreamName: &stream,
		}
		var _, err = client.DeleteStream(&deleteStreamReq)
		require.NoError(t, err, "failed to delete stream")
	}()
	awaitStreamActive(t, ctx, client, stream)

	var partitionKeys = []string{"canary", "fooo", "daffy", "pancakes"}
	var sequenceNumbers = make(map[string]string)
	for i := 0; i < 24; i++ {
		var partitionKey = partitionKeys[i%(len(partitionKeys)-1)]
		var input = &kinesis.PutRecordInput{
			StreamName:   &stream,
			PartitionKey: &partitionKey,
			Data:         []byte(fmt.Sprintf(`{"partitionKey":%q, "counter": %d}`, partitionKey, i)),
		}
		if prevSeq, ok := sequenceNumbers[partitionKey]; ok {
			input.SequenceNumberForOrdering = &prevSeq
		}

		resp, err := client.PutRecord(input)
		require.NoError(t, err)
		sequenceNumbers[partitionKey] = *resp.SequenceNumber
	}

	var dataCh = make(chan readResult)
	var waitGroup = new(sync.WaitGroup)

	var shard1Range = &pf.RangeSpec{
		KeyBegin: 0,
		KeyEnd:   math.MaxUint32 / 2,
	}
	var shard2Range = &pf.RangeSpec{
		KeyBegin: math.MaxUint32 / 2,
		KeyEnd:   math.MaxUint32,
	}
	waitGroup.Add(2)
	go readStream(ctx, shard1Range, client, 0, stream, nil, dataCh, waitGroup)
	go readStream(ctx, shard2Range, client, 1, stream, nil, dataCh, waitGroup)

	var countersByPartition = make(map[string]int)
	var foundRecords = 0
	for foundRecords < 24 {
		select {
		case next := <-dataCh:
			require.NoError(t, next.err, "readResult had error")
			for _, rec := range next.records {
				var target = struct {
					PartitionKey string
					Counter      int
				}{}
				err = json.Unmarshal(rec, &target)
				require.NoError(t, err, "failed to unmarshal record")

				if lastCounter, ok := countersByPartition[target.PartitionKey]; ok {
					require.Greaterf(t, target.Counter, lastCounter, "expected counter for partition '%s' to increase", target.PartitionKey)
				}
				countersByPartition[target.PartitionKey] = target.Counter
				foundRecords++
			}
		case <-time.After(time.Second * 5):
			require.Fail(t, "timed out receiving next record")
		}
	}
	require.Equal(t, 24, foundRecords)
}

func TestKinesisCapture(t *testing.T) {
	ctx := context.Background()

	conf := testConfig(t)
	client, err := connect(&conf)
	require.NoError(t, err)

	testStreams := []string{"test-stream-1", "test-stream-2"}

	for _, s := range testStreams {
		s := s
		_, err = client.CreateStreamWithContext(ctx, &kinesis.CreateStreamInput{
			StreamName: aws.String(s),
			ShardCount: aws.Int64(2),
		})
		require.NoError(t, err, "failed to create stream")

		awaitStreamActive(t, ctx, client, s)

		defer func() {
			_, err := client.DeleteStreamWithContext(ctx, &kinesis.DeleteStreamInput{
				StreamName: aws.String(s),
			})
			require.NoError(t, err, "failed to delete stream")
		}()
	}

	// Test the discover command and assert that it returns the streams we just created.
	d := &driver{}
	configJson, err := json.Marshal(conf)
	require.NoError(t, err)
	discovered, err := d.Discover(ctx, &pc.Request_Discover{
		ConfigJson: json.RawMessage(configJson),
	})
	require.NoError(t, err)

	bindings := []*pf.CaptureSpec_Binding{}
	for _, s := range testStreams {
		require.True(t, slices.ContainsFunc(discovered.Bindings, func(b *pc.Response_Discovered_Binding) bool {
			return b.RecommendedName == s
		}))

		resourceJson, err := json.Marshal(resource{
			Stream: s,
		})
		require.NoError(t, err)
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: resourceJson,
			ResourcePath:       []string{s},
		})

	}

	// Add test data to the streams to be captured.
	var recordCount = 10
	for i := 0; i < recordCount; i++ {
		var input = kinesis.PutRecordInput{
			StreamName:   aws.String(testStreams[i%2]),
			Data:         []byte(fmt.Sprintf(`{"i":"%d"}`, i)),
			PartitionKey: aws.String(fmt.Sprintf("partitionKey-%d", i)),
		}
		_, err := client.PutRecord(&input)
		require.NoError(t, err)
	}

	// Run the capture.
	capture := st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     bindings,
		Validator:    &st.SortedCaptureValidator{},
		Sanitizers: map[string]*regexp.Regexp{
			"<SEQUENCE_NUM>": regexp.MustCompile(`\d{56}`),
		},
	}

	captureCtx, cancelCapture := context.WithCancel(context.Background())

	const shutdownDelay = 100 * time.Millisecond
	var shutdownWatchdog *time.Timer

	capture.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
	})

	cupaloy.SnapshotT(t, capture.Summary())
}

// The kinesis stream could take a while before it becomes active, so this just polls until the
// status indicates that it's active.
func awaitStreamActive(t *testing.T, ctx context.Context, client *kinesis.Kinesis, stream string) {
	t.Helper()

	var input = kinesis.DescribeStreamInput{
		StreamName: &stream,
	}
	var err = client.WaitUntilStreamExistsWithContext(ctx, &input)
	require.NoError(t, err, "error waiting for kinesis stream to become active")
}

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

var generator = rand.New(rand.NewSource(time.Now().UnixNano()))

func randAlpha(n int) string {
	var target = make([]byte, n)
	for i := range target {
		target[i] = letterBytes[generator.Intn(len(letterBytes))]
	}
	return string(target)
}
