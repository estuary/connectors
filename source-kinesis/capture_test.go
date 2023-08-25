//go:build kinesistest
// +build kinesistest

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
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestIsRecordWithinRange(t *testing.T) {
	var flowRange = pf.RangeSpec{
		KeyBegin: 5,
		KeyEnd:   10,
	}
	var kinesisRange = pf.RangeSpec{
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

func TestKinesisCaptureWithShardOverlap(t *testing.T) {
	var conf = Config{
		Region:             "local",
		Endpoint:           "http://localhost:4566",
		AWSAccessKeyID:     "x",
		AWSSecretAccessKey: "x",
	}

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
	awaitStreamActive(t, client, stream)

	var dataCh = make(chan readResult)
	var ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	var waitGroup = new(sync.WaitGroup)

	var shard1Range = pf.RangeSpec{
		KeyBegin: 0,
		KeyEnd:   math.MaxUint32 / 2,
	}
	waitGroup.Add(1)
	go readStream(ctx, shard1Range, client, stream, nil, dataCh, nil, waitGroup)

	var shard2Range = pf.RangeSpec{
		KeyBegin: math.MaxUint32 / 2,
		KeyEnd:   math.MaxUint32,
	}
	waitGroup.Add(1)
	go readStream(ctx, shard2Range, client, stream, nil, dataCh, nil, waitGroup)

	var partitionKeys = []string{"furst", "sekund", "thuurd", "phorth"}
	var sequencNumbers = make(map[string]string)
	for i := 0; i < 24; i++ {
		var partitionKey = partitionKeys[i%(len(partitionKeys)-1)]
		var input = &kinesis.PutRecordInput{
			StreamName:   &stream,
			PartitionKey: &partitionKey,
			Data:         []byte(fmt.Sprintf(`{"partitionKey":%q, "counter": %d}`, partitionKey, i)),
		}
		if prevSeq, ok := sequencNumbers[partitionKey]; ok {
			input.SequenceNumberForOrdering = &prevSeq
		}

		var doPut = func() bool {
			var resp, putErr = client.PutRecord(input)
			if putErr == nil {
				sequencNumbers[partitionKey] = *resp.SequenceNumber
			}
			return putErr == nil
		}
		require.Eventually(t, doPut, time.Second, time.Millisecond*100, "failed to put record")
	}

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
	var conf = Config{
		Region:             "local",
		Endpoint:           "http://localhost:4566",
		AWSAccessKeyID:     "x",
		AWSSecretAccessKey: "x",
	}
	client, err := connect(&conf)
	require.NoError(t, err)

	var stream = "test-" + randAlpha(6)
	var testShards int64 = 1
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
	awaitStreamActive(t, client, stream)

	tmpDir, err := ioutil.TempDir("", "kinesis-capture-test-")
	require.NoError(t, err)

	// We'll seed the state with a random value so that we can assert that it gets included in the
	// final state. This is to ensure that we don't loose state if the connector gets invoked with a
	// different set of streams.
	var canaryState = randAlpha(20)
	var stateFile = path.Join(tmpDir, "state.json")
	var stateJson = fmt.Sprintf(`{"canary": {"foo": %q}}`, canaryState)
	err = ioutil.WriteFile(stateFile, []byte(stateJson), 0644)
	require.NoError(t, err)

	var ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	// Test the discover command and assert that it returns the stream we just created
	streamNames, err := listAllStreams(ctx, client)
	require.NoError(t, err)

	bindings := discoverStreams(ctx, client, streamNames)

	require.GreaterOrEqual(t, len(bindings), 1)
	var discoveredStream *pc.Response_Discovered_Binding
	for _, s := range bindings {
		var res resource
		err := pf.UnmarshalStrict(s.ResourceConfigJson, &res)
		require.NoError(t, err)

		if res.Stream == stream {
			discoveredStream = &s
			break
		}
	}

	start, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:10:00Z")
	require.NoError(t, err)

	capture := &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: conf,
		Bindings:     bindings,
		Validator: &st.WatchdogValidator{
			Inner:         &st.ChecksumValidator{},
			WatchdogTimer: wdt,
			ResetPeriod:   quiescentTimeout,
		},
		Sanitizers: make(map[string]*regexp.Regexp),
	}

	// We need to use a relatively long shutdown delay, since the backfill can potentially cover
	// periods of time with no historical data.
	const shutdownDelay = 5000 * time.Millisecond
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
	capture.Reset()

	var recordCount = 3
	var lastSeq string
	var shardId string
	for i := 0; i < recordCount; i++ {
		var input = kinesis.PutRecordInput{
			StreamName:   &stream,
			Data:         []byte(`{"oh":"my"}`),
			PartitionKey: aws.String("wat"),
		}
		require.Eventually(t, func() bool {
			out, err := client.PutRecord(&input)
			if err == nil {
				lastSeq = *out.SequenceNumber
				shardId = *out.ShardId
			}
			return err == nil
		}, time.Second, time.Millisecond*20, "failed to put record")
	}
}

// The kinesis stream could take a while before it becomes active, so this just polls until the
// status indicates that it's active.
func awaitStreamActive(t *testing.T, client *kinesis.Kinesis, stream string) {
	var input = kinesis.DescribeStreamInput{
		StreamName: &stream,
	}
	var err = client.WaitUntilStreamExists(&input)
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
