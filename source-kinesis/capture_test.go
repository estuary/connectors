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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/airbyte"
	"github.com/estuary/connectors/shardrange"
	"github.com/stretchr/testify/require"
)

func TestIsRecordWithinRange(t *testing.T) {
	var flowRange = shardrange.Range{
		Begin: 5,
		End:   10,
	}
	var kinesisRange = shardrange.Range{
		Begin: 7,
		End:   15,
	}
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 5))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 7))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 10))
	// Record is outside of the kinesis range, but is claimed by the flow shard because it overlaps
	// the low end of the kinesis range
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 4))

	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 11))
	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 17))

	flowRange.Begin = 8
	flowRange.End = 20
	// The record is no longer claimed by the flow shard because its range does not overlap the low
	// end of the kinesis range
	require.False(t, isRecordWithinRange(flowRange, kinesisRange, 4))
	// These should now be claimed because the flow range overlaps the high end of the kinesis range
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 17))
	require.True(t, isRecordWithinRange(flowRange, kinesisRange, 9999))
}

func TestKinesisCaptureWithShardOverlap(t *testing.T) {
	var configFile = airbyte.JSONFile("testdata/kinesis-config.json")
	var conf = Config{}
	var err = configFile.Parse(&conf)
	require.NoError(t, err)
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

	var shard1Conf = conf
	shard1Conf.ShardRange = &shardrange.Range{
		Begin: 0,
		End:   math.MaxUint32 / 2,
	}
	go readStream(ctx, shard1Conf, client, stream, nil, dataCh)

	var shard2Conf = conf
	shard2Conf.ShardRange = &shardrange.Range{
		Begin: math.MaxUint32 / 2,
		End:   math.MaxUint32,
	}
	go readStream(ctx, shard2Conf, client, stream, nil, dataCh)

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
	var configFile = airbyte.ConfigFile{
		ConfigFile: airbyte.JSONFile("testdata/kinesis-config.json"),
	}
	var conf = Config{}
	var err = configFile.ConfigFile.Parse(&conf)
	require.NoError(t, err)
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

	// Test the discover command and assert that it returns the stream we just created
	catalog, err := discoverCatalog(configFile)
	require.NoError(t, err, "discover catalog failed")

	require.GreaterOrEqual(t, len(catalog.Streams), 1)
	var discoveredStream *airbyte.Stream
	for _, s := range catalog.Streams {
		if s.Name == stream {
			discoveredStream = &s
			break
		}
	}
	require.NotNil(t, discoveredStream, "missing expected stream")
	var configuredCatalog = airbyte.ConfiguredCatalog{
		Streams: []airbyte.ConfiguredStream{{
			Stream:   *discoveredStream,
			SyncMode: airbyte.SyncModeIncremental,
		}},
	}
	catalogJson, err := json.Marshal(&configuredCatalog)
	require.NoError(t, err)
	var catalogFile = path.Join(tmpDir, "catalog.json")
	err = ioutil.WriteFile(catalogFile, catalogJson, 0644)
	require.NoError(t, err)

	// Test a basic read and assert that we get a proper state message at the end
	var readArgs = airbyte.ReadCmd{
		ConfigFile:  configFile,
		CatalogFile: airbyte.JSONFile(catalogFile),
		StateFile:   airbyte.JSONFile(stateFile),
	}

	var ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	var reader, writer = io.Pipe()

	go func() {
		var failure = readStreamsTo(ctx, readArgs, writer)
		writer.Close()
		if failure != nil {
			fmt.Printf("readStreamsTo failed with error: %v\n", failure)
		}
	}()
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

	var decoder = json.NewDecoder(reader)
	var foundRecords = 0
	for foundRecords < recordCount {
		var msg = airbyte.Message{}
		err = decoder.Decode(&msg)
		require.NoError(t, err)
		if msg.Record != nil {
			foundRecords++
		}
	}
	// After reading all the records, we should have a final state message.
	// There may have been intermediate state messages, but the goal here is to verify the last
	// one to make sure it includes the expected sequence number and the canary state we passed
	// in at the beginning
	var stateMessage = struct {
		State struct {
			Data map[string]map[string]string
		}
	}{}
	err = decoder.Decode(&stateMessage)
	require.NoError(t, err)
	require.NotNil(t, stateMessage.State, "expected non-nill state")

	var canaryResult, hasCanary = stateMessage.State.Data["canary"]
	require.True(t, hasCanary, "missing canary state")
	require.Equal(t, canaryState, canaryResult["foo"])

	var streamResult, hasStream = stateMessage.State.Data[stream]
	require.True(t, hasStream, "missing stream state")
	require.Equal(t, lastSeq, streamResult[shardId])
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
