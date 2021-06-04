// +build kinesistest

package main

// This integration test requires that kinesis is running and that the configuration in
// testdata/kinesis-config.json points to the correct instance. The config that's there is
// intended for use with a localstack container.
// You can run such a container using:

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/go/airbyte"
	"github.com/stretchr/testify/require"
)

const recordCount = 6

func TestKinesisCapture(t *testing.T) {
	var configFile = airbyte.JSONFile("testdata/kinesis-config.json")
	var conf = Config{}
	var err = configFile.Parse(&conf)
	require.NoError(t, err)
	client, err := connect(&conf)
	require.NoError(t, err)

	var stream = "test-" + randAlpha(6)
	var testShards int64 = 2
	var createStreamReq = &kinesis.CreateStreamInput{
		StreamName: &stream,
		ShardCount: &testShards,
	}
	_, err = client.CreateStream(createStreamReq)
	require.NoError(t, err, "failed to create stream")
	// Setup this defer before waiting for the stream to be active, that way we can still delete it
	// if there's an error waiting for it to become active.
	defer func() {
		var deleteStreamReq = kinesis.DeleteStreamInput{
			StreamName: &stream,
		}
		var _, err = client.DeleteStream(&deleteStreamReq)
		require.NoError(t, err, "failed to delete stream")
	}()
	awaitStreamActive(t, client, stream)

	tempDir, err := ioutil.TempDir("", "kinesis-test-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Assert that discover will return the expected stream
	catalog, err := discoverCatalog(airbyte.ConfigFile{ConfigFile: configFile})
	require.NoError(t, err, "discover failed")

	var discoveredStream airbyte.Stream
	for _, s := range catalog.Streams {
		if s.Name == stream {
			discoveredStream = s
			break
		}
	}
	require.NotEmpty(t, discoveredStream.Name, "discover did not return the expected stream")

	// Generate a random string that we'll put in each record so we can read it back.
	var canary = randAlpha(24)
	var dataCh = make(chan readResult)
	var ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	go readStream(ctx, conf, client, stream, nil, dataCh)
	go addData(ctx, t, client, stream, canary)

	var foundRecords = 0
	for foundRecords < recordCount {
		select {
		case next := <-dataCh:
			require.NoError(t, next.Error, "readResult had error")
			for _, rec := range next.Records {
				if bytes.Contains(rec, []byte(canary)) {
					foundRecords++
				} else {
					fmt.Printf("Got record that doesn't match canary: %#v\n", rec)
				}
			}
		case <-time.After(time.Second * 5):
			require.Fail(t, "timed out receiving next record")
		}
	}
	require.Equal(t, recordCount, foundRecords)
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

func addData(ctx context.Context, t *testing.T, client *kinesis.Kinesis, stream, canary string) {
	var input = &kinesis.PutRecordsInput{
		Records:    make([]*kinesis.PutRecordsRequestEntry, recordCount),
		StreamName: &stream,
	}
	for i := range input.Records {
		var partKey = randAlpha(8)
		input.Records[i] = &kinesis.PutRecordsRequestEntry{
			Data:         []byte(fmt.Sprintf(`{"foo": "bar", "id": %d, "canary": "%s"}`, i, canary)),
			PartitionKey: &partKey,
		}
	}

	var err error
	var resp *kinesis.PutRecordsOutput
	var doPutRecords = func() bool {
		resp, err = client.PutRecords(input)
		return err == nil && (resp.FailedRecordCount == nil || *resp.FailedRecordCount == 0)
	}
	require.Eventuallyf(
		t,
		doPutRecords,
		time.Second*5,
		time.Millisecond*200,
		"Failed to put records, resp: %#v, err: %v",
		resp, err,
	)
}

func writeJSONFile(t *testing.T, obj interface{}, path string) {
	var f, err = os.Create(path)
	require.NoError(t, err)
	err = json.NewEncoder(f).Encode(obj)
	require.NoError(t, err)
	require.NoError(t, f.Close())
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
