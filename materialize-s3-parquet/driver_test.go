package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestConfig(t *testing.T) {
	var validConfig = config{
		AWSAccessKeyID:          "testKey",
		AWSSecretAccessKey:      "testSecret",
		Bucket:                  "testBucket",
		Region:                  "us-east-1",
		UploadIntervalInSeconds: 60,
	}

	require.NoError(t, validConfig.Validate())

	var missingRegion = validConfig
	missingRegion.Region = ""
	require.Error(t, missingRegion.Validate(), "expected validation error")

	var missingBucket = validConfig
	missingBucket.Bucket = ""
	require.Error(t, missingBucket.Validate(), "expected validation error")

	var missingAccessKey = validConfig
	missingAccessKey.AWSAccessKeyID = ""
	require.Error(t, missingAccessKey.Validate(), "expected validation error")

	var missingSecretKey = validConfig
	missingSecretKey.AWSSecretAccessKey = ""
	require.Error(t, missingSecretKey.Validate(), "expected validation error")

	var negativeUpdateInterval = validConfig
	negativeUpdateInterval.UploadIntervalInSeconds = -10
	require.Error(t, negativeUpdateInterval.Validate(), "expected validation error")
}

func TestResource(t *testing.T) {
	var validResource = resource{
		PathPrefix:      "test_path_prefix",
		CompressionType: "snappy",
	}
	require.NoError(t, validResource.Validate())
	require.Equal(t, parquet.CompressionCodec_SNAPPY, validResource.CompressionCodec())

	var missingPathPrefix = validResource
	missingPathPrefix.PathPrefix = ""
	require.Error(t, missingPathPrefix.Validate(), "expected validation error")

	var invalidCompressionType = validResource
	invalidCompressionType.CompressionType = "random"
	require.Error(t, invalidCompressionType.Validate(), "expected validation error")
}

func TestMarshalAndUnmarshalDriverCheckpointJson(t *testing.T) {
	flowCheckpoint, nextSeqNumList, err := unmarshalDriverCheckpointJSON(nil)
	require.Nil(t, flowCheckpoint)
	require.Nil(t, nextSeqNumList)
	require.NoError(t, err)

	require.Panics(t, func() { marshalDriverCheckpointJSON([]byte{}, []int{0}) }, "expected panics for empty checkpoint")

	testCases := []struct {
		checkpoint     []byte
		nextSeqNumList []int
	}{
		{[]byte("checkpoint_a"), nil},
		{[]byte("checkpoint_b"), []int{}},
		{[]byte("checkpoint_c"), []int{1, 1, 2, 3, 4}},
	}

	for _, test := range testCases {
		marshaledDCJ, err := marshalDriverCheckpointJSON(test.checkpoint, test.nextSeqNumList)
		require.NoError(t, err)

		checkpoint, nextSeqNumList, err := unmarshalDriverCheckpointJSON(marshaledDCJ)
		require.NoError(t, err)
		require.Equal(t, test.checkpoint, checkpoint)
		require.Equal(t, test.nextSeqNumList, nextSeqNumList)
	}
	require.Panics(t, func() { marshalDriverCheckpointJSON(nil, []int{}) })
}

func TestS3ParquetDriverSpec(t *testing.T) {
	var drv = new(driver)
	var resp, err1 = drv.Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_FLOW_SINK})
	require.NoError(t, err1)
	var formatted, err2 = json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err2)
	cupaloy.SnapshotT(t, formatted)
}

func TestTransactor(t *testing.T) {
	var mockClock = clock.NewMock()
	var mockNextSeqNumList = []int{1, 2, 3}
	var transactor = &transactor{
		clock:                mockClock,
		fileProcessor:        newMockFileProcessor(mockNextSeqNumList),
		driverCheckpointJSON: nil,
		runtimeCheckpoint:    nil,
		uploadInterval:       time.Second,
		lastUploadTime:       mockClock.Now(),
	}

	var testFlowCheckpoint = []byte("test_checkPoint")
	transactor.runtimeCheckpoint = testFlowCheckpoint
	require.Equal(t, testFlowCheckpoint, transactor.runtimeCheckpoint)

	// driverCheckpoint is not set if no upload-to-cloud action is triggered.
	require.NoError(t, transactor.maybeUpload())
	require.Nil(t, transactor.driverCheckpointJSON)

	// driverCheckpoint is set after an upload-to-cloud action.
	mockClock.Add(time.Second * 2)
	require.NoError(t, transactor.maybeUpload())
	var expected, _ = marshalDriverCheckpointJSON(testFlowCheckpoint, mockNextSeqNumList)
	require.Equal(t, 0, bytes.Compare(transactor.driverCheckpointJSON, expected))
}
