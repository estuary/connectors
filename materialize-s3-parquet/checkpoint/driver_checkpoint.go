package checkpoint

// DriverCheckpoint represents the checkpoint value that is given to Flow by the driver. This exists in a separate
// package to allow the materialize-rockset connector to read checkpoints from a prior run of the materialize-s3-parquet
// connector. It does this to allow more efficient backfills of data into rockset, by first materializing parquet files
// into S3, and then switching the materialization over to the rockset connector.
type DriverCheckpoint struct {
	// The flow checkpoint (base64-encoded), which marks the txn that has been successfully
	// materialized and stored in the cloud. If the materialization process is stopped
	// for any reason, this is the checkpoint to resume from.
	B64EncodedFlowCheckpoint string `json:"b64EncodedFlowCheckpoint"`
	// The sequence number used to name the next files to be uploaded to the cloud.
	// To be specific, the next parquet file from the i-th binding is named using the deterministic pattern of
	// "<KeyBegin>_<KeyEnd>_<NextSeqNumList[i]>.parquet".
	// The NextSeqNumList[i] is increased by 1 after each successful upload from the i-th binding.
	NextSeqNumList []int `json:"nextSeqNumList"`
}

// Validate implements some interface that's required in order to use json.UnmarshalStrict
func (cp *DriverCheckpoint) Validate() error { return nil }
