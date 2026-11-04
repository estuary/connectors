package gosnowflake

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

type estuaryGcsMock struct {
	calls int
	do    func(req *http.Request) (*http.Response, error)
}

func (m *estuaryGcsMock) Do(req *http.Request) (*http.Response, error) {
	m.calls++
	return m.do(req)
}

// TestEstuaryGcsTransportErrorIsReturned checks that a PUT failing without a
// response is retried and then reported with its cause, not as
// "unkown error uploading".
func TestEstuaryGcsTransportErrorIsReturned(t *testing.T) {
	src := filepath.Join(t.TempDir(), "upload.json.gz")
	require.NoError(t, os.WriteFile(src, []byte(strings.Repeat("x", 1<<16)), 0o644))

	resetErr := &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.ECONNRESET)}
	mock := &estuaryGcsMock{do: func(req *http.Request) (*http.Response, error) {
		// Read part of the body, as a connection that dies mid-upload would.
		_, _ = io.CopyN(io.Discard, req.Body, 1<<10)
		return nil, resetErr
	}}

	info := execResponseStageInfo{Location: "gcs-blob/storage/users/456/", LocationType: "GCS"}
	gcsCli, err := new(snowflakeGcsClient).createClient(&info, false, &snowflakeTelemetry{})
	require.NoError(t, err)

	meta := fileMetadata{
		name:               "upload.json.gz",
		stageLocationType:  "GCS",
		noSleepingTime:     true,
		parallel:           1,
		client:             gcsCli,
		sha256Digest:       "123456789abcdef",
		stageInfo:          &info,
		dstFileName:        "upload.json.gz",
		srcFileName:        src,
		realSrcFileName:    src,
		uploadSize:         1 << 16,
		overwrite:          true,
		dstCompressionType: compressionTypes["GZIP"],
		options:            &SnowflakeFileTransferOptions{MultiPartThreshold: 8 << 20},
		mockGcsClient:      mock,
		sfa:                &snowflakeFileTransferAgent{sc: &snowflakeConn{cfg: &Config{}}},
	}

	err = new(remoteStorageUtil).uploadOneFile(context.Background(), &meta)
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.ECONNRESET)
	require.NotContains(t, err.Error(), "unkown error")
	require.Equal(t, needRetry, meta.resStatus)
	require.Equal(t, defaultMaxRetry, mock.calls)
}
