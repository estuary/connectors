package connector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassifyCommitQuery(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{
			name: "concurrent COPY INTO of the same files",
			err:  fmt.Errorf("query failed: %w", errors.New("[COPY_INTO_DUPLICATED_FILES_COPY_NOT_ALLOWED] Duplicated files were committed in a concurrent COPY INTO operation.")),
			want: retry.Retryable,
		},
		{
			name: "concurrent append",
			err:  errors.New("[DELTA_CONCURRENT_APPEND] Files were added to the root of the table by a concurrent update."),
			want: retry.Fatal,
		},
		{
			name: "missing staged files",
			err:  errors.New("[PATH_NOT_FOUND] Path does not exist"),
			want: retry.Fatal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyCommitQuery(tt.err))
		})
	}
}

func TestPutWorkerRetriesUploads(t *testing.T) {
	var uploadErr = errors.New("put failed")

	var run = func(t *testing.T, failFirst int) (string, error) {
		var dir = t.TempDir()
		var path = filepath.Join(dir, "file.json.gz")
		require.NoError(t, os.WriteFile(path, []byte("{}"), 0o644))

		var db = recordingDB(t, uploadErr)
		recording.failFirst = failFirst

		var paths = make(chan string, 1)
		paths <- path
		close(paths)
		var f = &stagedFile{dir: dir, root: "/Volumes/c/s/flow_staging", txnDir: "txn"}
		return path, f.putWorker(t.Context(), db, paths)
	}

	t.Run("succeeds on a later attempt", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			path, err := run(t, putRetry.MaxAttempts-1)
			require.NoError(t, err)
			require.Equal(t, putRetry.MaxAttempts, recording.attempts)
			require.NoFileExists(t, path)
		})
	})

	t.Run("gives up after its attempts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			path, err := run(t, 0)
			require.ErrorIs(t, err, uploadErr)
			require.Equal(t, putRetry.MaxAttempts, recording.attempts)
			require.FileExists(t, path)
		})
	})
}
