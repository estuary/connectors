package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestTransientFailures(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		want   []int
		wantOk bool
	}{
		{
			name:   "transient write errors",
			err:    bulkErr(map[int]int{2: 189}),
			want:   []int{2},
			wantOk: true,
		},
		{
			name: "a duplicate key alongside a transient error",
			err:  bulkErr(map[int]int{0: 189, 1: 11000}),
		},
		{
			name: "write concern error",
			err: mongo.BulkWriteException{
				WriteConcernError: &mongo.WriteConcernError{Code: 64, Message: "waiting for replication timed out"},
			},
		},
		{
			name: "command error, which the driver already retried",
			err:  mongo.CommandError{Code: 189, Message: "primary stepped down"},
		},
		{
			name: "context cancelled",
			err:  fmt.Errorf("bulk write: %w", context.Canceled),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got, ok = transientFailures(tt.err)
			require.Equal(t, tt.wantOk, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClassify(t *testing.T) {
	require.Equal(t, retry.Retryable, classify(&unwrittenModelsError{count: 1, err: bulkErr(map[int]int{0: 189})}))
	require.Equal(t, retry.Fatal, classify(bulkErr(map[int]int{0: 189})))
	require.Equal(t, retry.Fatal, classify(errors.New("connection reset")))
}
