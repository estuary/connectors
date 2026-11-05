package connector

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmTypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassifySSMPut(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{"too many updates", fmt.Errorf("operation error SSM: PutParameter: %w", &ssmTypes.TooManyUpdates{}), retry.Retryable},
		{"parameter limit exceeded", &ssmTypes.ParameterLimitExceeded{}, retry.Fatal},
		{"other error", errors.New("boom"), retry.Fatal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifySSMPut(tt.err))
		})
	}
}

type fakeParameterPutter struct {
	errs  []error
	calls int
	at    []time.Time
}

func (f *fakeParameterPutter) PutParameter(context.Context, *ssm.PutParameterInput, ...func(*ssm.Options)) (*ssm.PutParameterOutput, error) {
	f.calls++
	f.at = append(f.at, time.Now())
	if len(f.errs) == 0 {
		return &ssm.PutParameterOutput{}, nil
	}
	var err = f.errs[0]
	f.errs = f.errs[1:]
	return nil, err
}

func TestSSMPutParameterWithRetry(t *testing.T) {
	var tooManyUpdates = &ssmTypes.TooManyUpdates{}

	t.Run("succeeds after too many updates", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var putter = &fakeParameterPutter{errs: []error{tooManyUpdates, tooManyUpdates}}
			require.NoError(t, ssmPutParameterWithRetry(t.Context(), putter, "name", "value"))
			require.Equal(t, 3, putter.calls)
			require.GreaterOrEqual(t, putter.at[1].Sub(putter.at[0]), time.Second)
		})
	})

	t.Run("gives up after five attempts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var putter = &fakeParameterPutter{errs: []error{tooManyUpdates, tooManyUpdates, tooManyUpdates, tooManyUpdates, tooManyUpdates, tooManyUpdates}}
			require.ErrorIs(t, ssmPutParameterWithRetry(t.Context(), putter, "name", "value"), tooManyUpdates)
			require.Equal(t, 5, putter.calls)
		})
	})

	t.Run("other errors are not retried", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var other = errors.New("access denied")
			var putter = &fakeParameterPutter{errs: []error{other}}
			require.ErrorIs(t, ssmPutParameterWithRetry(t.Context(), putter, "name", "value"), other)
			require.Equal(t, 1, putter.calls)
		})
	})

	t.Run("stops waiting when the context is cancelled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var ctx, cancel = context.WithCancel(t.Context())
			var putter = &fakeParameterPutter{errs: []error{tooManyUpdates, tooManyUpdates}}
			go func() {
				time.Sleep(500 * time.Millisecond)
				cancel()
			}()
			require.ErrorIs(t, ssmPutParameterWithRetry(ctx, putter, "name", "value"), context.Canceled)
			require.Equal(t, 1, putter.calls)
		})
	})
}
