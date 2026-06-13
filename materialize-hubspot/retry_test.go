package hubspot

import (
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetry(t *testing.T) {
	tests := []struct {
		name           string
		backoffSeconds []float64
		results        []error
		attempts       int
		totalDelay     time.Duration
		expectedErr    bool
	}{
		{
			name:           "works second time",
			backoffSeconds: DefaultBackoff,
			results: []error{
				NewTemporaryError("attempt 1"),
				nil,
			},
			attempts:   2,
			totalDelay: 0,
		},
		{
			name:           "always fails",
			backoffSeconds: DefaultBackoff,
			results: []error{
				NewTemporaryError("attempt 1"),
				NewTemporaryError("attempt 2"),
				NewTemporaryError("attempt 3"),
				NewTemporaryError("attempt 4"),
				NewTemporaryError("attempt 5"),
				NewTemporaryError("attempt 6"),
				NewTemporaryError("attempt 7"),
			},
			attempts:    7,
			totalDelay:  (4 + 8 + 15 + 30 + 60) * time.Second,
			expectedErr: true,
		},
		{
			name:           "extra delay",
			backoffSeconds: DefaultBackoff,
			results: []error{
				&TemporaryError{err: errors.New("attempt 1"), extraDelay: 10 * time.Second},
				&TemporaryError{err: errors.New("attempt 2"), extraDelay: 10 * time.Second},
				nil,
			},
			attempts:   3,
			totalDelay: 24 * time.Second,
		},
		{
			// Ensure that ExtraDelay matches even when wrapped in another error.
			name:           "extra delay wrapped",
			backoffSeconds: DefaultBackoff,
			results: []error{
				fmt.Errorf("inner: %w", &TemporaryError{err: errors.New("attempt 1"), extraDelay: 10 * time.Second}),
				nil,
			},
			attempts:   2,
			totalDelay: 10 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now().UTC()

				try := 0
				err := Retry[*TemporaryError](t.Context(), tt.backoffSeconds, func(attempt int) error {
					try += 1
					return tt.results[attempt-1]
				})

				if tt.expectedErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, tt.attempts, try)

				delay := time.Now().UTC().Sub(start)
				require.Equal(t, tt.totalDelay, delay)
			})
		})
	}
}
