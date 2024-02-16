package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPeriodicSchedule(t *testing.T) {
	for _, tc := range []struct {
		Schedule string
		After    string
		Expect   string
	}{
		{"1h", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"2h", "2024-02-15T12:34:56Z", "2024-02-15T14:34:56Z"},
		{"6h", "2024-02-15T19:34:56Z", "2024-02-16T01:34:56Z"},
		{"24h", "2024-02-15T19:34:56Z", "2024-02-16T19:34:56Z"},
		{"168h", "2024-02-15T19:34:56Z", "2024-02-22T19:34:56Z"},
		{"30m", "2024-02-15T19:34:56Z", "2024-02-15T20:04:56Z"},
		{"10m", "2024-02-15T19:34:56Z", "2024-02-15T19:44:56Z"},
		{"1m", "2024-02-15T19:34:56Z", "2024-02-15T19:35:56Z"},
		{"30s", "2024-02-15T19:34:56Z", "2024-02-15T19:35:26Z"},
		{"10s", "2024-02-15T19:34:56Z", "2024-02-15T19:35:06Z"},
		{"1s", "2024-02-15T19:34:56Z", "2024-02-15T19:34:57Z"},
	} {
		sched, err := Parse(tc.Schedule)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, tc.After)
		require.NoError(t, err)
		var ts = sched.Next(after)
		require.Equal(t, tc.Expect, ts.Format(time.RFC3339))
	}
}

func TestDailySchedule(t *testing.T) {
	for _, tc := range []struct {
		Schedule string
		After    string
		Expect   string
	}{
		{"daily at 6:00Z", "2024-02-15T01:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T06:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 6:00Z", "2024-02-15T07:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T01:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T05:00:00Z", "2024-02-15T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T06:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 06:00Z", "2024-02-15T07:00:00Z", "2024-02-16T06:00:00Z"},
		{"daily at 13:55Z", "2024-02-15T01:00:00Z", "2024-02-15T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:54:00Z", "2024-02-15T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:55:00Z", "2024-02-16T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T13:56:00Z", "2024-02-16T13:55:00Z"},
		{"daily at 13:55Z", "2024-02-15T23:00:00Z", "2024-02-16T13:55:00Z"},
	} {
		sched, err := Parse(tc.Schedule)
		require.NoError(t, err)
		after, err := time.Parse(time.RFC3339, tc.After)
		require.NoError(t, err)
		var ts = sched.Next(after)
		require.Equal(t, tc.Expect, ts.Format(time.RFC3339))
	}
}
