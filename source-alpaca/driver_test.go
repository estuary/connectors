package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pc.SpecRequest{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestDiscover(t *testing.T) {
	// Discover for this capture does not actually make any external API or database calls, so we
	// can always test it as part of the normal unit tests.
	t.Setenv("TEST_DATABASE", "yes")

	startDate, err := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z")
	require.NoError(t, err)
	endDate, err := time.Parse(time.RFC3339Nano, "2007-02-03T15:04:05.999999999Z")
	require.NoError(t, err)

	capture := captureSpec(t, map[string]string{}, startDate, endDate, nil)
	capture.VerifyDiscover(context.Background(), t, nil...)
}

func TestConfigValidate(t *testing.T) {
	valid := config{
		ApiKey:    "something",
		ApiSecret: "something",
		Feed:      "iex",
		Symbols:   "AAPL,MSFT",
		StartDate: time.Now(),
	}

	stopBeforeStart := valid
	stopBeforeStart.StartDate = time.Now()
	stopBeforeStart.Advanced.StopDate = stopBeforeStart.StartDate.Add(-1 * time.Minute)

	missingStart := valid
	missingStart.StartDate = time.Time{}

	wrongFeed := valid
	wrongFeed.Feed = "otherThing"

	minBackfillBiggerThanMaxBackfill := valid
	minBackfillBiggerThanMaxBackfill.Advanced.MaxBackfillInterval = time.Duration(1 * time.Minute)
	minBackfillBiggerThanMaxBackfill.Advanced.MinBackfillInterval = time.Duration(2 * time.Minute)

	backfillAndRealTimeDisabled := valid
	backfillAndRealTimeDisabled.Advanced.DisableBackfill = true
	backfillAndRealTimeDisabled.Advanced.DisableRealTime = true

	tests := []struct {
		name   string
		config config
		want   error
	}{
		{
			name:   "valid",
			config: valid,
			want:   nil,
		},
		{
			name:   "stop date before start date",
			config: stopBeforeStart,
			want:   fmt.Errorf("stop_date %s cannot be before start_date %s", stopBeforeStart.Advanced.StopDate, stopBeforeStart.StartDate),
		},
		{
			name:   "missing start date",
			config: missingStart,
			want:   fmt.Errorf("must provide a value for start_date"),
		},
		{
			name:   "invalid feed",
			config: wrongFeed,
			want:   fmt.Errorf("feed must be iex or sip, got %s", wrongFeed.Feed),
		},
		{
			name:   "min backfill duration too large",
			config: minBackfillBiggerThanMaxBackfill,
			want:   fmt.Errorf("min_backfill_interval of %s cannot be greater than max_backfill_interval of %s", minBackfillBiggerThanMaxBackfill.Advanced.MinBackfillInterval, minBackfillBiggerThanMaxBackfill.Advanced.MaxBackfillInterval),
		},
		{
			name:   "backfill and real time both disabled",
			config: backfillAndRealTimeDisabled,
			want:   fmt.Errorf("cannot disable both real time and backfill data collection"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.config.Validate())
		})
	}

	t.Run("default intervals are set correctly", func(t *testing.T) {
		conf := &config{
			ApiKey:    "something",
			ApiSecret: "something",
			Feed:      "iex",
			Symbols:   "AAPL,MSFT",
			StartDate: time.Now(),
		}

		require.Equal(t, nil, conf.Validate())
		require.Equal(t, defaultMaxBackfillInterval, conf.Advanced.MaxBackfillInterval)
		require.Equal(t, defaultMinBackfillInterval, conf.Advanced.MinBackfillInterval)
	})
}
