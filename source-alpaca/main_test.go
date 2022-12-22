package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	valid := config{
		ApiKeyID:     "something",
		ApiSecretKey: "something",
		Feed:         "iex",
		Symbols:      "AAPL,MSFT",
		StartDate:    time.Now(),
	}

	stopBeforeStart := valid
	stopBeforeStart.StartDate = time.Now()
	stopBeforeStart.Advanced.StopDate = stopBeforeStart.StartDate.Add(-1 * time.Minute)

	missingStart := valid
	missingStart.StartDate = time.Time{}

	wrongFeed := valid
	wrongFeed.Feed = "otherThing"

	minBackfillBiggerThanMaxBackfill := valid
	minBackfillBiggerThanMaxBackfill.Advanced.MaxBackfillInterval = "15m0s"
	minBackfillBiggerThanMaxBackfill.Advanced.MinBackfillInterval = "16m0s"

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
			ApiKeyID:     "something",
			ApiSecretKey: "something",
			Feed:         "iex",
			Symbols:      "AAPL,MSFT",
			StartDate:    time.Now(),
		}

		require.Equal(t, nil, conf.Validate())
		require.Equal(t, defaultMaxBackfillInterval, conf.effectiveMaxBackfillInterval)
		require.Equal(t, defaultMinBackfillInterval, conf.effectiveMinBackfillInterval)
	})
}

// The remaining end-to-end tests interact with the live Alpaca API and assert the correctness of
// the results from the capture's backfilling capability. They will not be run automatically as part
// of CI due to how long they take to run, but can be run manually as desired.

var testApiKey = flag.String(
	"api_key_id",
	"",
	"Alpaca API Key to use for tests",
)
var testApiSecret = flag.String(
	"api_secret_key",
	"",
	"Alpaca API secret key to use for tests",
)

func TestMain(m *testing.M) {
	flag.Parse()
	if level, err := log.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		log.SetLevel(level)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.SetFormatter(&log.JSONFormatter{
		DataKey:  "data",
		FieldMap: log.FieldMap{log.FieldKeyTime: "@ts"},
	})
	os.Exit(m.Run())
}

func TestCaptureMultipleBindings(t *testing.T) {
	start, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:10:00Z")
	require.NoError(t, err)

	capture := captureSpec(t,
		[]string{"first", "second"},
		// BRK-B does not have trades in this time period.
		[]string{"AAPL", "MSFT", "AMZN", "BRK-B"},
		start,
		end,
		&st.SortedCaptureValidator{},
	)

	captureCtx, cancelCapture := context.WithCancel(context.Background())

	// We need to use a relatively long shutdown delay, since the backfill can potentially cover
	// periods of time with no historical data.
	const shutdownDelay = 5000 * time.Millisecond
	var shutdownWatchdog *time.Timer

	capture.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
	})
	cupaloy.SnapshotT(t, capture.Summary())
	capture.Reset()
}

// TestLargeCapture tests a fairly large backfill over a week's time with high-volume symbols. The
// watchdog timeout may need to be increased based on specific network conditions.
func TestLargeCapture(t *testing.T) {
	start, err := time.Parse(time.RFC3339Nano, "2022-12-12T00:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339Nano, "2022-12-17T00:00:00Z")
	require.NoError(t, err)

	// Set up a watchdog timeout which will terminate the overall context after nothing has been
	// captured for 60 seconds. The WDT will be fed via the `WatchdogValidator` wrapper around the
	// actual capture output validator.
	const quiescentTimeout = 60 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	wdt := time.AfterFunc(quiescentTimeout, func() {
		log.WithField("timeout", quiescentTimeout.String()).Debug("capture watchdog timeout expired")
		cancel()
	})

	capture := captureSpec(t,
		[]string{"trades"},
		[]string{"AAPL", "MSFT", "GOOG", "AMZN"},
		start,
		end,
		&st.WatchdogValidator{
			Inner:         &st.ChecksumValidator{},
			WatchdogTimer: wdt,
			ResetPeriod:   quiescentTimeout,
		},
	)

	// Run the capture and verify the results. The capture is killed every 10s and then restarted
	// from the previous checkpoint, in order to exercise incremental backfill resuming behavior.
	// Since the backfill takes about 300s to complete this should restart about 30 times along the
	// way.
	for ctx.Err() == nil {
		ctx, cancel := context.WithCancel(ctx)
		time.AfterFunc(10*time.Second, cancel)
		capture.Capture(ctx, t, nil)
	}
	cupaloy.SnapshotT(t, capture.Summary())
	capture.Reset()
}

func captureSpec(t testing.TB, names []string, symbols []string, start, end time.Time, validator st.CaptureValidator) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	// Use credentials if provided by flags, otherwise use placeholders for tests that don't
	// interact with a real API.
	apiKey := "apiKey"
	apiSecret := "apiSecret"
	if *testApiKey != "" {
		apiKey = *testApiKey
	}
	if *testApiSecret != "" {
		apiSecret = *testApiSecret
	}

	endpointSpec := &config{
		ApiKeyID:     apiKey,
		ApiSecretKey: apiSecret,
		Feed:         "iex",
		Symbols:      strings.Join(symbols, ","),
		StartDate:    start,
		Advanced: advancedConfig{
			IsFreePlan:      true,
			StopDate:        end,
			DisableRealTime: true,
		},
	}

	return &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: endpointSpec,
		Bindings:     makeBindings(t, names, start, end),
		Validator:    validator,
		Sanitizers:   make(map[string]*regexp.Regexp),
	}
}

func makeBindings(t testing.TB, names []string, start, end time.Time) []*flow.CaptureSpec_Binding {
	var bindings []*flow.CaptureSpec_Binding
	for _, name := range names {
		spec := resource{
			Name: name,
		}

		specBytes, err := json.Marshal(spec)
		require.NoError(t, err)

		bindings = append(bindings, &flow.CaptureSpec_Binding{
			ResourceSpecJson: json.RawMessage(specBytes),
			ResourcePath:     []string{name},
		})
	}
	return bindings
}
