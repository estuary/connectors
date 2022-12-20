package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var testApiKey = flag.String(
	"api_key",
	"",
	"Alpaca API Key to use for tests",
)
var testApiSecret = flag.String(
	"api_secret",
	"",
	"Alpaca API Key secret to use for tests",
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

// The remaining 2 tests are end-to-end tests that will interact with the live Alpaca API and assert
// the correctness of the results of the capture's backfilling capability. They will not be run
// automatically as part of CI due to how long they take to run, but can be run manually as desired.

// Mapping of binding name to the ticker symbols it should capture.
type bindingsMapping map[string]string

func TestCaptureMultipleBindings(t *testing.T) {
	start, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339Nano, "2022-12-19T14:10:00Z")
	require.NoError(t, err)

	var capture = captureSpec(t, map[string]string{
		"first":  "AAPL,MSFT",  // Both have trades in this time period.
		"second": "AMZN,BRK-B", // Only AMZN has trades in this time period.
		"third":  "UNH,JNJ",    // Neither has trades in this time period.
	}, start, end, &st.SortedCaptureValidator{})

	var captureCtx, cancelCapture = context.WithCancel(context.Background())

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

	capture := captureSpec(t, map[string]string{
		"first":  "AAPL,MSFT",
		"second": "GOOG,AMZN",
	}, start, end, &st.WatchdogValidator{
		Inner:         &st.ChecksumValidator{},
		WatchdogTimer: wdt,
		ResetPeriod:   quiescentTimeout,
	})

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

func captureSpec(t testing.TB, bMappings bindingsMapping, start, end time.Time, validator st.CaptureValidator) *st.CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var endpointSpec = &config{
		ApiKey:    *testApiKey,
		ApiSecret: *testApiSecret,
		Advanced: advancedConfig{
			IsFreePlan: true,
		},
	}

	return &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: endpointSpec,
		Bindings:     makeBindings(t, bMappings, start, end),
		Validator:    validator,
		Sanitizers:   make(map[string]*regexp.Regexp),
	}
}

func makeBindings(t testing.TB, bMappings bindingsMapping, start, end time.Time) []*flow.CaptureSpec_Binding {
	var bindings []*flow.CaptureSpec_Binding
	for name, symbols := range bMappings {
		spec := resource{
			Name:      name,
			StartDate: start,
			Feed:      "iex",
			Currency:  "usd",
			Symbols:   symbols,
			Advanced: advancedResourceConfig{
				StopDate:        end,
				DisableRealTime: true,
			},
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
