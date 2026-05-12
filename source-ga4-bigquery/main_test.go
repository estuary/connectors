package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	bqclient "github.com/estuary/connectors/go/capture/bigquery/client"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

var (
	testCredentialsPath = flag.String("creds_path", "~/.config/gcloud/application_default_credentials.json", "Path to BigQuery credentials JSON used by integration tests")
	testProjectID       = flag.String("project_id", "estuary-theatre", "Operator's GCP project. For public-sample tests this is the billing project. For future stimulus-response tests it would also be the project hosting the test dataset.")
)

// Identifiers of the GA4 obfuscated public sample dataset, which is the
// fixed read-only target of integration tests in this package. Other
// connectors with a stimulus-response test pattern would take operator-
// supplied dataset identifiers via flags, but here the dataset is part of
// the test itself.
const (
	publicSampleProjectID = "bigquery-public-data"
	publicSampleDataset   = "ga4_obfuscated_sample_ecommerce"
)

// TestSpec verifies the connector's response to the Spec RPC against a snapshot.
func TestSpec(t *testing.T) {
	response, err := ga4Driver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

// TestResourceValidate exercises the Resource.Validate logic.
func TestResourceValidate(t *testing.T) {
	for _, tc := range []struct {
		Name      string
		Resource  Resource
		WantError string
	}{
		{Name: "valid_events", Resource: Resource{Dataset: "ds", StreamType: StreamEvents}, WantError: ""},
		{Name: "valid_users", Resource: Resource{Dataset: "ds", StreamType: StreamUsers}, WantError: ""},
		{Name: "valid_pseudonymous_users", Resource: Resource{Dataset: "ds", StreamType: StreamPseudonymousUsers}, WantError: ""},
		{Name: "missing_dataset", Resource: Resource{StreamType: StreamEvents}, WantError: "missing 'dataset'"},
		{Name: "invalid_stream_type", Resource: Resource{Dataset: "ds", StreamType: "garbage"}, WantError: `invalid 'stream_type' "garbage"`},
		{Name: "empty_stream_type", Resource: Resource{Dataset: "ds", StreamType: ""}, WantError: `invalid 'stream_type' ""`},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			err := tc.Resource.Validate()
			if tc.WantError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.WantError)
			}
		})
	}
}

// TestEvaluatePollCycle exercises the pure-logic core of a polling cycle:
// given dataset suffixes and prior state, what gets enqueued and how do
// scheduling cursors advance?
func TestEvaluatePollCycle(t *testing.T) {
	const windowDays = 4

	for _, tc := range []struct {
		Name                string
		Initial             streamState
		Suffixes            []string
		WindowDays          int
		CaptureIntermediate bool
		MinDate             string
		WantPending         []string
		WantPrimaryThrough  string
		WantFinalThrough    string
		WantAdded           int
		WantCurrentNil      bool
	}{
		{
			// First poll: PrimaryThrough is "" so every live-window table satisfies T > PrimaryThrough and gets enqueued as
			// primary. Endpoints-only mode only kicks in on subsequent polls once PrimaryThrough has advanced.
			Name:               "initial_backfill_endpoints_only",
			Initial:            streamState{},
			Suffixes:           []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260106", "20260105", "20260104", "20260103", "20260102", "20260101"},
			WantPrimaryThrough: "20260106",
			WantFinalThrough:   "20260103",
			WantAdded:          6,
		},
		{
			// First poll, intermediate mode: every live-window table is enqueued as primary too.
			Name:                "initial_backfill_intermediate",
			Initial:             streamState{},
			Suffixes:            []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106"},
			WindowDays:          windowDays,
			CaptureIntermediate: true,
			WantPending:         []string{"20260106", "20260105", "20260104", "20260103", "20260102", "20260101"},
			WantPrimaryThrough:  "20260106",
			WantFinalThrough:    "20260103",
			WantAdded:           6,
		},
		{
			// Steady state, endpoint-only: window slid by one. Only the newly-oldest (rotated final) and the newly-newest (new
			// primary) get enqueued; previously-primary'd intermediates are skipped.
			Name: "steady_state_window_advances_by_one",
			Initial: streamState{
				PrimaryThrough: "20260106",
				FinalThrough:   "20260103",
			},
			Suffixes:           []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106", "20260107"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260107", "20260104"},
			WantPrimaryThrough: "20260107",
			WantFinalThrough:   "20260104",
			WantAdded:          2,
		},
		{
			// MinDate filter: tables strictly before the cutoff are silently dropped, but FinalThrough still advances past
			// window_oldest so future polls don't reconsider them. Live-window tables still get primary'd on the first poll
			// (because PrimaryThrough = "").
			Name:               "min_date_filters_old_tables",
			Initial:            streamState{},
			Suffixes:           []string{"20250101", "20250203", "20260503", "20260504", "20260505", "20260506"},
			WindowDays:         windowDays,
			MinDate:            "20260101",
			WantPending:        []string{"20260506", "20260505", "20260504", "20260503"}, // 2025 skipped
			WantPrimaryThrough: "20260506",
			WantFinalThrough:   "20260503",
			WantAdded:          4,
		},
		{
			// Post-pause catch-up: PrimaryThrough is far behind the dataset's current state. Tables between the old
			// FinalThrough and new window_oldest are finalized backlog; tables in the new live window are all > PrimaryThrough
			// so they all get primary captures (Option B falls out).
			Name: "post_pause_catchup",
			Initial: streamState{
				PrimaryThrough: "20260104",
				FinalThrough:   "20260101",
			},
			Suffixes:           []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106", "20260107"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260107", "20260106", "20260105", "20260104", "20260103", "20260102"},
			WantPrimaryThrough: "20260107",
			WantFinalThrough:   "20260104",
			WantAdded:          6,
		},
		{
			// Dataset smaller than window_days (e.g., freshly-enabled GA4 export): we don't yet have enough history to
			// know where the live window's far edge is, so finalization is deferred. Every table gets a primary capture,
			// FinalThrough stays empty, and the oldest table will roll into a final capture later once the dataset has
			// grown to window_days tables.
			Name:               "small_dataset_defers_finalization",
			Initial:            streamState{},
			Suffixes:           []string{"20260504", "20260505", "20260506"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260506", "20260505", "20260504"},
			WantPrimaryThrough: "20260506",
			WantFinalThrough:   "",
			WantAdded:          3,
		},
		{
			// Already-pending suffixes are not duplicated.
			Name: "dedup_against_pending",
			Initial: streamState{
				Pending:        []string{"20260104"},
				PrimaryThrough: "20260106",
				FinalThrough:   "20260103",
			},
			Suffixes:           []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106", "20260107"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260107", "20260104"},
			WantPrimaryThrough: "20260107",
			WantFinalThrough:   "20260104",
			WantAdded:          1,
		},
		{
			// If a polling cycle wants to re-enqueue the suffix currently being read out, Current is abandoned. Here
			// 20260103 is the still-being-primary'd table that's just rotated to window_oldest position, so it's
			// re-enqueued as a final capture.
			Name: "abandons_current_on_re_enqueue",
			Initial: streamState{
				Current:        &currentTable{Suffix: "20260104"},
				PrimaryThrough: "20260106",
				FinalThrough:   "20260103",
			},
			Suffixes:           []string{"20260101", "20260102", "20260103", "20260104", "20260105", "20260106", "20260107"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260107", "20260104"},
			WantPrimaryThrough: "20260107",
			WantFinalThrough:   "20260104",
			WantAdded:          2,
			WantCurrentNil:     true,
		},
		{
			// Empty dataset: function is a no-op.
			Name:        "empty_dataset_noop",
			Initial:     streamState{},
			Suffixes:    nil,
			WindowDays:  windowDays,
			WantPending: nil,
			WantAdded:   0,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			state := tc.Initial
			added := evaluatePollCycle(&state, tc.Suffixes, tc.WindowDays, tc.CaptureIntermediate, tc.MinDate)
			require.Equal(t, tc.WantPending, state.Pending, "pending list (descending)")
			require.Equal(t, tc.WantPrimaryThrough, state.PrimaryThrough, "primary_through")
			require.Equal(t, tc.WantFinalThrough, state.FinalThrough, "final_through")
			require.Equal(t, tc.WantAdded, added, "added count")
			if tc.WantCurrentNil {
				require.Nil(t, state.Current, "Current should have been abandoned")
			}
		})
	}
}

// TestEvaluatePollCycleIdempotent verifies that repeated polling cycles
// without dataset changes produce no new enqueues.
func TestEvaluatePollCycleIdempotent(t *testing.T) {
	suffixes := []string{"20260101", "20260503", "20260504", "20260505", "20260506"}
	state := streamState{}

	added := evaluatePollCycle(&state, suffixes, 4, false, "")
	require.Greater(t, added, 0, "first poll should enqueue something")

	state.Pending = nil // simulate readout drainage
	added = evaluatePollCycle(&state, suffixes, 4, false, "")
	require.Equal(t, 0, added, "no new enqueues with unchanged dataset")
	require.Empty(t, state.Pending)
}

// TestEvaluatePollCycleWarmup walks a freshly-enabled GA4 export through
// successive polling cycles. While the dataset has fewer than window_days
// tables we don't yet know where the live window's far edge is, so every
// table gets a primary capture and FinalThrough stays empty. The moment
// the dataset reaches window_days tables, normal sliding-window scheduling
// engages and the oldest table rolls into its final capture on that same
// poll.
func TestEvaluatePollCycleWarmup(t *testing.T) {
	const windowDays = 4
	var state = streamState{}

	// Day 1: One table, primary capture only.
	var tables = []string{"20260510"}
	evaluatePollCycle(&state, tables, windowDays, false, "")
	require.Equal(t, []string{"20260510"}, state.Pending)
	require.Equal(t, "20260510", state.PrimaryThrough)
	require.Equal(t, "", state.FinalThrough)
	state.Pending = nil // simulate readout

	// Day 2: Two tables, primary capture only.
	tables = append(tables, "20260511")
	evaluatePollCycle(&state, tables, windowDays, false, "")
	require.Equal(t, []string{"20260511"}, state.Pending)
	require.Equal(t, "20260511", state.PrimaryThrough)
	require.Equal(t, "", state.FinalThrough)
	state.Pending = nil

	// Day 3: Three tables, primary capture only.
	tables = append(tables, "20260512")
	evaluatePollCycle(&state, tables, windowDays, false, "")
	require.Equal(t, []string{"20260512"}, state.Pending)
	require.Equal(t, "20260512", state.PrimaryThrough)
	require.Equal(t, "", state.FinalThrough) // still no FinalThrough up to here
	state.Pending = nil

	// Day 4: Four tables, the live window is now well defined, primary capture
	// the newest table and final capture of the oldest in the window.
	tables = append(tables, "20260513")
	evaluatePollCycle(&state, tables, windowDays, false, "")
	require.Equal(t, []string{"20260513", "20260510"}, state.Pending)
	require.Equal(t, "20260513", state.PrimaryThrough)
	require.Equal(t, "20260510", state.FinalThrough)
}

// TestBuildTableQuery verifies that per-table queries are constructed
// correctly with and without a cursor for each stream type.
func TestBuildTableQuery(t *testing.T) {
	for _, tc := range []struct {
		Name       string
		StreamType StreamType
		Cursor     any
		Want       string
	}{
		{
			Name:       "events_no_cursor",
			StreamType: StreamEvents,
			Cursor:     nil,
			Want:       "SELECT * FROM `test-project`.`analytics_1234`.`events_20260501` ORDER BY `event_timestamp`",
		},
		{
			Name:       "events_with_cursor",
			StreamType: StreamEvents,
			Cursor:     int64(1714521600000000),
			Want:       "SELECT * FROM `test-project`.`analytics_1234`.`events_20260501` WHERE `event_timestamp` > @cursor ORDER BY `event_timestamp`",
		},
		{
			Name:       "users_with_cursor",
			StreamType: StreamUsers,
			Cursor:     "u-abc",
			Want:       "SELECT * FROM `test-project`.`analytics_1234`.`users_20260501` WHERE `user_id` > @cursor ORDER BY `user_id`",
		},
		{
			Name:       "pseudonymous_users_no_cursor",
			StreamType: StreamPseudonymousUsers,
			Cursor:     nil,
			Want:       "SELECT * FROM `test-project`.`analytics_1234`.`pseudonymous_users_20260501` ORDER BY `pseudo_user_id`",
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			got := buildTableQuery("test-project", "analytics_1234", streams[tc.StreamType], "20260501", tc.Cursor)
			require.Equal(t, tc.Want, got)
		})
	}
}

// TestBuildQueryParams verifies cursor type binding (handles JSON-decoded
// float64 → int64 conversion).
func TestBuildQueryParams(t *testing.T) {
	for _, tc := range []struct {
		Name       string
		StreamType StreamType
		Cursor     any
		WantValue  any
	}{
		{Name: "nil_returns_no_params", StreamType: StreamEvents, Cursor: nil, WantValue: nil},
		{Name: "int64_cursor", StreamType: StreamEvents, Cursor: int64(123), WantValue: int64(123)},
		{Name: "float64_cursor_from_state", StreamType: StreamEvents, Cursor: float64(123), WantValue: int64(123)},
		{Name: "int_cursor", StreamType: StreamEvents, Cursor: 456, WantValue: int64(456)},
		{Name: "string_cursor", StreamType: StreamUsers, Cursor: "u-1", WantValue: "u-1"},
		{Name: "string_cursor_pseudo", StreamType: StreamPseudonymousUsers, Cursor: "p-1", WantValue: "p-1"},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			got := buildQueryParams(streams[tc.StreamType], tc.Cursor)
			if tc.WantValue == nil {
				require.Nil(t, got)
				return
			}
			require.Len(t, got, 1)
			require.Equal(t, "cursor", got[0].Name)
			require.Equal(t, tc.WantValue, got[0].Value)
		})
	}
}

// TestMarshalIncrementalCheckpoint verifies that incremental readout
// checkpoints patch only the in-progress table state.
func TestMarshalIncrementalCheckpoint(t *testing.T) {
	current := &currentTable{
		Suffix:    "20260501",
		Cursor:    int64(123),
		StartedAt: time.Unix(123, 0).UTC(),
		Index:     1000,
	}

	got, err := marshalIncrementalCheckpoint("binding-key", current)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"bindingStateV1": {
			"binding-key": {
				"current": {
					"cursor": 123,
					"row_id": 1000,
					"started_at": "1970-01-01T00:02:03Z",
					"suffix": "20260501"
				}
			}
		}
	}`, string(got))

	current.Cursor = nil
	got, err = marshalIncrementalCheckpoint("binding-key", current)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"bindingStateV1": {
			"binding-key": {
				"current": {
					"cursor": null,
					"row_id": 1000,
					"started_at": "1970-01-01T00:02:03Z",
					"suffix": "20260501"
				}
			}
		}
	}`, string(got))

	got, err = marshalIncrementalCheckpoint("binding-key", nil)
	require.Error(t, err)
	require.Nil(t, got)
}

// TestRecommendedName verifies the {dataset}/{stream_type} naming convention.
func TestRecommendedName(t *testing.T) {
	require.Equal(t, "analytics_1234/events", recommendedName("analytics_1234", StreamEvents))
	require.Equal(t, "analytics_1234/users", recommendedName("analytics_1234", StreamUsers))
	require.Equal(t, "ga4_obfuscated_sample_ecommerce/events", recommendedName("ga4_obfuscated_sample_ecommerce", StreamEvents))
	// Hyphens, underscores, and dots are preserved (matching sqlcapture).
	// Other characters are replaced with underscore. Letters are lowercased.
	require.Equal(t, "analytics-1234/events", recommendedName("Analytics-1234", StreamEvents))
	require.Equal(t, "weird_dataset_name/events", recommendedName("Weird Dataset Name", StreamEvents))
}

// testDiscoveryConfig builds a Config wired to the public sample dataset.
// Skips the test if TEST_DATABASE != "yes" or credentials are unavailable.
func testDiscoveryConfig(t testing.TB) *Config {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: TEST_DATABASE != \"yes\"", t.Name())
	}
	credPath := strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credBytes, err := os.ReadFile(credPath)
	require.NoError(t, err)

	return &Config{
		ProjectID: publicSampleProjectID,
		Dataset:   publicSampleDataset,
		Credentials: &bqclient.Credentials{
			AuthType: bqclient.CredentialsJSON,
			CredentialsJSONConfig: bqclient.CredentialsJSONConfig{
				CredentialsJSON: string(credBytes),
			},
		},
		Advanced: advancedConfig{
			BillingProjectID: *testProjectID,
		},
	}
}

// testBigQueryClient is a helper for tests that need a raw BigQuery client.
// Unused by the standard test suite; provided as a building block for
// follow-up tests that exercise actual data capture.
func testBigQueryClient(t testing.TB) *bigquery.Client {
	t.Helper()
	cfg := testDiscoveryConfig(t)
	bq, err := connectBigQuery(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { bq.Close() })
	return bq
}
