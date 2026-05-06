package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	bqclient "github.com/estuary/connectors/go/capture/bigquery/client"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

var (
	testCredentialsPath  = flag.String("creds_path", "~/.config/gcloud/application_default_credentials.json", "Path to BigQuery credentials JSON used by integration tests")
	testProjectID        = flag.String("project_id", "bigquery-public-data", "BigQuery project ID owning the dataset under test")
	testDataset          = flag.String("test_dataset", "ga4_obfuscated_sample_ecommerce", "BigQuery dataset for integration tests")
	testBillingProjectID = flag.String("billing_project_id", "", "BigQuery project for billing and job execution. Required when project_id points at a dataset the credentials cannot create jobs in (e.g. bigquery-public-data).")
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
			// First poll: PrimaryThrough is "" so every live-window
			// table satisfies T > PrimaryThrough and gets enqueued as
			// primary. Endpoints-only mode only kicks in on subsequent
			// polls once PrimaryThrough has advanced.
			Name:               "initial_backfill_endpoints_only",
			Initial:            streamState{},
			Suffixes:           []string{"20260101", "20260102", "20260503", "20260504", "20260505", "20260506"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260506", "20260505", "20260504", "20260503", "20260102", "20260101"},
			WantPrimaryThrough: "20260506",
			WantFinalThrough:   "20260503",
			WantAdded:          6,
		},
		{
			// First poll, intermediate mode: every live-window table is
			// enqueued as primary too.
			Name:                "initial_backfill_intermediate",
			Initial:             streamState{},
			Suffixes:            []string{"20260101", "20260503", "20260504", "20260505", "20260506"},
			WindowDays:          windowDays,
			CaptureIntermediate: true,
			WantPending:         []string{"20260506", "20260505", "20260504", "20260503", "20260101"},
			WantPrimaryThrough:  "20260506",
			WantFinalThrough:    "20260503",
			WantAdded:           5,
		},
		{
			// Steady state, endpoint-only: window slid by one. Only the
			// newly-oldest (rotated final) and the newly-newest (new
			// primary) get enqueued; previously-primary'd intermediates
			// are skipped.
			Name: "steady_state_window_advances_by_one",
			Initial: streamState{
				PrimaryThrough: "20260506",
				FinalThrough:   "20260503",
			},
			Suffixes:           []string{"20260101", "20260503", "20260504", "20260505", "20260506", "20260507"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260507", "20260504"},
			WantPrimaryThrough: "20260507",
			WantFinalThrough:   "20260504",
			WantAdded:          2,
		},
		{
			// MinDate filter: tables strictly before the cutoff are
			// silently dropped, but FinalThrough still advances past
			// window_oldest so future polls don't reconsider them.
			// Live-window tables still get primary'd on the first poll
			// (because PrimaryThrough = "").
			Name:               "min_date_filters_old_tables",
			Initial:            streamState{},
			Suffixes:           []string{"20250101", "20250601", "20260503", "20260504", "20260505", "20260506"},
			WindowDays:         windowDays,
			MinDate:            "20260101",
			WantPending:        []string{"20260506", "20260505", "20260504", "20260503"}, // 20250* dropped
			WantPrimaryThrough: "20260506",
			WantFinalThrough:   "20260503",
			WantAdded:          4,
		},
		{
			// Post-pause catch-up: PrimaryThrough is far behind the
			// dataset's current state. Tables between the old
			// FinalThrough and new window_oldest are finalized backlog;
			// tables in the new live window are all > PrimaryThrough
			// so they all get primary captures (Option B falls out).
			Name: "post_pause_catchup",
			Initial: streamState{
				PrimaryThrough: "20260201",
				FinalThrough:   "20260131",
			},
			Suffixes:           []string{"20260131", "20260201", "20260403", "20260601", "20260602", "20260603", "20260604"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260604", "20260603", "20260602", "20260601", "20260403", "20260201"},
			WantPrimaryThrough: "20260604",
			WantFinalThrough:   "20260601",
			WantAdded:          6,
		},
		{
			// Dataset smaller than window_days: window_oldest clamps
			// to the dataset's oldest suffix.
			Name:               "small_dataset_clamps_window",
			Initial:            streamState{},
			Suffixes:           []string{"20260504", "20260505", "20260506"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260506", "20260505", "20260504"},
			WantPrimaryThrough: "20260506",
			WantFinalThrough:   "20260504",
			WantAdded:          3,
		},
		{
			// Already-pending suffixes are not duplicated.
			Name: "dedup_against_pending",
			Initial: streamState{
				Pending:        []string{"20260506"},
				PrimaryThrough: "20260506",
				FinalThrough:   "20260502",
			},
			Suffixes:           []string{"20260503", "20260504", "20260505", "20260506", "20260507"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260507", "20260506", "20260504", "20260503"},
			WantPrimaryThrough: "20260507",
			WantFinalThrough:   "20260504",
			WantAdded:          3,
		},
		{
			// If a polling cycle wants to re-enqueue the suffix
			// currently being read out, Current is abandoned. Here
			// 20260504 is the still-being-primary'd table that's
			// just rotated to window_oldest position, so it's
			// re-enqueued as a final capture.
			Name: "abandons_current_on_re_enqueue",
			Initial: streamState{
				Current:        &currentTable{Suffix: "20260504"},
				PrimaryThrough: "20260507",
				FinalThrough:   "20260503",
			},
			Suffixes:           []string{"20260503", "20260504", "20260505", "20260506", "20260507", "20260508"},
			WindowDays:         windowDays,
			WantPending:        []string{"20260508", "20260505", "20260504"},
			WantPrimaryThrough: "20260508",
			WantFinalThrough:   "20260505",
			WantAdded:          3,
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
			require.Equal(t, tc.WantAdded, added, "added count")
			require.Equal(t, tc.WantPending, state.Pending, "pending list (descending)")
			require.Equal(t, tc.WantPrimaryThrough, state.PrimaryThrough, "primary_through")
			require.Equal(t, tc.WantFinalThrough, state.FinalThrough, "final_through")
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
		ProjectID: *testProjectID,
		Dataset:   *testDataset,
		Credentials: &bqclient.Credentials{
			AuthType: bqclient.CredentialsJSON,
			CredentialsJSONConfig: bqclient.CredentialsJSONConfig{
				CredentialsJSON: string(credBytes),
			},
		},
		Advanced: advancedConfig{
			BillingProjectID: *testBillingProjectID,
		},
	}
}

// TestDiscoverPublicSample exercises Discover against the public GA4 sample
// dataset. Requires real BigQuery credentials and TEST_DATABASE=yes. The
// public sample only has events_YYYYMMDD tables, so we expect exactly one
// binding (events).
func TestDiscoverPublicSample(t *testing.T) {
	cfg := testDiscoveryConfig(t)
	cs := &st.CaptureSpec{
		Driver:       ga4Driver,
		EndpointSpec: cfg,
		Validator:    &st.SortedCaptureValidator{IncludeSourcedSchemas: true, PrettyDocuments: true},
		Sanitizers:   make(map[string]*regexp.Regexp),
	}
	bindings := cs.Discover(context.Background(), t)
	require.Len(t, bindings, 1, "expected exactly one binding (events) for the public sample dataset")
	require.Equal(t, *testDataset+"/events", bindings[0].RecommendedName)

	var res Resource
	require.NoError(t, json.Unmarshal(bindings[0].ResourceConfigJson, &res))
	require.Equal(t, *testDataset, res.Dataset)
	require.Equal(t, StreamEvents, res.StreamType)
	require.Equal(t, []string{"/event_timestamp", "/event_name", "/user_pseudo_id", "/event_bundle_sequence_id"}, bindings[0].Key)

	// Verify the schema includes _meta and the key columns.
	var schema map[string]any
	require.NoError(t, json.Unmarshal(bindings[0].DocumentSchemaJson, &schema))
	props, ok := schema["properties"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, props, "_meta")
	require.Contains(t, props, "event_timestamp")
	require.Contains(t, props, "event_name")
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
