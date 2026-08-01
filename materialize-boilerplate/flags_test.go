package boilerplate

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/estuary/connectors/go/common"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type flagsTestConfig struct {
	raw string
}

func (c flagsTestConfig) Validate() error          { return nil }
func (c flagsTestConfig) DefaultNamespace() string { return "" }
func (c flagsTestConfig) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.raw, map[string]common.FlagDefault{
		"fixed_on":  common.FlagEnabled,
		"fixed_off": common.FlagDisabled,
		"gated":     common.FlagEnabledForTasksCreatedAfter("2026-06-10"),
	}
}

func TestResolveFlags(t *testing.T) {
	for _, tt := range []struct {
		name      string
		cfg       flagsTestConfig
		createdAt string
		wantGated bool
	}{
		{
			name:      "task created before the cutoff resolves gated flag off",
			createdAt: "2026-01-01",
			wantGated: false,
		},
		{
			name:      "task created after the cutoff resolves gated flag on",
			createdAt: "2026-07-01",
			wantGated: true,
		},
		{
			// A brand-new task's creation isn't committed during its first
			// build, so the runtime has no date to stamp onto the spec.
			name:      "unstamped creation date resolves gated flag on",
			createdAt: "",
			wantGated: true,
		},
		{
			name:      "explicit config entry wins over the cutoff",
			cfg:       flagsTestConfig{raw: "no_gated"},
			createdAt: "2026-07-01",
			wantGated: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			createdAt, err := common.ParseCreatedAt(tt.createdAt)
			require.NoError(t, err)
			flags := resolveFlags(tt.cfg, createdAt)
			require.Equal(t, tt.wantGated, flags["gated"])
			require.True(t, flags["fixed_on"])
			require.False(t, flags["fixed_off"])
		})
	}
}

func TestSpecCreatedAt(t *testing.T) {
	for _, spec := range []*pf.MaterializationSpec{nil, {}} {
		createdAt, err := specCreatedAt(spec)
		require.NoError(t, err)
		require.True(t, createdAt.IsBrandNew())
	}

	createdAt, err := specCreatedAt(&pf.MaterializationSpec{CreatedAt: "2026-07-01"})
	require.NoError(t, err)
	require.Equal(t, "2026-07-01", createdAt.String())

	// A spec carrying a date the connector cannot place fails the RPC rather
	// than resolving flags from a value nobody understood.
	_, err = specCreatedAt(&pf.MaterializationSpec{CreatedAt: "not-a-date"})
	require.ErrorContains(t, err, `task creation date "not-a-date"`)
}

// TestUnparseableCreatedAtFailsRPCs verifies that a spec carrying a creation
// date the connector cannot parse fails the RPC, rather than being resolved
// around and silently giving the task the behavior of the wrong era.
func TestUnparseableCreatedAtFailsRPCs(t *testing.T) {
	ctx := context.Background()
	spec := loadMaterializerSpec(t, "base.flow.proto")
	spec.CreatedAt = "6/10/2026"
	is := testInfoSchemaFromSpec(t, spec, func(in string) string { return in })
	fn := makeTestMaterializerFn(&testCalls{}, is)

	_, err := RunValidate(ctx, &pm.Request_Validate{
		Name:                spec.Name,
		ConnectorType:       spec.ConnectorType,
		ConfigJson:          spec.ConfigJson,
		LastMaterialization: spec,
	}, fn)
	require.ErrorContains(t, err, `task creation date "6/10/2026"`)

	_, err = RunApply(ctx, &pm.Request_Apply{Materialization: spec, LastMaterialization: spec}, fn)
	require.ErrorContains(t, err, `task creation date "6/10/2026"`)

	_, _, _, err = RunNewTransactor(ctx, pm.Request_Open{
		Materialization: spec,
		Version:         "aVersion",
		Range:           &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}, nil, fn)
	require.ErrorContains(t, err, `task creation date "6/10/2026"`)
}

// TestRunApplyRecordsFlagPinsInConfig verifies that Apply emits a configUpdate
// event restating the endpoint config with the resolved value of a cutoff-gated
// flag made explicit, and that the recorded value matches how the connector
// itself resolved the flag from the task's creation date.
func TestRunApplyRecordsFlagPinsInConfig(t *testing.T) {
	ctx := context.Background()

	// Stand in for the config encryption service by echoing the config back, so
	// the emitted event carries an inspectable document.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Config json.RawMessage `json:"config"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.Write(body.Config)
	}))
	defer server.Close()
	t.Setenv("ENCRYPTION_URL", server.URL)

	// Connector events are only emitted with the runtime's JSON log format.
	var logs bytes.Buffer
	logger := log.StandardLogger()
	priorFormatter, priorOut := logger.Formatter, logger.Out
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetOutput(&logs)
	t.Cleanup(func() { logger.SetFormatter(priorFormatter); logger.SetOutput(priorOut) })

	for _, tt := range []struct {
		name      string
		createdAt string
		want      string
	}{
		{"task predating the cutoff records the disabled form", "2020-01-01", "no_test_gated"},
		{"task postdating the cutoff records the enabled form", "2030-01-01", "test_gated"},
		{"brand new task records the enabled form", "", "test_gated"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()
			spec := loadMaterializerSpec(t, "base.flow.proto")
			spec.CreatedAt = tt.createdAt
			is := testInfoSchemaFromSpec(t, spec, func(in string) string { return in })

			var gotFlags map[string]bool
			capturingFn := func(
				ctx context.Context,
				materializationName string,
				endpointConfig testEndpointConfiger,
				featureFlags map[string]bool,
			) (Materializer[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper], error) {
				gotFlags = featureFlags
				return &testMaterializer{calls: &testCalls{}, is: is}, nil
			}

			_, err := RunApply(ctx, &pm.Request_Apply{
				Materialization:     spec,
				LastMaterialization: spec,
			}, capturingFn, WithConfigUpdates(json.RawMessage(`{}`), []string{"config", "advanced", "feature_flags"}))
			require.NoError(t, err)

			recorded := recordedFeatureFlags(t, logs.Bytes())
			require.Equal(t, tt.want, recorded, "recorded feature flags")
			// The recorded value must agree with the behavior of this very Apply.
			require.Equal(t, gotFlags["test_gated"], recorded == "test_gated")
		})
	}

	t.Run("a flag already present in the config is not re-recorded", func(t *testing.T) {
		logs.Reset()
		spec := loadMaterializerSpec(t, "base.flow.proto")
		spec.CreatedAt = "2030-01-01"
		cfg, err := common.SetJSONProperty(spec.ConfigJson, []string{"config", "advanced", "feature_flags"}, "no_test_gated")
		require.NoError(t, err)
		spec.ConfigJson = cfg
		is := testInfoSchemaFromSpec(t, spec, func(in string) string { return in })

		_, err = RunApply(ctx, &pm.Request_Apply{
			Materialization:     spec,
			LastMaterialization: spec,
		}, makeTestMaterializerFn(&testCalls{}, is),
			WithConfigUpdates(json.RawMessage(`{}`), []string{"config", "advanced", "feature_flags"}))
		require.NoError(t, err)
		require.Empty(t, recordedFeatureFlags(t, logs.Bytes()), "no configUpdate should be emitted")
	})
}

// recordedFeatureFlags returns the feature flags string of the config carried by
// a configUpdate event in the given JSON log lines, or "" if there is no such
// event.
func recordedFeatureFlags(t *testing.T, logs []byte) string {
	t.Helper()

	for _, line := range bytes.Split(logs, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var entry struct {
			EventType string `json:"eventType"`
			Config    struct {
				Config struct {
					Advanced struct {
						FeatureFlags string `json:"feature_flags"`
					} `json:"advanced"`
				} `json:"config"`
			} `json:"config"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue // Not a structured line we care about.
		}
		if entry.EventType == "configUpdate" {
			return entry.Config.Config.Advanced.FeatureFlags
		}
	}
	return ""
}

func TestParseFlags(t *testing.T) {
	// Without a spec to take a creation date from, date-gated flags resolve as
	// they would for a brand-new task.
	require.True(t, ParseFlags(flagsTestConfig{})["gated"])
	require.False(t, ParseFlags(flagsTestConfig{raw: "no_gated"})["gated"])
}
