package boilerplate

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/estuary/connectors/go/common"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestRunNewTransactorRecordsSkipHostKeyVerification checks that Open sets
// `skipHostKeyVerification` for a tunnel with no `knownHosts`. It also checks
// that the feature flag configUpdate from the same Open keeps that setting.
func TestRunNewTransactorRecordsSkipHostKeyVerification(t *testing.T) {
	ctx := context.Background()

	var logs bytes.Buffer
	logger := log.StandardLogger()
	priorFormatter, priorOut := logger.Formatter, logger.Out
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetOutput(&logs)
	t.Cleanup(func() { logger.SetFormatter(priorFormatter); logger.SetOutput(priorOut) })

	tunnelSpec := func(t *testing.T) *pf.MaterializationSpec {
		spec := loadMaterializerSpec(t, "base.flow.proto")
		spec.CreatedAt = "2030-01-01"
		cfg, err := common.SetJSONProperty(spec.ConfigJson, []string{"networkTunnel"}, map[string]any{
			"sshForwarding": map[string]any{"sshEndpoint": "ssh://user@bastion.example.com:2222", "privateKey": "key"},
		})
		require.NoError(t, err)
		spec.ConfigJson = cfg
		return spec
	}

	// overlays returns the sops overlay of each configUpdate event in the logs.
	overlays := func(t *testing.T) []string {
		var out []string
		for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
			var entry struct {
				EventType string `json:"eventType"`
				Config    struct {
					Sops struct {
						Overlay json.RawMessage `json:"overlay"`
					} `json:"sops"`
				} `json:"config"`
			}
			if json.Unmarshal(line, &entry) == nil && entry.EventType == "configUpdate" {
				out = append(out, string(entry.Config.Sops.Overlay))
			}
		}
		return out
	}

	t.Run("set without feature flag recording", func(t *testing.T) {
		logs.Reset()
		spec := tunnelSpec(t)
		is := testInfoSchemaFromSpec(t, spec, func(in string) string { return in })

		_, _, _, err := RunNewTransactor(ctx, pm.Request_Open{
			Materialization:  spec,
			Version:          "aVersion",
			Range:            &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
			SealedConfigJson: json.RawMessage(sealedTestConfig),
		}, nil, makeTestMaterializerFn(&testCalls{}, is))
		require.NoError(t, err)

		got := overlays(t)
		require.Len(t, got, 1)
		require.JSONEq(t, `{"networkTunnel":{"sshForwarding":{"skipHostKeyVerification":true}}}`, got[0])
	})

	t.Run("feature flag update on the same Open keeps it", func(t *testing.T) {
		logs.Reset()
		spec := tunnelSpec(t)
		is := testInfoSchemaFromSpec(t, spec, func(in string) string { return in })

		_, _, _, err := runTestTransactor(ctx, spec, json.RawMessage(sealedTestConfig), makeTestMaterializerFn(&testCalls{}, is))
		require.NoError(t, err)

		got := overlays(t)
		require.Len(t, got, 2)
		require.JSONEq(t, `{"networkTunnel":{"sshForwarding":{"skipHostKeyVerification":true}}}`, got[0])
		require.JSONEq(t, `{"config":{"advanced":{"feature_flags":"test_gated"}},"networkTunnel":{"sshForwarding":{"skipHostKeyVerification":true}}}`, got[1])
	})
}
