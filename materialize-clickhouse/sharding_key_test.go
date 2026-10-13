package connector

import (
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func validateReq(t *testing.T, cfg config, rc tableConfig, backfill uint32, lastSpec *pf.MaterializationSpec) *pm.Request_Validate {
	t.Helper()

	var spec = loadBaseSpec(t)
	return &pm.Request_Validate{
		Name:                spec.Name,
		ConnectorType:       pf.MaterializationSpec_IMAGE,
		ConfigJson:          mustMarshal(t, cfg),
		LastMaterialization: lastSpec,
		Bindings: []*pm.Request_Validate_Binding{{
			ResourceConfigJson: mustMarshal(t, rc),
			Collection:         spec.Bindings[0].Collection,
			Backfill:           backfill,
		}},
	}
}

func specWith(t *testing.T, cfg config, rc tableConfig, backfill uint32) *pf.MaterializationSpec {
	t.Helper()

	var spec = loadBaseSpec(t)
	spec.ConfigJson = mustMarshal(t, cfg)
	spec.Bindings[0].ResourceConfigJson = mustMarshal(t, rc)
	spec.Bindings[0].ResourcePath = []string{rc.Table}
	spec.Bindings[0].Backfill = backfill
	return spec
}

func incompatibleFields(resp *pm.Response_Validated, reason string) []string {
	var out []string
	for _, pc := range resp.Bindings[0].ProjectionConstraints {
		if pc.Constraint.Type == pm.Response_Validated_Constraint_INCOMPATIBLE {
			if reason != "" && !strings.Contains(pc.Constraint.Reason, reason) {
				continue
			}
			out = append(out, pc.Field)
		}
	}
	return out
}

func TestValidateClusterSingleNode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)
	var cfg = testConfig()
	var ctx = t.Context()
	var table = "test_validate_cluster"

	t.Run("sharding_key without a cluster is a user error", func(t *testing.T) {
		_, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 0, nil))
		require.ErrorContains(t, err, "no 'cluster'")
	})

	t.Run("unknown cluster is a user error", func(t *testing.T) {
		var clustered = cfg
		clustered.Cluster = "no_such_cluster"
		_, err := NewDriver().Validate(ctx, validateReq(t, clustered, tableConfig{Table: table}, 0, nil))
		require.ErrorContains(t, err, "not defined on this server")
	})
}

func TestValidateClusterShardingKey(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureClusterUp(t)
	var cfg = testClusterConfig()
	cfg.Cluster = "flow_cluster"
	var ctx = t.Context()
	var table = "test_validate_sharding_key"

	t.Run("sharding key that is a subset of the key is accepted", func(t *testing.T) {
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 0, nil))
		require.NoError(t, err)
		require.Empty(t, incompatibleFields(resp, ""))
	})

	t.Run("empty sharding key is accepted", func(t *testing.T) {
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table}, 0, nil))
		require.NoError(t, err)
		require.Empty(t, incompatibleFields(resp, ""))
	})

	t.Run("sharding key outside the key is a user error", func(t *testing.T) {
		_, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key", "requiredString"}}, 0, nil))
		require.ErrorContains(t, err, "[requiredString] are not part of the table's key")
	})

	t.Run("changed sharding key without backfill is INCOMPATIBLE on every projection", func(t *testing.T) {
		var last = specWith(t, cfg, tableConfig{Table: table}, 0)
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 0, last))
		require.NoError(t, err)
		var fields = incompatibleFields(resp, "sharding_key")
		require.Contains(t, fields, "key")
		require.Contains(t, fields, "requiredString")
		require.Contains(t, fields, "flow_document")
	})

	t.Run("changed sharding key with a backfill bump is allowed", func(t *testing.T) {
		var last = specWith(t, cfg, tableConfig{Table: table}, 0)
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 1, last))
		require.NoError(t, err)
		require.Empty(t, incompatibleFields(resp, ""))
	})

	t.Run("switching a binding onto a cluster without backfill is INCOMPATIBLE", func(t *testing.T) {
		var single = cfg
		single.Cluster = ""
		var last = specWith(t, single, tableConfig{Table: table}, 0)
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table}, 0, last))
		require.NoError(t, err)
		require.Contains(t, incompatibleFields(resp, "'cluster' setting changed"), "key")
	})

	t.Run("unchanged sharding key needs no backfill", func(t *testing.T) {
		var last = specWith(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 0)
		resp, err := NewDriver().Validate(ctx, validateReq(t, cfg, tableConfig{Table: table, ShardingKey: []string{"key"}}, 0, last))
		require.NoError(t, err)
		require.Empty(t, incompatibleFields(resp, ""))
	})
}
