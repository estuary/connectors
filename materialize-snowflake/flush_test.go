package connector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestFlushFeatureFlags(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		flags string
		want  map[int]time.Time
	}{
		{flags: "", want: map[int]time.Time{0: boundary}},
		{flags: "no_truncate_after_backfill", want: map[int]time.Time{}},
		{flags: "retain_existing_data_on_backfill", want: map[int]time.Time{}},
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var d = &transactor{
				bindings:              []*binding{{target: sql.Table{}, truncateSQL: "DELETE"}},
				truncateAfterBackfill: sql.TruncateAfterBackfill(flags),
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestFlushDeltaUpdatesExempt(t *testing.T) {
	var d = &transactor{
		bindings:              []*binding{{target: sql.Table{TableShape: sql.TableShape{DeltaUpdates: true}}, truncateSQL: "DELETE"}},
		truncateAfterBackfill: true,
	}
	require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: time.Now()}))
	require.Equal(t, map[int]time.Time{}, d.truncations)
}

func TestCheckpointCarriesTruncation(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 100, time.UTC)
	var newBinding = func(stateKey string) *binding {
		var b = &binding{target: sql.Table{StateKey: stateKey, Identifier: stateKey}}
		b.store.stage = &stagedFile{}
		return b
	}
	var d = &transactor{
		bindings:    []*binding{newBinding("truncated"), newBinding("untouched")},
		cp:          make(checkpoint),
		staged:      make(stagedCheckpoint),
		rangeKey:    fullRangeKey,
		truncations: map[int]time.Time{0: boundary},
	}

	cpJSON, err := d.buildDriverCheckpoint(t.Context(), nil, 7)
	require.NoError(t, err)
	require.Nil(t, d.truncations)

	// A binding that stored nothing still gets an item carrying the boundary.
	var got map[string]map[string]map[string]any
	require.NoError(t, json.Unmarshal(cpJSON, &got))
	require.Equal(t, map[string]map[string]map[string]any{
		"truncated": {fullRangeKey: {
			"Table": "truncated", "Query": "", "StagedDir": "", "StreamBlobs": nil,
			"PipeName": "", "PipeFiles": nil, "Version": "", "EncryptionKey": "",
			"Round": float64(7), "Rows": float64(0),
			"TruncateBefore": "2026-09-29T00:00:00.0000001Z",
		}},
	}, got)
}

func TestMergeBoundsCoverOnlyTheirRound(t *testing.T) {
	var key = sql.Column{
		Projection: sql.Projection{Projection: pf.Projection{
			Field:     "id",
			Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST},
		}},
		Identifier: "ID",
	}
	var b = &binding{target: sql.Table{StateKey: "t", Identifier: "T", Keys: []sql.Column{key}}}
	b.store.stage = &stagedFile{}
	b.store.mergeBounds = sql.NewMergeBoundsBuilder(b.target.Keys, testDialect.Literal)
	var d = &transactor{
		bindings:  []*binding{b},
		cp:        make(checkpoint),
		staged:    make(stagedCheckpoint),
		rangeKey:  "00000000-ffffffff",
		templates: testTemplates,
		profiler:  newQueryProfiler(nil, []*binding{b}),
	}

	var round = func(mustMerge bool, keys ...string) *checkpointItem {
		b.store.stage.started, b.store.stage.finished = true, true
		b.store.stage.group = &errgroup.Group{}
		b.store.mustMerge = mustMerge
		for _, k := range keys {
			b.store.mergeBounds.NextKey([]any{k})
		}
		_, err := d.buildDriverCheckpoint(t.Context(), nil, 0)
		require.NoError(t, err)
		return d.staged["t"][d.rangeKey]
	}

	require.Contains(t, round(false, "a", "z").Query, "COPY INTO")
	var merge = round(true, "m").Query
	require.Contains(t, merge, "MERGE INTO")
	require.Contains(t, merge, "l.ID >= 'm' AND l.ID <= 'm'")
}
