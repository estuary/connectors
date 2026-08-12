package materialize

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestScheduleConfigSchema(t *testing.T) {
	schema := schemagen.GenerateSchema("ScheduleConfig", ScheduleConfig{})
	formatted, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}

func TestRuntimePacesCommits(t *testing.T) {
	for _, tt := range []struct {
		name string
		spec *pf.MaterializationSpec
		want bool
	}{
		{"no spec", nil, false},
		{"unset schedule", &pf.MaterializationSpec{}, false},
		{"empty schedule", &pf.MaterializationSpec{SyncScheduleJson: []byte{}}, false},
		{"zero interval", &pf.MaterializationSpec{SyncScheduleJson: []byte(`{"baseInterval":"0s"}`)}, true},
		{"defaulted schedule", &pf.MaterializationSpec{SyncScheduleJson: []byte(`{}`)}, true},
		{"configured schedule", &pf.MaterializationSpec{SyncScheduleJson: []byte(`{"baseInterval":"4h"}`)}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, runtimePacesCommits(tt.spec))
		})
	}
}
