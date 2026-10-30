package materialize

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/stretchr/testify/require"
)

func TestScheduleConfigSchema(t *testing.T) {
	var schema = schemagen.GenerateSchema("ScheduleConfig", ScheduleConfig{})
	var formatted, err = json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}
