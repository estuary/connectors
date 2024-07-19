package aws

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/stretchr/testify/require"
)

func TestConfigSchema(t *testing.T) {
	t.Parallel()

	schema := schemagen.GenerateSchema("Test Config Schema", CredentialConfig{})
	formatted, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}
