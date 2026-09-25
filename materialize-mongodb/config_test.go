package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	var resp, err = (driver{}).
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestRedactedAddress(t *testing.T) {
	require.Equal(t, "mongodb+srv://user:xxxxx@my-mongo.test/?authSource=admin", redactedAddress("mongodb+srv://user:secret@my-mongo.test/?authSource=admin"))
	require.Equal(t, "mongodb://my-mongo.test:27017", redactedAddress("mongodb://my-mongo.test:27017"))
	require.Equal(t, "<unparseable address>", redactedAddress("mongodb://user:secret@my-mongo.test:bad"))
}
