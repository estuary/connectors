package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestDriverSpec(t *testing.T) {
	var drv = driver{}
	var resp, err1 = drv.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err1)
	var formatted, err2 = json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err2)
	cupaloy.SnapshotT(t, formatted)
}
