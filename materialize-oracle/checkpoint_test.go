package connector

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

func TestOpenRecoversCheckpoint(t *testing.T) {
	spec := testMaterialization(t, testConfig())
	checkpoint := &protocol.Checkpoint{Sources: map[pb.Journal]protocol.Checkpoint_Source{
		"test/journal": {ReadThrough: 1234},
	}}
	encoded, err := checkpoint.Marshal()
	require.NoError(t, err)
	client := &existingTableClient{t: t, idType: "NUMBER", checkpoint: encoded}
	d := testDriver(t, client)

	// Connector state is empty even after a commit: the destination checkpoint
	// is authoritative, including when the runtime crashes before acknowledging.
	// A first Apply of a task keeps a stored checkpoint, as materialize-sql does.
	for attempt := 0; attempt < 2; attempt++ {
		req := &pm.Request_Apply{Materialization: spec, Version: "next"}
		if attempt > 0 {
			req.LastMaterialization, req.LastVersion = spec, "previous"
		}
		_, err := d.Apply(t.Context(), req)
		require.NoError(t, err)
		transactor, opened, _, err := d.NewTransactor(t.Context(), pm.Request_Open{
			Materialization: spec, Version: "next", Range: &pf.RangeSpec{KeyEnd: 0xffffffff, RClockEnd: 0xffffffff},
		}, nil)
		require.NoError(t, err)
		transactor.Destroy()
		require.Equal(t, checkpoint, opened.RuntimeCheckpoint)
		require.Empty(t, client.statements)
	}
}
