package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	d := &transactor{
		cp: checkpoint{
			"a_table.v1": {Table: "a_table", Query: "MERGE INTO a", StagedDir: "dir"},
		},
		bindings: []*binding{{target: sql.Table{StateKey: "a_table.v1"}}},
	}

	// The staged entry's state key is not requested: nothing may execute (the
	// nil database would panic if a query ran) and no state update is
	// returned, so the entry remains pending in the persisted state.
	state, err := d.Acknowledge(context.Background(), nil, []string{"other_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, d.cp["a_table.v1"])
	require.True(t, d.didRecovery)
}

// The streaming v2 counter is durable per-binding state, not a staged
// transaction: the checkpoint-clearing pass of Acknowledge - which is also what
// the Apply-time pending-transaction drain relies on - must leave it in place,
// and must report no state update on its account, since an update which changes
// nothing would burn an iteration of the runtime's bounded Apply loop.
func TestAcknowledgeKeepsStreamV2Counter(t *testing.T) {
	d := &transactor{
		cp: checkpoint{
			"a_table.v1": {Table: "a_table", StreamV2: map[string]*streamV2Item{
				"00000000-ffffffff": {Channel: "chan", Counter: 42, KeyEnd: 0xffffffff},
			}},
		},
		bindings: []*binding{{target: sql.Table{StateKey: "a_table.v1"}, streamingV2: true}},
		be:       m.NewBindingEvents(),
		// The rows the counter accounts for were committed as the checkpoint was
		// produced, so Acknowledge needs nothing from the manager and no sidecar
		// is started.
		snowpipeStreamingV2: &streamV2Manager{},
	}

	state, err := d.Acknowledge(context.Background(), nil, []string{"a_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.Equal(t, int64(42), d.cp["a_table.v1"].StreamV2["00000000-ffffffff"].Counter)
}

// TestAcknowledgeRetiresStreamV2Markers covers the drop half of the break-glass
// downgrade: a marker-only key is drained only on the retirement's account, and
// only once this session has fenced the channels it names.
func TestAcknowledgeRetiresStreamV2Markers(t *testing.T) {
	var ctx = context.Background()
	const stateKey, channel = "ack.v1", "task_00000000_ack_v1"

	t.Run("a v2 binding's marker-only key is not drained", func(t *testing.T) {
		var d = &transactor{
			cp: checkpoint{stateKey: {StreamV2: map[string]*streamV2Item{
				"00000000-ffffffff": {Channel: channel, Counter: 42, KeyEnd: math.MaxUint32, Retiring: true},
			}}},
			bindings:            []*binding{{target: sql.Table{StateKey: stateKey, TableShape: sql.TableShape{Path: []string{"TBL"}}}, streamingV2: true}},
			be:                  m.NewBindingEvents(),
			snowpipeStreamingV2: &streamV2Manager{},
		}

		state, err := d.Acknowledge(ctx, nil, []string{stateKey})
		require.NoError(t, err)
		require.Nil(t, state)
		require.True(t, d.cp[stateKey].StreamV2["00000000-ffffffff"].Retiring)
	})

	// The remaining subtests drive a real retirement against the fake sidecar.
	singleChannelLayout(t)
	var statePath = filepath.Join(t.TempDir(), "channels.json")
	t.Setenv("FAKE_SIDECAR_STATE", statePath)

	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	var items = map[string]*streamV2Item{"00000000-ffffffff": {Channel: channel, Counter: 3, KeyEnd: math.MaxUint32}}

	var newAckTransactor = func(t *testing.T) *transactor {
		t.Helper()
		var sv2 = newStreamV2Manager(ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/ack", "acct", fullRange)
		sv2.argv = fakeSidecarArgv(t)
		t.Cleanup(sv2.stop)

		var d = &transactor{
			cp:                  checkpoint{stateKey: {StreamV2: items}},
			bindings:            []*binding{{target: sql.Table{StateKey: stateKey, TableShape: sql.TableShape{Path: []string{"TBL"}}}}},
			be:                  m.NewBindingEvents(),
			snowpipeStreamingV2: sv2,
		}
		d.retirement = newStreamV2Retirement(sv2)
		d.retirement.register(stateKey, "DB", "SCH", "TBL", items, fullRange)
		return d
	}

	t.Run("retiring markers with no pending work are left alone before the fence", func(t *testing.T) {
		var d = newAckTransactor(t)

		state, err := d.Acknowledge(ctx, nil, []string{stateKey})
		require.NoError(t, err)
		require.Nil(t, state)
		require.Equal(t, items, d.cp[stateKey].StreamV2)
	})

	t.Run("fenced markers are dropped and only this shard's keys are cleared", func(t *testing.T) {
		var d = newAckTransactor(t)
		require.NoError(t, d.retirement.fence(ctx))

		state, err := d.Acknowledge(ctx, nil, []string{stateKey})
		require.NoError(t, err)
		require.NotNil(t, state)

		var patch checkpoint
		require.NoError(t, json.Unmarshal(state.UpdatedJson, &patch))
		require.Contains(t, patch, stateKey)
		require.Len(t, patch[stateKey].StreamV2, 1)
		for _, v := range patch[stateKey].StreamV2 {
			require.Nil(t, v)
		}

		require.NotContains(t, fakeCommittedTokens(t, statePath), channel)
		require.True(t, d.retirement.done())
	})
}

func TestSpecification(t *testing.T) {
	var resp, err = NewDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestValidHost(t *testing.T) {
	for _, tt := range []struct {
		host string
		want error
	}{
		{"orgname-accountname.snowflakecomputing.com", nil},
		{"identifer.snowflakecomputing.com", nil},
		{"ORGNAME-accountname.snowFLAKEcomputing.coM", nil},
		{"orgname-accountname.aws.us-east-2.snowflakecomputing.com", nil},
		{"http://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "http://orgname-accountname.snowflakecomputing.com")},
		{"https://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "https://orgname-accountname.snowflakecomputing.com")},
		{"orgname-accountname.snowflakecomputin.com", fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", "orgname-accountname.snowflakecomputin.com")},
	} {
		t.Run(tt.host, func(t *testing.T) {
			require.Equal(t, tt.want, validHost(tt.host))
		})
	}
}
