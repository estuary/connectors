package testutil

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/consumer/protocol"
)

// RunAcknowledgeAfterFieldAddTest verifies that a post-commit apply connector
// commits a staged transaction correctly when the session acknowledging it
// was opened on a later specification than the session that staged it. The
// later specification selects an additional field which sorts ahead of every
// previously selected value, so a connector that stages rows positionally
// must commit them by the column list in force when they were staged, not by
// the current binding's.
//
//  1. A base specification is applied, creating the resource.
//  2. A session on the base specification stores one document and returns the
//     connector state staging it, without acknowledging.
//  3. The specification adding the field is applied without connector state,
//     so no drain of the staged work occurs before the resource is altered.
//  4. A session on the updated specification recovers the staged state and
//     acknowledges. The committed row must carry each staged value in its own
//     column.
func RunAcknowledgeAfterFieldAddTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	driver boilerplate.Connector,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	cfg EC,
	res RC,
) {
	ctx := context.Background()
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)

	materializer, err := newMaterializer(ctx, "acknowledge-after-field-add-test", cfg, harnessFlags(t, cfg))
	require.NoError(t, err)

	resourcePath, _, err := res.Parameters()
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := DropResource(ctx, materializer, resourcePath); err != nil {
			t.Log("failed to clean up resource:", err)
		}
	})

	initial := loadSpec(t, "base.flow.proto")
	updated := loadSpec(t, "add-single-optional.flow.proto")

	validateRes, err := driver.Validate(ctx, validateReq(initial, nil, configJson, resourceConfigJson))
	require.NoError(t, err)
	_, err = driver.Apply(ctx, applyReq(initial, nil, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	fullRange := &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	// Stage one document under the base specification.
	staging, _, _, err := driver.NewTransactor(ctx, pm.Request_Open{
		Materialization: initial,
		Version:         "priorVersion",
		Range:           fullRange,
	}, m.NewBindingEvents())
	require.NoError(t, err)

	binding := initial.Bindings[0]
	it := m.NewStoreIterator(ctx, &singleStoreStream{store: &pm.Request_Store{
		KeyPacked:    stagedTuple(t, binding.FieldSelection.Keys).Pack(),
		ValuesPacked: stagedTuple(t, binding.FieldSelection.Values).Pack(),
		DocJson:      []byte(stagedDocument),
	}}, &pm.Request{Flush: &pm.Request_Flush{}})

	startCommit, err := staging.Store(it)
	require.NoError(t, err)
	require.NoError(t, it.Err())

	stagedState, op := startCommit(ctx, &protocol.Checkpoint{})
	if op != nil {
		require.NoError(t, op.Err())
	}
	require.NotNil(t, stagedState, "a post-commit apply connector must stage its transaction in connector state")
	stateJson := reduceConnectorState(t, []byte("{}"), stagedState)
	staging.Destroy()

	// Alter the resource for the updated specification. No connector state
	// accompanies the Apply, so nothing is drained first.
	validateRes, err = driver.Validate(ctx, validateReq(updated, initial, configJson, resourceConfigJson))
	require.NoError(t, err)
	applied, err := driver.Apply(ctx, applyReq(updated, initial, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)
	require.Nil(t, applied.State, "an Apply without connector state must not drain")

	// Acknowledge the staged transaction from a session on the updated
	// specification.
	recovering, _, _, err := driver.NewTransactor(ctx, pm.Request_Open{
		Materialization: updated,
		Version:         "nextVersion",
		Range:           fullRange,
		StateJson:       stateJson,
	}, m.NewBindingEvents())
	require.NoError(t, err)
	defer recovering.Destroy()
	require.NoError(t, recovering.UnmarshalState(stateJson))

	_, err = recovering.Acknowledge(ctx, nil, nil)
	require.NoError(t, err, "acknowledging a transaction staged under the prior specification")

	colNames, rows, err := materializer.SnapshotTestResource(ctx, resourcePath)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the staged transaction's row must have been committed")

	for field, want := range stagedScalars {
		idx := -1
		for i, col := range colNames {
			if strings.EqualFold(col, field) {
				idx = i
				break
			}
		}
		require.NotEqual(t, -1, idx, "column for field %q not found in %v", field, colNames)
		require.Equal(t, want, fmt.Sprint(rows[0][idx]), "column %q holds the wrong value", field)
	}
	for i, col := range colNames {
		if strings.EqualFold(col, "addedOptionalString") {
			require.Nil(t, rows[0][i], "the added column must be null for a row staged before it existed")
		}
	}
}

// stagedDocument is the document staged by RunAcknowledgeAfterFieldAddTest,
// whose values are stagedValues.
const stagedDocument = `{"key":"k1","requiredString":"req","requiredInteger":1,"requiredBoolean":true,"requiredObject":{"req":true},"optionalString":"opt","optionalInteger":2,"optionalBoolean":true,"optionalObject":{"opt":true}}`

// stagedValues are the tuple elements of stagedDocument's projections, as the
// runtime packs them, for every field the drain fixture specifications
// (validate_apply_test_cases) may select.
var stagedValues = map[string]tuple.TupleElement{
	"key":                  "k1",
	"flow_published_at":    "2026-01-01T00:00:00Z",
	"_meta/flow_truncated": false,
	"optionalBoolean":      true,
	"requiredBoolean":      true,
	"optionalInteger":      int64(2),
	"requiredInteger":      int64(1),
	"optionalString":       "opt",
	"requiredString":       "req",
	"optionalObject":       []byte(`{"opt":true}`),
	"requiredObject":       []byte(`{"req":true}`),
	"second_root":          []byte(stagedDocument),
}

// stagedScalars are the committed row's expected scalar columns, rendered as
// strings so that the comparison is independent of the destination's native
// scalar types.
var stagedScalars = map[string]string{
	"key":             "k1",
	"optionalBoolean": "true",
	"requiredBoolean": "true",
	"optionalInteger": "2",
	"requiredInteger": "1",
	"optionalString":  "opt",
	"requiredString":  "req",
}

func stagedTuple(t *testing.T, fields []string) tuple.Tuple {
	t.Helper()

	out := make(tuple.Tuple, 0, len(fields))
	for _, f := range fields {
		v, ok := stagedValues[f]
		require.True(t, ok, "no staged value for selected field %q", f)
		out = append(out, v)
	}
	return out
}

// singleStoreStream serves one Store request to a StoreIterator, then the
// end of the transaction's stores.
type singleStoreStream struct {
	store *pm.Request_Store
}

func (s *singleStoreStream) Send(*pm.Response) error { return nil }

func (s *singleStoreStream) RecvMsg(request *pm.Request) error {
	request.Reset()
	if s.store != nil {
		request.Store, s.store = s.store, nil
	}
	return nil
}
