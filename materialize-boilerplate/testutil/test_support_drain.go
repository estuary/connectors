package testutil

import (
	"context"
	"encoding/json"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// RunApplyDrainTest verifies the two-iteration Apply drain of a post-commit
// apply connector, which stages transactions in its connector state and only
// commits them during Acknowledge:
//
//  1. A base specification is applied, creating the resource.
//  2. seedPending stages destination-side pending work for the resource, as a
//     committed-but-unacknowledged transaction would, and returns the
//     connector state document describing it.
//  3. An Apply adding a column to the resource is invoked carrying that state.
//     It must commit the pending work and return a clearing state update
//     WITHOUT altering the resource.
//  4. The same Apply is re-invoked with the reduced state, as the runtime's
//     apply loop does. It must perform the schema update and return no
//     further state update, and a subsequent re-invocation (as after a crash)
//     must converge as a no-op.
func RunApplyDrainTest[EC boilerplate.EndpointConfiger, FC boilerplate.FieldConfiger, RC boilerplate.Resourcer[RC, EC], MT boilerplate.MappedTyper](
	t *testing.T,
	driver boilerplate.Connector,
	newMaterializer boilerplate.NewMaterializerFn[EC, FC, RC, MT],
	cfg EC,
	res RC,
	seedPending func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage,
	verifyDrained func(t *testing.T, appliedSpec *pf.MaterializationSpec, colNames []string, rows [][]any),
) {
	ctx := context.Background()
	configJson, resourceConfigJson := rawJson(t, cfg), rawJson(t, res)

	materializer, err := newMaterializer(ctx, "apply-drain-test", cfg, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)

	resourcePath, _, err := res.Parameters()
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, fn, err := materializer.DeleteResource(ctx, resourcePath); err == nil && fn != nil {
			_ = fn(ctx)
		}
	})

	initial := loadSpec(t, "base.flow.proto")
	updated := loadSpec(t, "add-single-optional.flow.proto")

	validateRes, err := driver.Validate(ctx, validateReq(initial, nil, configJson, resourceConfigJson))
	require.NoError(t, err)
	_, err = driver.Apply(ctx, applyReq(initial, nil, configJson, resourceConfigJson, validateRes, true))
	require.NoError(t, err)

	baseSchema := dumpSchema(t, ctx, materializer, res)
	stateJson := seedPending(t, initial)

	validateRes, err = driver.Validate(ctx, validateReq(updated, initial, configJson, resourceConfigJson))
	require.NoError(t, err)
	req := applyReq(updated, initial, configJson, resourceConfigJson, validateRes, true)
	req.LastVersion = "priorVersion"
	req.StateJson = stateJson

	// First iteration: the affected binding's pending work is committed and a
	// clearing state update returned, with the resource left unaltered.
	firstApplied, err := driver.Apply(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, firstApplied.State, "first Apply must return a state update draining the pending transaction")
	require.NotEmpty(t, firstApplied.ActionDescription, "a state-updating Applied must carry a non-empty action description")
	require.Equal(t, baseSchema, dumpSchema(t, ctx, materializer, res), "first Apply must not alter the resource")
	colNames, rows, err := materializer.SnapshotTestResource(ctx, resourcePath)
	require.NoError(t, err)
	verifyDrained(t, initial, colNames, rows)

	// The runtime durably persists the update and re-invokes Apply with the
	// reduced state.
	req.StateJson = reduceConnectorState(t, stateJson, firstApplied.State)

	// Second iteration: nothing remains pending, so the schema update is
	// performed with no further state update, ending the runtime's loop.
	secondApplied, err := driver.Apply(ctx, req)
	require.NoError(t, err)
	require.Nil(t, secondApplied.State, "second Apply must not return a state update")
	require.NotEmpty(t, secondApplied.ActionDescription)
	require.NotEqual(t, baseSchema, dumpSchema(t, ctx, materializer, res), "second Apply must alter the resource")

	// Apply must be idempotent: last_materialization is only bumped once the
	// loop converges, so a crash-and-retry re-runs the same request.
	thirdApplied, err := driver.Apply(ctx, req)
	require.NoError(t, err)
	require.Nil(t, thirdApplied.State)
}

// reduceConnectorState applies a connector state update to a prior state
// document exactly as the runtime does between Apply iterations.
func reduceConnectorState(t *testing.T, prior json.RawMessage, update *pf.ConnectorState) json.RawMessage {
	t.Helper()

	if !update.MergePatch {
		return update.UpdatedJson
	}

	var before, patch any
	require.NoError(t, json.Unmarshal(prior, &before))
	require.NoError(t, json.Unmarshal(update.UpdatedJson, &patch))
	reduced, err := json.Marshal(applyMergePatch(before, patch))
	require.NoError(t, err)

	return reduced
}

// applyMergePatch applies an RFC 7396 JSON merge patch to target, returning
// the patched value without modifying target.
func applyMergePatch(target, patch any) any {
	patchObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}

	targetObj, ok := target.(map[string]any)
	if !ok {
		targetObj = nil
	}

	out := make(map[string]any, len(targetObj)+len(patchObj))
	for k, v := range targetObj {
		out[k] = v
	}
	for k, v := range patchObj {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = applyMergePatch(out[k], v)
		}
	}
	return out
}
