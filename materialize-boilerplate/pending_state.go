package boilerplate

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// drainPendingState commits the transaction previously staged for the given
// state keys, if any, by invoking the connector's own Transactor.Acknowledge
// restricted to those keys. It returns the connector state update clearing
// the committed entries, or nil when there was no pending work to commit.
//
// The transactor is built against the last-applied specification: pending
// work was staged under its bindings and state keys, and the current
// (pre-Apply) resources still match it.
func drainPendingState[EC EndpointConfiger, FC FieldConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	ctx context.Context,
	materializer Materializer[EC, FC, RC, MT],
	req *pm.Request_Apply,
	endpointCfg EC,
	is *InfoSchema,
	stateKeys []string,
) (*pf.ConnectorState, error) {
	lastSpec := req.LastMaterialization

	mapped := make([]MappedBinding[EC, RC, MT], 0, len(lastSpec.Bindings))
	for idx := range lastSpec.Bindings {
		if mb, err := buildMappedBinding(endpointCfg, materializer, *lastSpec, idx); err != nil {
			return nil, fmt.Errorf("building mapped binding %d of the last-applied specification: %w", idx, err)
		} else {
			mapped = append(mapped, *mb)
		}
	}

	// The Apply RPC runs on shard zero before any transactions session opens,
	// so the transactor stands in for the task's entire key range.
	open := pm.Request_Open{
		Materialization: lastSpec,
		Version:         req.LastVersion,
		Range: &pf.RangeSpec{
			KeyEnd:    math.MaxUint32,
			RClockEnd: math.MaxUint32,
		},
		StateJson: req.StateJson,
	}

	transactor, err := materializer.NewTransactor(ctx, open, *is, mapped, m.NewBindingEvents())
	if err != nil {
		return nil, fmt.Errorf("building transactor: %w", err)
	}
	defer transactor.Destroy()

	if err := transactor.UnmarshalState(req.StateJson); err != nil {
		return nil, fmt.Errorf("unmarshalling state: %w", err)
	}

	patch, err := transactor.Acknowledge(ctx, nil, stateKeys)
	if err != nil {
		return nil, fmt.Errorf("acknowledging pending transaction: %w", err)
	} else if patch == nil || stateUnchanged(req.StateJson, patch) {
		return nil, nil
	}

	return patch, nil
}

// stateUnchanged reports whether applying the connector state update to the
// prior state produces no change. It protects the runtime's bounded Apply
// loop: a non-nil state update in the Applied response causes the runtime to
// persist it and invoke Apply again, so an update that changes nothing must
// never be returned, lest the loop's iteration budget be exhausted by
// connectors which return an unconditional clearing update.
func stateUnchanged(prior json.RawMessage, update *pf.ConnectorState) bool {
	var before any
	if err := json.Unmarshal(prior, &before); err != nil {
		return false
	}

	var after any
	if update.MergePatch {
		var patch any
		if err := json.Unmarshal(update.UpdatedJson, &patch); err != nil {
			return false
		}
		after = ApplyMergePatch(before, patch)
	} else if err := json.Unmarshal(update.UpdatedJson, &after); err != nil {
		return false
	}

	return reflect.DeepEqual(before, after)
}

// ApplyMergePatch applies an RFC 7396 JSON merge patch to target, returning
// the patched value without modifying target.
func ApplyMergePatch(target, patch any) any {
	patchObj, ok := patch.(map[string]any)
	if !ok {
		return patch
	}

	targetObj, ok := target.(map[string]any)
	if !ok {
		targetObj = nil // a non-object target is replaced by the patched object
	}

	out := make(map[string]any, len(targetObj)+len(patchObj))
	for k, v := range targetObj {
		out[k] = v
	}
	for k, v := range patchObj {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = ApplyMergePatch(out[k], v)
		}
	}
	return out
}
