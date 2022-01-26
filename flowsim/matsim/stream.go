package matsim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/estuary/connectors/flowsim/testdata"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// SetupConnectorOpenTransactions runs a connector through the Spec/Validate/Apply/Transactions lifecycle and returns the Transactions stream.
func SetupConnectorOpenTransactions(ctx context.Context, materialization *pf.MaterializationSpec, client pm.DriverClient, deltaUpdates bool) (pm.Driver_TransactionsClient, error) {

	// Make the spec request to setup the materialization.
	respSpec, err := client.Spec(ctx, &pm.SpecRequest{
		EndpointType:     pf.EndpointType_FLOW_SINK,
		EndpointSpecJson: materialization.EndpointSpecJson,
	})
	if err != nil {
		return nil, fmt.Errorf("spec error: %v", err)
	}
	log.Debugf("spec response: %#v", respSpec)

	// Build the bindings and send the Validate request.
	var bindings = make([]*pm.ValidateRequest_Binding, len(materialization.Bindings))
	for i, binding := range materialization.Bindings {
		bindings[i] = &pm.ValidateRequest_Binding{
			Collection:       binding.Collection,
			ResourceSpecJson: binding.ResourceSpecJson,
			FieldConfigJson:  binding.FieldSelection.FieldConfigJson,
		}

	}

	respValidate, err := client.Validate(ctx, &pm.ValidateRequest{
		Materialization:  materialization.Materialization,
		EndpointType:     materialization.EndpointType,
		EndpointSpecJson: materialization.EndpointSpecJson,
		Bindings:         bindings,
	})
	if err != nil {
		return nil, fmt.Errorf("validate error: %v", err)
	}
	log.Debugf("validate response: %#v", respValidate)

	// We need to ensure that the DeltaUpdate and ResourcePath match the validate response.
	// Since we use SQLite to generate the catalog, we cannot be be sure that these values
	// match until Validate is called with the real connector.
	for i, binding := range materialization.Bindings {
		binding.DeltaUpdates = respValidate.Bindings[i].DeltaUpdates
		binding.ResourcePath = respValidate.Bindings[i].ResourcePath
		if binding.DeltaUpdates != deltaUpdates {
			return nil, fmt.Errorf("delta updates mismatch. connector: %v tester: %v", binding.DeltaUpdates, deltaUpdates)
		}
	}

	for _, respBindings := range respValidate.Bindings {
		for field, constraint := range respBindings.Constraints {
			switch constraint.Type {
			case pm.Constraint_FIELD_FORBIDDEN, pm.Constraint_UNSATISFIABLE:
				log.Warnf("binding: %v field: %s is missing from materialization.", respBindings.ResourcePath, field)
				err = fmt.Errorf("binding: %v field: %s is missing from materialization", respBindings.ResourcePath, field)
			}
		}
	}
	if err != nil {
		return nil, err
	}

	// Apply the materialization.
	respApply, err := client.ApplyUpsert(ctx, &pm.ApplyRequest{
		Materialization: materialization,
		Version:         "1.0",
		DryRun:          false,
	})
	if err != nil {
		return nil, fmt.Errorf("apply error: %v", err)
	}
	log.Debugf("apply response: %#v", respApply)

	// Open the transactions connection
	stream, err := client.Transactions(ctx)
	if err != nil {
		return nil, fmt.Errorf("transactions error: %v", err)
	}

	return stream, err

}

// SendLoad sends load requests of size batchSize with load requests (keys) of the passed items.
func SendLoad(stream pm.Driver_TransactionsClient, items []testdata.TestData, batchSize int) error {

	var keys [][]byte

	// Send the current batch.
	var sendRequest = func() error {
		if len(keys) == 0 {
			return nil
		}

		var arena pf.Arena
		var arenaPackedKeys = arena.AddAll(keys...)
		if err := stream.Send(&pm.TransactionRequest{
			Load: &pm.TransactionRequest_Load{
				Arena:      arena,
				PackedKeys: arenaPackedKeys,
			},
		}); err != nil {
			return err
		}
		keys = keys[:0]
		return nil
	}

	// Loop through the items sending at batchSize.
	for pos := 0; pos < len(items); pos++ {
		keys = append(keys, items[pos].Keys().Pack())
		if len(keys) == batchSize {
			if err := sendRequest(); err != nil {
				return err
			}
		}
	}

	// Send the final request.
	return sendRequest()

}

// SendPrepare sends a Prepare request with the specified checkpoint.
func SendPrepare(stream pm.Driver_TransactionsClient, checkpoint []byte) error {
	return stream.Send(&pm.TransactionRequest{
		Prepare: &pm.TransactionRequest_Prepare{
			FlowCheckpoint: []byte(checkpoint),
		},
	})
}

func SendAcknowledge(stream pm.Driver_TransactionsClient) error {
	return stream.Send(&pm.TransactionRequest{
		Acknowledge: &pm.TransactionRequest_Acknowledge{},
	})
}

func AwaitAcknowledged(stream pm.Driver_TransactionsClient) error {
	acked, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("reading Acknowledged: %w", err)
	} else if acked.Acknowledged == nil {
		return fmt.Errorf("expected Acknowledged, got %#v", acked.String())
	}

	return nil
}

func Acknowledge(stream pm.Driver_TransactionsClient) error {
	if err := SendAcknowledge(stream); err != nil {
		return fmt.Errorf("sending Acknowledge: %w", err)
	}

	if err := AwaitAcknowledged(stream); err != nil {
		return fmt.Errorf("awaiting Acknowledged: %w", err)
	}

	return nil
}

// SendStore sends store requests in blocks of batchSize with all the specified items.
func SendStore(stream pm.Driver_TransactionsClient, items []testdata.TestData, exists []bool, batchSize int) error {
	if len(items) != len(exists) {
		return errors.New("length of items does not match length of exists")
	}

	var docs [][]byte
	var keys [][]byte
	var values [][]byte
	var batchStart int

	// Send the current batch.
	var sendRequest = func(pos int) error {
		if len(docs) == 0 {
			return nil
		}
		var storeRequest = pm.TransactionRequest_Store{}
		storeRequest.DocsJson = storeRequest.Arena.AddAll(docs...)
		storeRequest.PackedKeys = storeRequest.Arena.AddAll(keys...)
		storeRequest.PackedValues = storeRequest.Arena.AddAll(values...)
		storeRequest.Exists = exists[batchStart : pos+1]

		if err := stream.Send(&pm.TransactionRequest{
			Store: &storeRequest,
		}); err != nil {
			return err
		}

		docs = docs[:0]
		keys = keys[:0]
		values = values[:0]
		batchStart = pos + 1

		return nil
	}

	// Loop through the items sending at batchSize.
	for pos := 0; pos < len(items); pos++ {
		var doc, err = json.Marshal(items[pos])
		if err != nil {
			return fmt.Errorf("doc json: %w", err)
		}
		docs = append(docs, doc)
		keys = append(keys, items[pos].Keys().Pack())
		values = append(values, items[pos].Values().Pack())
		if len(docs) == batchSize {
			if err := sendRequest(pos); err != nil {
				return err
			}
		}
	}

	// Send the final request.
	return sendRequest(len(items) - 1)
}

// SendCommit sends a commit message.
func SendCommit(stream pm.Driver_TransactionsClient) error {
	return stream.Send(&pm.TransactionRequest{
		Commit: &pm.TransactionRequest_Commit{},
	})
}

// AwaitCommitted receives a committed message.
func AwaitCommitted(stream pm.Driver_TransactionsClient) error {
	committed, err := stream.Recv()
	if err != nil {
		return err
	} else if committed.DriverCommitted == nil {
		return fmt.Errorf("unexpected message: %#v", committed)
	}
	return nil
}

// HandleLoadedPrepared reads messages and passes loaded message to the passed function and returns the checkpoint once received.
func HandleLoadedPrepared(stream pm.Driver_TransactionsClient, f func(*pm.TransactionResponse_Loaded) error) (*pf.DriverCheckpoint, error) {
	for {
		next, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if next.Loaded != nil {
			if err = f(next.Loaded); err != nil {
				return nil, err
			}
		} else if next.Prepared != nil {
			return next.Prepared, nil
		} else {
			return nil, fmt.Errorf("unexpected message: %#v", next)
		}
	}

}
