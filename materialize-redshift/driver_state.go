package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// stagedTransaction is one binding's staged-but-not-yet-applied work: the S3
// objects its Store phase uploaded, and what Acknowledge needs to commit them
// into the target table.
type stagedTransaction struct {
	StoreManifest  string
	DeleteManifest string
	Files          []string
	MustMerge      bool
	// Widen are the identifiers of target table columns which must be altered
	// to VARCHAR(MAX) before the COPY, having been found too narrow for a
	// value the transaction staged.
	Widen []string
}

// token identifies the transaction within the applied-token store. Manifest
// keys are generated per transaction, so either manifest names it uniquely,
// and both are written by the same apply transaction.
func (e *stagedTransaction) token() string {
	if e.StoreManifest != "" {
		return e.StoreManifest
	}
	return e.DeleteManifest
}

// connectorState is the connector state document: the transaction each binding
// has staged but not yet applied, keyed by the binding's state key.
type connectorState map[string]*stagedTransaction

// parseConnectorState decodes a connector state document. The connector this
// one replaces emitted no state at all, so an upgraded task starts with
// nothing staged. Unknown fields are rejected rather than ignored: an entry
// this connector cannot fully understand describes data it would otherwise
// apply incorrectly.
func parseConnectorState(data json.RawMessage) (connectorState, error) {
	var out = make(connectorState)
	if len(bytes.TrimSpace(data)) == 0 {
		return out, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing connector state: %w", err)
	}

	for stateKey, val := range raw {
		var entry stagedTransaction
		var dec = json.NewDecoder(bytes.NewReader(val))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&entry); err != nil {
			return nil, fmt.Errorf("parsing staged transaction %q: %w", stateKey, err)
		}
		out[stateKey] = &entry
	}

	return out, nil
}

// appliedTokens is the payload of a materialization's row in the checkpoints
// table: the token of the transaction most recently applied for each binding,
// scoped by the key range of the shard which staged it.
//
//	{ "<keyBegin>-<keyEnd>": { "<stateKey>": "<manifestKey>" } }
//
// Scoping by range is what will let a coordinating shard record tokens for
// work its peers staged without their entries colliding.
type appliedTokens map[string]map[string]string
