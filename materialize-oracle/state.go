package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// stateItem is the rows one shard staged for one binding in one transaction,
// which are applied to the binding's table once the transaction commits.
type stateItem struct {
	// Token tags the staged rows in the binding's stage table.
	Token string `json:"token"`
	// Round is the transaction round that staged the rows, and Inserts,
	// Updates and Deletes count them by operation, for the row stats reported
	// when they are applied.
	Round   int   `json:"round"`
	Inserts int64 `json:"inserts"`
	Updates int64 `json:"updates"`
	Deletes int64 `json:"deletes"`
	// TruncateBefore is the boundary of a backfill that completed in the
	// transaction. Rows published before it are deleted after the stores.
	TruncateBefore *time.Time `json:"truncateBefore"`
}

// connectorState is staged transactions by binding state key, then by the
// staging shard's key range.
type connectorState map[string]map[string]*stateItem

func (s connectorState) add(stateKey, rangeKey string, e *stateItem) {
	if s[stateKey] == nil {
		s[stateKey] = make(map[string]*stateItem)
	}
	s[stateKey][rangeKey] = e
}

func rangeKeyOf(keyBegin, keyEnd uint32) string {
	return fmt.Sprintf("%08x-%08x", keyBegin, keyEnd)
}

// parseState decodes a state document or a StartedCommit patch of the same
// shape.
func parseState(raw json.RawMessage) (connectorState, error) {
	var state = make(connectorState)
	if len(bytes.TrimSpace(raw)) == 0 {
		return state, nil
	}

	var dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&state); err != nil {
		return nil, fmt.Errorf("parsing connector state: %w", err)
	}
	return state, nil
}
