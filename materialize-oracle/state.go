package connector

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// A token is a task's tag, a hash of its name, then a UUIDv7. The text of one
// task's tokens sorts by the time they were made.
func taskTag(materialization string) string {
	var sum = sha256.Sum256([]byte(materialization))
	return hex.EncodeToString(sum[:4])
}

func newToken(tag string) string {
	return tag + "-" + uuid.Must(uuid.NewV7()).String()
}

// tokenBefore returns text that sorts after every token of tag made before
// ts, and before every one made from ts on. tag+"-" sorts before all of
// them.
func tokenBefore(tag string, ts time.Time) string {
	var ms = uint64(ts.UnixMilli())
	return fmt.Sprintf("%s-%08x-%04x-0000-0000-000000000000", tag, ms>>16, ms&0xffff)
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
