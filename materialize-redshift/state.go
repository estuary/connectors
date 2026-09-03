package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// checkpointItem is one binding's staged-but-not-yet-applied work. Every
// field is written even when empty, so an entry's merge patch replaces the
// previous entry outright.
type checkpointItem struct {
	// ID is the transaction's token in the checkpoints table once applied.
	ID          string   `json:"id"`
	StoreFiles  []string `json:"storeFiles"`
	DeleteFiles []string `json:"deleteFiles"`
	MustMerge   bool     `json:"mustMerge"`
	Widen       []string `json:"widen"`
}

// objects is every S3 object the transaction staged.
func (e *checkpointItem) objects() []string {
	return append(append([]string{}, e.StoreFiles...), e.DeleteFiles...)
}

// pendingState is the connector state: staged transactions by binding state
// key, then by the staging shard's key range.
type pendingState map[string]map[string]*checkpointItem

func (p pendingState) add(stateKey, rangeKey string, e *checkpointItem) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]*checkpointItem)
	}
	p[stateKey][rangeKey] = e
}

// hasRange reports whether any state key has an entry staged under rangeKey.
func (p pendingState) hasRange(rangeKey string) bool {
	for _, bucket := range p {
		if _, ok := bucket[rangeKey]; ok {
			return true
		}
	}
	return false
}

func rangeKeyOf(keyBegin, keyEnd uint32) string {
	return fmt.Sprintf("%08x-%08x", keyBegin, keyEnd)
}

// parseState decodes a state document or a StartedCommit patch of the same
// shape.
func parseState(raw json.RawMessage) (pendingState, error) {
	var state = make(pendingState)
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

// checkpoints is what the checkpoints row holds once a task has crossed over:
// the last applied transaction's ID by state key and staging key range.
type checkpoints map[string]map[string]string

func (p checkpoints) add(stateKey, rangeKey, token string) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]string)
	}
	p[stateKey][rangeKey] = token
}

// parseCheckpointsRow decodes a checkpoints row's checkpoint column, which
// reads back as hex text because the column is VARBYTE. Its decoded bytes are
// either a token payload or, for a row still in the old format, a base64
// (possibly gzipped) runtime checkpoint. Base64 cannot produce a leading `{`.
func (s *checkpointsTable) parseCheckpointsRow(raw string) (checkpoints, []byte, error) {
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding checkpoints row: %w", err)
	}

	if len(decoded) > 0 && decoded[0] == '{' {
		var tokens checkpoints
		if err := json.Unmarshal(decoded, &tokens); err != nil {
			return nil, nil, fmt.Errorf("parsing applied tokens: %w", err)
		}
		return tokens, nil, nil
	}

	b64decoded, err := base64.StdEncoding.DecodeString(string(decoded))
	if err != nil {
		return nil, nil, fmt.Errorf("decoding checkpoint: %w", err)
	} else if len(b64decoded) == 0 {
		return nil, nil, nil
	}
	checkpoint, err := maybeDecompressBytes(b64decoded)
	if err != nil {
		return nil, nil, err
	}
	return nil, checkpoint, nil
}

// checkpointsTable reaches one materialization's checkpoints row for one key range,
// and mirrors that row's payload.
type checkpointsTable struct {
	table            string // quoted identifier
	materialization  string
	keyBegin, keyEnd uint32
	payload          checkpoints
}

// readAll returns the tokens committed by every row of the materialization,
// whatever key range wrote them, and the runtime checkpoint of this range's
// row when that row is still in the old format. A primary of a previous
// shard topology committed into a different row, and its tokens must still
// settle the entries it applied.
func (s *checkpointsTable) readAll(ctx context.Context, conn *pgx.Conn) (map[string]bool, []byte, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT key_begin, key_end, checkpoint FROM %s WHERE materialization = $1;", s.table), s.materialization)
	if err != nil {
		return nil, nil, fmt.Errorf("reading checkpoints rows: %w", err)
	}
	defer rows.Close()

	var applied = make(map[string]bool)
	var legacyCheckpoint []byte
	for rows.Next() {
		var keyBegin, keyEnd uint32
		var raw string
		if err := rows.Scan(&keyBegin, &keyEnd, &raw); err != nil {
			return nil, nil, fmt.Errorf("scanning checkpoints row: %w", err)
		}
		tokens, legacy, err := s.parseCheckpointsRow(raw)
		if err != nil {
			return nil, nil, err
		}
		for _, bucket := range tokens {
			for _, token := range bucket {
				applied[token] = true
			}
		}
		if keyBegin == s.keyBegin && keyEnd == s.keyEnd {
			s.payload, legacyCheckpoint = tokens, legacy
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("reading checkpoints rows: %w", err)
	}
	return applied, legacyCheckpoint, nil
}

// write records the tokens of just-applied transactions in the row, within txn.
func (s *checkpointsTable) write(ctx context.Context, txn pgx.Tx, applied checkpoints) error {
	// Materializations sharing a metadata schema update the same table
	// concurrently, which raises serializable isolation violations even for
	// different rows. All applies take this one lock in the same order.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", s.table)); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	if s.payload == nil {
		s.payload = make(checkpoints)
	}
	for sk, bucket := range applied {
		for rk, token := range bucket {
			s.payload.add(sk, rk, token)
		}
	}

	payload, err := json.Marshal(s.payload)
	if err != nil {
		return fmt.Errorf("marshalling applied tokens: %w", err)
	}

	res, err := txn.Exec(ctx, fmt.Sprintf(
		"UPDATE %s SET checkpoint = $1 WHERE materialization = $2 AND key_begin = $3 AND key_end = $4;", s.table),
		string(payload), s.materialization, s.keyBegin, s.keyEnd,
	)
	if err != nil {
		return fmt.Errorf("writing applied tokens: %w", err)
	} else if res.RowsAffected() > 1 {
		return fmt.Errorf("checkpoints table has %d rows for materialization %q and key range %s", res.RowsAffected(), s.materialization, rangeKeyOf(s.keyBegin, s.keyEnd))
	} else if res.RowsAffected() == 1 {
		return nil
	}

	// The fence column is NOT NULL; nothing reads it anymore.
	if _, err := txn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (materialization, key_begin, key_end, fence, checkpoint) VALUES ($1, $2, $3, 0, $4);", s.table),
		s.materialization, s.keyBegin, s.keyEnd, string(payload),
	); err != nil {
		return fmt.Errorf("inserting applied tokens: %w", err)
	}
	return nil
}
