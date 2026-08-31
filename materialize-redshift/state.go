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

// stagedTransaction is one binding's staged-but-not-yet-applied work. Every
// field is written even when empty, so an entry's merge patch replaces the
// previous entry outright.
type stagedTransaction struct {
	// ID is the transaction's token in the checkpoints table once applied.
	ID          string   `json:"id"`
	StoreFiles  []string `json:"storeFiles"`
	DeleteFiles []string `json:"deleteFiles"`
	MustMerge   bool     `json:"mustMerge"`
	Widen       []string `json:"widen"`
}

// objects is every S3 object the transaction staged.
func (e *stagedTransaction) objects() []string {
	return append(append([]string{}, e.StoreFiles...), e.DeleteFiles...)
}

// pendingState is the connector state: staged transactions by binding state
// key, then by the staging shard's key range.
type pendingState map[string]map[string]*stagedTransaction

func (p pendingState) add(stateKey, rangeKey string, e *stagedTransaction) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]*stagedTransaction)
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
// shape. A null entry (a cleared one) is dropped.
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
	if state == nil {
		// A null document or patch is empty, per RFC 7396.
		state = make(pendingState)
	}
	for sk, bucket := range state {
		for rk, e := range bucket {
			if e == nil {
				delete(bucket, rk)
			}
		}
		if len(bucket) == 0 {
			delete(state, sk)
		}
	}

	return state, nil
}

// tokenPayload is what the checkpoints row holds once a task has crossed over:
// the last applied transaction's ID by state key and staging key range.
type tokenPayload map[string]map[string]string

func (p tokenPayload) add(stateKey, rangeKey, token string) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]string)
	}
	p[stateKey][rangeKey] = token
}

// parseCheckpointsRow tells a token payload from a row still in the old
// format, which held a base64 (possibly gzipped) runtime checkpoint. Base64
// cannot produce a leading `{`.
func parseCheckpointsRow(raw []byte) (tokenPayload, []byte, error) {
	if len(raw) > 0 && raw[0] == '{' {
		var tokens tokenPayload
		if err := json.Unmarshal(raw, &tokens); err != nil {
			return nil, nil, fmt.Errorf("parsing applied tokens: %w", err)
		}
		return tokens, nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(string(raw))
	if err != nil {
		return nil, nil, fmt.Errorf("decoding checkpoint: %w", err)
	} else if len(decoded) == 0 {
		return nil, nil, nil
	}
	checkpoint, err := maybeDecompressBytes(decoded)
	if err != nil {
		return nil, nil, err
	}
	return nil, checkpoint, nil
}

// tokenStore reaches one materialization's checkpoints row for one key range,
// and mirrors that row's payload.
type tokenStore struct {
	table            string // quoted identifier
	materialization  string
	keyBegin, keyEnd uint32
	payload          tokenPayload
}

// readAll returns the tokens committed by every row of the materialization,
// whatever key range wrote them, and the runtime checkpoint of this range's
// row when that row is still in the old format. A primary of a previous
// shard topology committed into a different row, and its tokens must still
// settle the entries it applied.
func (s *tokenStore) readAll(ctx context.Context, conn *pgx.Conn) (map[string]bool, []byte, error) {
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
		tokens, legacy, err := decodeCheckpointsRow(raw)
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

// decodeCheckpointsRow parses a checkpoint column value, which reads back as
// hex text because the column is VARBYTE.
func decodeCheckpointsRow(raw string) (tokenPayload, []byte, error) {
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding checkpoints row: %w", err)
	}
	return parseCheckpointsRow(decoded)
}

// write records the tokens of just-applied transactions in the row, within txn.
func (s *tokenStore) write(ctx context.Context, txn pgx.Tx, applied tokenPayload) error {
	// Materializations sharing a metadata schema update the same table
	// concurrently, which raises serializable isolation violations even for
	// different rows. All applies take this one lock in the same order.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", s.table)); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	if s.payload == nil {
		s.payload = make(tokenPayload)
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
