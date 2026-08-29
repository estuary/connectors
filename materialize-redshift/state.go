package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// stagedTransaction is one binding's staged-but-not-yet-applied work: the S3
// objects it staged and what the apply needs to know to commit them. Every
// field is written even when empty so that an entry's merge patch replaces
// the previous entry outright rather than inheriting its fields.
type stagedTransaction struct {
	StoreManifest  string   `json:"storeManifest"`
	DeleteManifest string   `json:"deleteManifest"`
	Objects        []string `json:"objects"`
	MustMerge      bool     `json:"mustMerge"`
	Widen          []string `json:"widen"`
}

func newStagedTransaction() *stagedTransaction {
	return &stagedTransaction{Objects: []string{}, Widen: []string{}}
}

// token identifies the transaction in the checkpoints table once applied.
func (e *stagedTransaction) token() string {
	if e.StoreManifest != "" {
		return e.StoreManifest
	}
	return e.DeleteManifest
}

// parseState decodes the connector state: staged transactions keyed by the
// state key of the binding that staged them.
func parseState(raw json.RawMessage) (map[string]*stagedTransaction, error) {
	var pending = make(map[string]*stagedTransaction)
	if len(bytes.TrimSpace(raw)) == 0 {
		return pending, nil
	}

	var dec = json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&pending); err != nil {
		return nil, fmt.Errorf("parsing connector state: %w", err)
	}

	return pending, nil
}

// tokenPayload is what the checkpoints row holds once a task has crossed
// over: the manifest key of the last applied transaction of each state key,
// scoped by shard key range.
type tokenPayload map[string]map[string]string

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

var errAppliedConcurrently = errors.New("transaction was already applied by another instance of this materialization; restarting will skip it")

// tokenStore reaches the checkpoints row of one materialization and key range.
type tokenStore struct {
	table            string // quoted identifier
	materialization  string
	keyBegin, keyEnd uint32
}

func (s *tokenStore) rangeKey() string {
	return fmt.Sprintf("%08x-%08x", s.keyBegin, s.keyEnd)
}

type pgxQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// read returns the row's tokens for this range and, for a row still in the
// old format, its runtime checkpoint. Both are nil when there is no row.
func (s *tokenStore) read(ctx context.Context, q pgxQuerier) (tokenPayload, []byte, error) {
	var raw string
	if err := q.QueryRow(ctx, fmt.Sprintf(
		"SELECT checkpoint FROM %s WHERE materialization = $1 AND key_begin = $2 AND key_end = $3;", s.table),
		s.materialization, s.keyBegin, s.keyEnd,
	).Scan(&raw); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, fmt.Errorf("reading checkpoints row: %w", err)
	}

	// VARBYTE values read back as hex text.
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding checkpoints row: %w", err)
	}
	return parseCheckpointsRow(decoded)
}

// write records the tokens of just-applied transactions in the row, within
// txn, which must roll back if another instance already committed one of them.
func (s *tokenStore) write(ctx context.Context, txn pgx.Tx, applied map[string]string) error {
	// Materializations sharing a metadata schema update the same table
	// concurrently, which raises serializable isolation violations even for
	// different rows. All applies take this one lock in the same order.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", s.table)); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	tokens, _, err := s.read(ctx, txn)
	if err != nil {
		return err
	}
	if tokens == nil {
		tokens = make(tokenPayload)
	}
	if tokens[s.rangeKey()] == nil {
		tokens[s.rangeKey()] = make(map[string]string)
	}
	for sk, token := range applied {
		if tokens[s.rangeKey()][sk] == token {
			return errAppliedConcurrently
		}
		tokens[s.rangeKey()][sk] = token
	}

	payload, err := json.Marshal(tokens)
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
		return fmt.Errorf("checkpoints table has %d rows for materialization %q and key range %s", res.RowsAffected(), s.materialization, s.rangeKey())
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
