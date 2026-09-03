package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
)

// stagedTransaction is one binding's staged-but-not-yet-applied work: the S3
// objects it staged and what the apply needs to know to commit them. Every
// field is written even when empty so that an entry's merge patch replaces
// the previous entry outright rather than inheriting its fields.
type stagedTransaction struct {
	StoreManifest  string   `json:"storeManifest"`
	DeleteManifest string   `json:"deleteManifest"`
	StoreFiles     []string `json:"storeFiles"`
	DeleteFiles    []string `json:"deleteFiles"`
	MustMerge      bool     `json:"mustMerge"`
	Widen          []string `json:"widen"`
}

func newStagedTransaction() *stagedTransaction {
	return &stagedTransaction{StoreFiles: []string{}, DeleteFiles: []string{}, Widen: []string{}}
}

// token identifies the transaction in the checkpoints table once applied.
func (e *stagedTransaction) token() string {
	if e.StoreManifest != "" {
		return e.StoreManifest
	}
	return e.DeleteManifest
}

// objects is every S3 object the transaction staged.
func (e *stagedTransaction) objects() []string {
	var out []string
	for _, m := range []string{e.StoreManifest, e.DeleteManifest} {
		if m != "" {
			out = append(out, m)
		}
	}
	out = append(out, e.StoreFiles...)
	return append(out, e.DeleteFiles...)
}

// pendingState is the connector state: staged transactions keyed by the
// staging shard's key range, then by binding state key. Ranges are disjoint
// across the shards of a task, so their merge patches never clobber each other.
type pendingState map[string]map[string]*stagedTransaction

func (p pendingState) add(rangeKey, stateKey string, e *stagedTransaction) {
	if p[rangeKey] == nil {
		p[rangeKey] = make(map[string]*stagedTransaction)
	}
	p[rangeKey][stateKey] = e
}

var rangeKeyRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{8}$`)

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
	for rk, bucket := range state {
		if !rangeKeyRe.MatchString(rk) {
			return nil, fmt.Errorf("parsing connector state: %q is not a shard key range", rk)
		}
		for sk, e := range bucket {
			if e == nil {
				delete(bucket, sk)
			}
		}
		if len(bucket) == 0 {
			delete(state, rk)
		}
	}

	return state, nil
}

// tokenPayload is what the checkpoints row holds once a task has crossed
// over: the manifest key of the last applied transaction of each state key,
// scoped by the key range of the shard that staged it.
type tokenPayload map[string]map[string]string

func (p tokenPayload) add(rangeKey, stateKey, token string) {
	if p[rangeKey] == nil {
		p[rangeKey] = make(map[string]string)
	}
	p[rangeKey][stateKey] = token
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

var errAppliedConcurrently = errors.New("transaction was already applied by another instance of this materialization; restarting will skip it")

// tokenStore reaches one materialization's checkpoints row for one key range.
type tokenStore struct {
	table            string // quoted identifier
	materialization  string
	keyBegin, keyEnd uint32
}

type pgxQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// read returns the row's tokens and, for a row still in the old format, its
// runtime checkpoint. Both are nil when there is no row.
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
	return decodeCheckpointsRow(raw)
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
			legacyCheckpoint = legacy
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

// write records the tokens of just-applied transactions in the row, within
// txn, which must roll back if another instance already committed one of them.
func (s *tokenStore) write(ctx context.Context, txn pgx.Tx, applied tokenPayload) error {
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
	for rk, bucket := range applied {
		for sk, token := range bucket {
			if tokens[rk][sk] == token {
				return errAppliedConcurrently
			}
			tokens.add(rk, sk, token)
		}
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
