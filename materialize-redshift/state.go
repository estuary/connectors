package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// checkpointTokensColumn holds checkpointTokensMap. A NULL value means this
// row has never been written by this connector: its legacy checkpoint is
// still authoritative.
const checkpointTokensColumn = "checkpoint_tokens"

// stateItem is one binding's staged-but-not-yet-applied work. Every
// field is written even when empty, so an entry's merge patch replaces the
// previous entry outright.
type stateItem struct {
	// ID is the transaction's token in the checkpoints table once applied.
	ID          string   `json:"id"`
	StoreFiles  []string `json:"storeFiles"`
	DeleteFiles []string `json:"deleteFiles"`
	MustMerge   bool     `json:"mustMerge"`
	Widen       []string `json:"widen"`
}

// objects is every S3 object the transaction staged.
func (e *stateItem) objects() []string {
	return slices.Concat(e.StoreFiles, e.DeleteFiles)
}

// connectorState is staged transactions by binding state key, then by the
// staging shard's key range.
type connectorState map[string]map[string]*stateItem

func (p connectorState) add(stateKey, rangeKey string, e *stateItem) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]*stateItem)
	}
	p[stateKey][rangeKey] = e
}

// hasRange reports whether any state key has an entry staged under rangeKey.
func (p connectorState) hasRange(rangeKey string) bool {
	for _, bucket := range p {
		if _, ok := bucket[rangeKey]; ok {
			return true
		}
	}
	return false
}

// entryCount is the total number of entries across every state key.
func (p connectorState) entryCount() int {
	var n int
	for _, bucket := range p {
		n += len(bucket)
	}
	return n
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

// checkpointTokensMap is what the checkpoints row holds once a task has
// crossed over: the last applied transaction's ID by state key and staging
// key range.
type checkpointTokensMap map[string]map[string]string

func (p checkpointTokensMap) add(stateKey, rangeKey, token string) {
	if p[stateKey] == nil {
		p[stateKey] = make(map[string]string)
	}
	p[stateKey][rangeKey] = token
}

// decodeLegacyCheckpoint decodes a checkpoints row's checkpoint column: a
// base64 (possibly gzipped) runtime checkpoint, the format written by
// pre-migration versions of this connector and mirrored by this one. It
// reads back as hex text because the column is VARBYTE.
func decodeLegacyCheckpoint(raw string) ([]byte, error) {
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("decoding checkpoints row: %w", err)
	}
	b64decoded, err := base64.StdEncoding.DecodeString(string(decoded))
	if err != nil {
		return nil, fmt.Errorf("decoding checkpoint: %w", err)
	} else if len(b64decoded) == 0 {
		return nil, nil
	}
	return maybeDecompressBytes(b64decoded)
}

// decodeTokensColumn decodes a checkpoints row's checkpoint_tokens column,
// nil when raw is the column's NULL. It reads back as hex text because the
// column is VARBYTE.
func decodeTokensColumn(raw *string) (checkpointTokensMap, error) {
	if raw == nil {
		return nil, nil
	}
	decoded, err := hex.DecodeString(*raw)
	if err != nil {
		return nil, fmt.Errorf("decoding checkpoint tokens row: %w", err)
	}
	var tokens checkpointTokensMap
	if err := json.Unmarshal(decoded, &tokens); err != nil {
		return nil, fmt.Errorf("parsing applied tokens: %w", err)
	}
	return tokens, nil
}

// checkpointsTable reaches one materialization's checkpoints row for one key range,
// and mirrors that row's payload.
type checkpointsTable struct {
	table            string // quoted identifier
	materialization  string
	keyBegin, keyEnd uint32
	payload          checkpointTokensMap
}

// ensureTokensColumn adds the checkpoint_tokens column to a checkpoints
// table predating it, ignoring Redshift's "column already exists" error
// once it's there.
func (s *checkpointsTable) ensureTokensColumn(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s VARBYTE(1024000);", s.table, checkpointTokensColumn)); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42701" {
			return nil
		}
		return fmt.Errorf("adding %s column: %w", checkpointTokensColumn, err)
	}
	return nil
}

// readAll returns the tokens committed by every row of the materialization,
// whatever key range wrote them; this range's row's mirrored legacy runtime
// checkpoint; and whether this row's checkpoint_tokens have ever been
// written by this connector, in which case the legacy checkpoint is no
// longer authoritative. A primary of a previous shard topology committed
// into a different row, and its tokens must still settle the entries it
// applied.
func (s *checkpointsTable) readAll(ctx context.Context, conn *pgx.Conn) (map[string]bool, []byte, bool, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		"SELECT key_begin, key_end, checkpoint, %s FROM %s WHERE materialization = $1;", checkpointTokensColumn, s.table), s.materialization)
	if err != nil {
		return nil, nil, false, fmt.Errorf("reading checkpoints rows: %w", err)
	}
	defer rows.Close()

	var applied = make(map[string]bool)
	var legacyCheckpoint []byte
	var crossedOver bool
	for rows.Next() {
		var keyBegin, keyEnd uint32
		var checkpointRaw string
		var tokensRaw *string
		if err := rows.Scan(&keyBegin, &keyEnd, &checkpointRaw, &tokensRaw); err != nil {
			return nil, nil, false, fmt.Errorf("scanning checkpoints row: %w", err)
		}
		tokens, err := decodeTokensColumn(tokensRaw)
		if err != nil {
			return nil, nil, false, err
		}
		for _, bucket := range tokens {
			for _, token := range bucket {
				applied[token] = true
			}
		}
		if keyBegin == s.keyBegin && keyEnd == s.keyEnd {
			s.payload, crossedOver = tokens, tokensRaw != nil
			if legacyCheckpoint, err = decodeLegacyCheckpoint(checkpointRaw); err != nil {
				return nil, nil, false, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, fmt.Errorf("reading checkpoints rows: %w", err)
	}
	return applied, legacyCheckpoint, crossedOver, nil
}

// write records the tokens of just-applied transactions in the row, within
// txn. When legacyCheckpoint is non-nil, it's mirrored into the legacy
// checkpoint column too, so a downgraded connector can resume from it; the
// caller passes one only once every entry that was pending is settled by
// this call, so the mirror never claims a checkpoint whose data is only
// partially applied.
func (s *checkpointsTable) write(ctx context.Context, txn pgx.Tx, applied checkpointTokensMap, legacyCheckpoint []byte) error {
	// Materializations sharing a metadata schema update the same table
	// concurrently, which raises serializable isolation violations even for
	// different rows. All applies take this one lock in the same order.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", s.table)); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	if s.payload == nil {
		s.payload = make(checkpointTokensMap)
	}
	for sk, bucket := range applied {
		for rk, token := range bucket {
			s.payload.add(sk, rk, token)
		}
	}

	tokensPayload, err := json.Marshal(s.payload)
	if err != nil {
		return fmt.Errorf("marshalling applied tokens: %w", err)
	}

	var legacyPayload *string
	if legacyCheckpoint != nil {
		compressed, err := compressBytes(legacyCheckpoint)
		if err != nil {
			return fmt.Errorf("compressing runtime checkpoint: %w", err)
		}
		var encoded = base64.StdEncoding.EncodeToString(compressed)
		legacyPayload = &encoded
	}

	var rowsAffected int64
	if legacyPayload == nil {
		res, err := txn.Exec(ctx, fmt.Sprintf(
			"UPDATE %s SET %s = $1 WHERE materialization = $2 AND key_begin = $3 AND key_end = $4;", s.table, checkpointTokensColumn),
			string(tokensPayload), s.materialization, s.keyBegin, s.keyEnd,
		)
		if err != nil {
			return fmt.Errorf("writing applied tokens: %w", err)
		}
		rowsAffected = res.RowsAffected()
	} else {
		res, err := txn.Exec(ctx, fmt.Sprintf(
			"UPDATE %s SET %s = $1, checkpoint = $2 WHERE materialization = $3 AND key_begin = $4 AND key_end = $5;", s.table, checkpointTokensColumn),
			string(tokensPayload), *legacyPayload, s.materialization, s.keyBegin, s.keyEnd,
		)
		if err != nil {
			return fmt.Errorf("writing applied tokens: %w", err)
		}
		rowsAffected = res.RowsAffected()
	}
	if rowsAffected > 1 {
		return fmt.Errorf("checkpoints table has %d rows for materialization %q and key range %s", rowsAffected, s.materialization, rangeKeyOf(s.keyBegin, s.keyEnd))
	} else if rowsAffected == 1 {
		return nil
	}

	// The fence column is NOT NULL; nothing reads it anymore.
	if legacyPayload == nil {
		if _, err := txn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, fence, %s) VALUES ($1, $2, $3, 0, $4);", s.table, checkpointTokensColumn),
			s.materialization, s.keyBegin, s.keyEnd, string(tokensPayload),
		); err != nil {
			return fmt.Errorf("inserting applied tokens: %w", err)
		}
	} else if _, err := txn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (materialization, key_begin, key_end, fence, checkpoint, %s) VALUES ($1, $2, $3, 0, $4, $5);", s.table, checkpointTokensColumn),
		s.materialization, s.keyBegin, s.keyEnd, *legacyPayload, string(tokensPayload),
	); err != nil {
		return fmt.Errorf("inserting applied tokens: %w", err)
	}
	return nil
}
