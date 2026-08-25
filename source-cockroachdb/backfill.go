package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/sirupsen/logrus"
)

// ScanTableChunk reads a chunk of rows from a table as part of the initial backfill.
//
// The chunk is read `AS OF SYSTEM TIME <anchor>`, where the anchor is the latest resolved
// timestamp the changefeed has reported (or the capture's start position before any). Reading at
// the latest resolved timestamp means a chunk can never observe row state older than a change
// event which has already been emitted, while all changes after it are still streamed afterwards
// and supersede the chunk — this is what makes chunked backfill and unfiltered streaming converge.
// Each row is fetched as `to_jsonb(row)`, which produces exactly the same JSON encoding as the
// changefeed's `after` value, so backfilled and streamed documents are byte-for-byte consistent.
func (db *cockroachdbDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var keyColumns = state.KeyColumns
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var anchor = db.snapshotTimestamp() // One consistent read timestamp for this whole chunk.

	var docMergeColumns []string
	if details, ok := info.ExtraDetails.(*cockroachdbTableDetails); ok {
		docMergeColumns = details.docMergeColumns
	}

	var query string
	var args []any
	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		// Tables always have a primary key or a synthesized rowid (which discovery adopts as the
		// collection key), so keyless mode is never suggested and cannot be captured correctly:
		// keyless collections fall back to a key on the HLC cursor value, which is shared by every
		// backfill row and by all rows in a transaction, collapsing them to a single document.
		return false, nil, fmt.Errorf("keyless backfill mode is not supported for %q: capture the table with its primary key (or synthesized rowid) instead", streamID)
	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		if state.Scanned != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(state.Scanned, decodeKeyFDB)
			if err != nil {
				return false, nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return false, nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
			}
			args = resumeKey
			query = db.buildScanQuery(anchor, false, keyColumns, docMergeColumns, schema, table)
		} else {
			query = db.buildScanQuery(anchor, true, keyColumns, docMergeColumns, schema, table)
		}
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	db.explainQuery(ctx, streamID, query, args)
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing backfill query")

	var rows, err = db.conn.Query(ctx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("error executing backfill query for %q: %w", streamID, err)
	}
	defer rows.Close()

	var resultRows int
	var nextRowKey []byte
	var rowOffset = state.BackfilledCount
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return false, nil, fmt.Errorf("error reading backfill row for %q: %w", streamID, err)
		}
		// The query selects the key columns (in key order) followed by the row document, so the
		// document is always the final value and the preceding values are the key columns.
		var docStr, ok = values[len(values)-1].(string)
		if !ok {
			return false, nil, fmt.Errorf("expected string document for %q but got %T", streamID, values[len(values)-1])
		}

		var rowKey []byte
		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			// A 19-digit decimal is sufficient to hold any 63-bit row offset.
			rowKey = []byte(fmt.Sprintf("B%019d", rowOffset))
		} else {
			rowKey, err = encodeRowKey(values[:len(keyColumns)])
			if err != nil {
				return false, nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}
		nextRowKey = rowKey

		if err := callback(&cockroachdbChangeEvent{
			stream:   streamID,
			op:       sqlcapture.InsertOp,
			document: []byte(docStr),
			rowKey:   rowKey,
			source: cockroachdbSource{
				SourceCommon: sqlcapture.SourceCommon{
					Schema:   schema,
					Table:    table,
					Snapshot: true,
				},
				Cursor: anchor,
				Tag:    db.config.Advanced.SourceTag,
			},
		}); err != nil {
			return false, nil, fmt.Errorf("error processing backfill row for %q: %w", streamID, err)
		}
		resultRows++
		rowOffset++
	}
	if err := rows.Err(); err != nil {
		return false, nil, fmt.Errorf("error reading backfill results for %q: %w", streamID, err)
	}

	var backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	return backfillComplete, nextRowKey, nil
}

// buildScanQuery constructs a keyed backfill query. When `start` is false it adds a row-value
// comparison predicate `(k1, k2, ...) > ($1, $2, ...)` to resume after the previous chunk;
// CockroachDB supports this composite comparison directly, which is simpler than the expanded
// per-column OR form needed by some other databases.
func (db *cockroachdbDatabase) buildScanQuery(anchor string, start bool, keyColumns, docMergeColumns []string, schema, table string) string {
	var quotedKeys = make([]string, len(keyColumns))
	for i, col := range keyColumns {
		quotedKeys[i] = quoteIdentifier(col)
	}

	// Hidden stored key columns (the synthesized rowid) appear in changefeed `after` documents
	// but not in `to_jsonb(row)`, so they're merged into the backfill document to match. The
	// jsonb merge re-sorts object keys the same way, keeping the two renderings byte-identical.
	var docExpr = "to_jsonb(t.*)"
	for _, col := range docMergeColumns {
		docExpr = fmt.Sprintf("(%s || jsonb_build_object('%s', t.%s))", docExpr, strings.ReplaceAll(col, "'", "''"), quoteIdentifier(col))
	}

	// Key columns are projected as `::STRING` so that the resume key round-trips exactly:
	// CockroachDB's canonical text rendering of a value is always accepted back as a text bind
	// argument compared against the native column, whereas the Go values pgx would otherwise
	// decode ([16]byte UUIDs, time.Time, pgtype.Numeric) have no such guarantee. The WHERE and
	// ORDER BY clauses keep the raw column references so the primary-key index is still used.
	//
	// The aliases are load-bearing: an unaliased `"id"::STRING` output column is still named
	// "id", which `ORDER BY "id"` then resolves to — silently sorting by the STRING rendering
	// (an unindexed top-k sort, and an ordering inconsistent with the raw-column resume
	// predicate). Aliasing the projection keeps ORDER BY bound to the table column.
	var castKeys = make([]string, len(keyColumns))
	for i, col := range quotedKeys {
		castKeys[i] = fmt.Sprintf("%s::STRING AS __key%d", col, i)
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT %s, %s::STRING AS __doc", strings.Join(castKeys, ", "), docExpr)
	fmt.Fprintf(query, " FROM %s.%s AS t", quoteIdentifier(schema), quoteIdentifier(table))
	fmt.Fprintf(query, " AS OF SYSTEM TIME '%s'", anchor)
	if !start {
		var placeholders = make([]string, len(keyColumns))
		for i := range keyColumns {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		fmt.Fprintf(query, " WHERE (%s) > (%s)", strings.Join(quotedKeys, ", "), strings.Join(placeholders, ", "))
	}
	fmt.Fprintf(query, " ORDER BY %s", strings.Join(quotedKeys, ", "))
	fmt.Fprintf(query, " LIMIT %d", db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func encodeRowKey(values []any) ([]byte, error) {
	var elements = make([]tuple.TupleElement, len(values))
	for i, v := range values {
		var elem, err = encodeKeyFDB(v)
		if err != nil {
			return nil, err
		}
		elements[i] = elem
	}
	return sqlcapture.PackTuple(elements)
}

// quoteIdentifier double-quotes a SQL identifier, doubling any embedded quote characters.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (db *cockroachdbDatabase) explainQuery(ctx context.Context, streamID sqlcapture.StreamID, query string, args []any) {
	if db.explained == nil {
		db.explained = make(map[sqlcapture.StreamID]struct{})
	}
	if _, ok := db.explained[streamID]; ok {
		return
	}
	db.explained[streamID] = struct{}{}

	var rows, err = db.conn.Query(ctx, "EXPLAIN "+query, args...)
	if err != nil {
		logrus.WithFields(logrus.Fields{"stream": streamID, "err": err}).Debug("failed to explain backfill query")
		return
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return
		}
		var parts []string
		for _, v := range values {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		plan = append(plan, strings.Join(parts, " "))
	}
	logrus.WithFields(logrus.Fields{"stream": streamID, "plan": plan}).Debug("backfill query plan")
}
