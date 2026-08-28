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
// Rows are read `AS OF SYSTEM TIME <anchor>`, the latest resolved timestamp the changefeed has
// reported, so a chunk can never observe row state older than an already-emitted change event —
// this is what lets chunked backfill and unfiltered streaming converge. Each row is fetched as
// `to_jsonb(row)`, matching the changefeed's `after` encoding byte-for-byte.
func (db *cockroachdbDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, backfillFilter string, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var keyColumns = state.KeyColumns
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var anchor = db.snapshotTimestamp() // One consistent read timestamp for this whole chunk.

	var docMergeColumns []string
	if details, ok := info.ExtraDetails.(*cockroachdbTableDiscoveryDetails); ok {
		docMergeColumns = details.docMergeColumns
	}

	var query string
	var args []any
	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		// Every table has a primary key or a synthesized rowid, so keyless mode is never
		// suggested; a keyless collection would key on the shared HLC cursor value instead,
		// collapsing every row in a chunk into one document.
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
			query = db.buildScanQuery(anchor, false, keyColumns, docMergeColumns, schema, table, backfillFilter)
		} else {
			query = db.buildScanQuery(anchor, true, keyColumns, docMergeColumns, schema, table, backfillFilter)
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

		rowKey, err := encodeRowKey(values[:len(keyColumns)])
		if err != nil {
			return false, nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
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
func (db *cockroachdbDatabase) buildScanQuery(anchor string, start bool, keyColumns, docMergeColumns []string, schema, table, backfillFilter string) string {
	// Key columns are projected `::STRING` so the resume key round-trips exactly as a text bind
	// argument (unlike the Go values pgx would otherwise decode). The projection is aliased to
	// `__key<N>` so ORDER BY still resolves to the raw column rather than the STRING rendering.
	var quotedKeys = make([]string, len(keyColumns))
	var castKeys = make([]string, len(keyColumns))
	for i, col := range keyColumns {
		quotedKeys[i] = quoteIdentifier(col)
		castKeys[i] = fmt.Sprintf("%s::STRING AS __key%d", quotedKeys[i], i)
	}

	// Hidden stored key columns (the synthesized rowid) appear in changefeed `after` documents
	// but not in `to_jsonb(row)`, so they're merged into the backfill document to match. The
	// jsonb merge re-sorts object keys the same way, keeping the two renderings byte-identical.
	var docExpr = "to_jsonb(t.*)"
	for _, col := range docMergeColumns {
		docExpr = fmt.Sprintf("(%s || jsonb_build_object('%s', t.%s))", docExpr, strings.ReplaceAll(col, "'", "''"), quoteIdentifier(col))
	}

	// Generate a list of individual WHERE clauses which should be ANDed together.
	var whereClauses []string
	if !start {
		var placeholders = make([]string, len(keyColumns))
		for i := range keyColumns {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
		}
		whereClauses = append(whereClauses, fmt.Sprintf("(%s) > (%s)", strings.Join(quotedKeys, ", "), strings.Join(placeholders, ", ")))
	}
	if backfillFilter != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(%s)", backfillFilter))
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT %s, %s::STRING AS __doc", strings.Join(castKeys, ", "), docExpr)
	fmt.Fprintf(query, " FROM %s.%s AS t", quoteIdentifier(schema), quoteIdentifier(table))
	fmt.Fprintf(query, " AS OF SYSTEM TIME '%s'", anchor)
	if len(whereClauses) > 0 {
		fmt.Fprintf(query, " WHERE %s", strings.Join(whereClauses, " AND "))
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

func (db *cockroachdbDatabase) EstimatedRowCounts(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	db.tableStatistics.Lock()
	defer db.tableStatistics.Unlock()
	if db.tableStatistics.counts == nil {
		db.tableStatistics.counts = make(map[sqlcapture.StreamID]int)
	}

	var result = make(map[sqlcapture.TableID]int, len(tables))
	var queriedSchemas = make(map[string]bool)
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Table)
		if _, ok := db.tableStatistics.counts[streamID]; !ok && !queriedSchemas[table.Schema] {
			queriedSchemas[table.Schema] = true
			db.queryEstimatedRowCounts(ctx, table.Schema)
		}
		result[table] = db.tableStatistics.counts[streamID]
	}
	return result, nil
}

// queryEstimatedRowCounts fetches and caches the optimizer's row-count estimate (via `SHOW
// TABLES`, which exposes it without a full scan) for every table in schema in a single query,
// rather than issuing one query per requested table.
func (db *cockroachdbDatabase) queryEstimatedRowCounts(ctx context.Context, schema string) {
	var query = fmt.Sprintf("SELECT table_name, estimated_row_count FROM [SHOW TABLES FROM %q]", schema)
	var rows, err = db.conn.Query(ctx, query)
	if err != nil {
		logrus.WithFields(logrus.Fields{"schema": schema, "err": err}).Debug("error querying estimated row counts")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var tableName string
		var count int
		if err := rows.Scan(&tableName, &count); err != nil {
			logrus.WithFields(logrus.Fields{"schema": schema, "err": err}).Debug("error scanning estimated row count")
			continue
		}
		db.tableStatistics.counts[sqlcapture.JoinStreamID(schema, tableName)] = count
	}
}

func (db *cockroachdbDatabase) explainQuery(ctx context.Context, streamID sqlcapture.StreamID, query string, args []any) {
	if !logrus.IsLevelEnabled(logrus.DebugLevel) {
		return
	}
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
