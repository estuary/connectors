package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

func (db *db2Database) ShouldBackfill(streamID sqlcapture.StreamID) bool { return true }

func (db *db2Database) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, backfillFilter string, callback func(sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var query, args, err = buildBackfillQuery(info, state, db.config.Advanced.BackfillChunkSize, backfillFilter)
	if err != nil {
		return false, nil, err
	}
	var lobs = backfillLOBColumns(info)
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("unable to execute backfill query: %w", err)
	}
	defer rows.Close()

	var values = make([]any, len(info.ColumnNames))
	var pointers = make([]any, len(values))
	for i := range values {
		pointers[i] = &values[i]
	}
	var lengths = make([]sql.NullInt64, len(lobs))
	for i := range lengths {
		pointers = append(pointers, &lengths[i])
	}
	var keyless = state.Mode == sqlcapture.TableStateKeylessBackfill || len(state.KeyColumns) == 0
	var rid int64
	if keyless {
		pointers = append(pointers, &rid)
	}
	var keyColumns = state.KeyColumns
	var from = quoteTableName(info.Schema, info.Name)
	if keyless {
		from += " AS T WHERE RID(T) = ?"
	} else {
		var predicates = make([]string, len(keyColumns))
		for i, name := range keyColumns {
			predicates[i] = quoteIdentifier(name) + " = ?"
		}
		from += " WHERE " + strings.Join(predicates, " AND ")
	}
	var count int
	var rowKey = state.Scanned
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return false, nil, fmt.Errorf("error scanning backfill row: %w", err)
		}
		if len(lobs) > 0 {
			var key = []any{rid}
			if !keyless {
				key = make([]any, len(keyColumns))
				for i, name := range keyColumns {
					var value = values[slices.Index(info.ColumnNames, name)]
					var ct = info.Columns[name].DataType.(db2ColumnType)
					if ct.Name == "TIMESTAMP" {
						key[i], err = db2TimestampText(value)
					} else {
						key[i], err = db.translateDB2Value(value, ct)
					}
					if err != nil {
						return false, nil, err
					}
				}
			}
			// Re-reads may observe updates or deletes; the CD table emits those changes after backfill.
			if err := recoverLOBs(ctx, db.conn, from, key, lobs, lengths, values, true); err != nil {
				return false, nil, err
			}
		}
		var fields = make(map[string]any, len(values))
		for i, name := range info.ColumnNames {
			fields[name] = values[i]
		}
		var offset = state.BackfilledCount + count
		if keyless {
			rowKey = tuple.Tuple{rid}.Pack()
		} else if rowKey, err = db.encodeRowKey(state.KeyColumns, fields, info.Columns); err != nil {
			return false, nil, fmt.Errorf("error encoding backfill key: %w", err)
		}
		if err := db.translateRecord(fields, info.Columns); err != nil {
			return false, nil, err
		}

		var intent [8]byte
		binary.BigEndian.PutUint64(intent[:], uint64(offset))
		var event = &sqlcapture.OldChangeEvent{
			Operation: sqlcapture.InsertOp,
			RowKey:    rowKey,
			After:     fields,
			Source: db2SourceInfo{
				SourceCommon: sqlcapture.SourceCommon{Schema: info.Schema, Table: info.Name, Snapshot: true},
				Intent:       hex.EncodeToString(intent[:]),
				Tag:          db.config.Advanced.SourceTag,
			},
		}
		if err := callback(event); err != nil {
			return false, nil, fmt.Errorf("error processing backfill row: %w", err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return false, nil, fmt.Errorf("error reading backfill rows: %w", err)
	}
	return count < db.config.Advanced.BackfillChunkSize, rowKey, nil
}

func buildBackfillQuery(info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, chunkSize int, backfillFilter string) (string, []any, error) {
	var keyless = state.Mode == sqlcapture.TableStateKeylessBackfill || len(state.KeyColumns) == 0
	var columns = make([]string, len(info.ColumnNames))
	for i, name := range info.ColumnNames {
		columns[i] = info.Columns[name].DataType.(db2ColumnType).projection(name)
	}
	var lobs = backfillLOBColumns(info)
	for _, column := range lobs {
		columns = append(columns, column.lengthProjection())
	}
	if keyless {
		columns = append(columns, "RID(T)")
	}
	var query = "SELECT " + strings.Join(columns, ", ") + " FROM " + quoteTableName(info.Schema, info.Name) + " AS T"
	var predicates []string
	var args []any
	var keys = make([]string, len(state.KeyColumns))
	for i, name := range state.KeyColumns {
		if _, ok := info.Columns[name]; !ok {
			return "", nil, fmt.Errorf("backfill key column %q does not exist", name)
		}
		keys[i] = "T." + quoteIdentifier(name)
	}
	if keyless && len(state.Scanned) != 0 {
		var resume, err = tuple.Unpack(state.Scanned)
		if err != nil || len(resume) != 1 {
			return "", nil, fmt.Errorf("invalid keyless backfill resume key for %s.%s; re-backfill the binding", info.Schema, info.Name)
		}
		var rid, ok = resume[0].(int64)
		if !ok {
			return "", nil, fmt.Errorf("invalid keyless backfill RID for %s.%s; re-backfill the binding", info.Schema, info.Name)
		}
		predicates = append(predicates, "RID(T) > ?")
		args = append(args, rid)
	}
	if !keyless && len(state.Scanned) != 0 {
		var resume, err = tuple.Unpack(state.Scanned)
		if err != nil || len(resume) != len(keys) {
			return "", nil, fmt.Errorf("invalid backfill resume key for %s.%s: %v", info.Schema, info.Name, err)
		}
		// Expand the tuple comparison into ranges that Db2 can use with a composite index.
		var terms []string
		for i, key := range keys {
			var term []string
			for j := 0; j < i; j++ {
				term = append(term, keys[j]+" = ?")
				args = append(args, resume[j])
			}
			term = append(term, key+" > ?")
			args = append(args, resume[i])
			terms = append(terms, "("+strings.Join(term, " AND ")+")")
		}
		predicates = append(predicates, "("+strings.Join(terms, " OR ")+")")
	}
	if backfillFilter != "" {
		predicates = append(predicates, "("+backfillFilter+")")
	}
	if len(predicates) > 0 {
		query += " WHERE " + strings.Join(predicates, " AND ")
	}
	if keyless {
		query += " ORDER BY RID(T)"
	} else {
		query += " ORDER BY " + strings.Join(keys, ", ")
	}
	query += fmt.Sprintf(" FETCH FIRST %d ROWS ONLY", chunkSize)
	return query, args, nil
}

func (db *db2Database) EstimatedRowCounts(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	var counts = make(map[sqlcapture.TableID]int)
	for _, table := range tables {
		var count int64
		if err := db.conn.QueryRowContext(ctx, "SELECT CARD FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?", table.Schema, table.Table).Scan(&count); err != nil {
			return nil, fmt.Errorf("error reading table statistics: %w", err)
		}
		if count >= 0 {
			counts[table] = int(count)
		}
	}
	return counts, nil
}
