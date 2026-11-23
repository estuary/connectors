package main

import (
	"context"
	"database/sql"
	"fmt"
	"unicode/utf8"

	"github.com/estuary/connectors/sqlcapture"
)

type db2LOBColumn struct {
	Name  string
	Index int
	Type  db2ColumnType
}

func (ct db2ColumnType) isLOB() bool {
	return ct.Name == "CLOB" || ct.Name == "BLOB" || ct.Name == "DBCLOB" || ct.Name == "XML"
}

func backfillLOBColumns(info *sqlcapture.DiscoveryInfo) []db2LOBColumn {
	var columns []db2LOBColumn
	for i, name := range info.ColumnNames {
		if ct := info.Columns[name].DataType.(db2ColumnType); ct.isLOB() {
			columns = append(columns, db2LOBColumn{Name: name, Index: i, Type: ct})
		}
	}
	return columns
}

func (c db2LOBColumn) expression() string {
	if c.Type.Name == "DBCLOB" {
		return "CLOB(" + quoteIdentifier(c.Name) + ")"
	}
	if c.Type.Name == "XML" {
		return "XMLSERIALIZE(" + quoteIdentifier(c.Name) + " AS CLOB(2G))"
	}
	return quoteIdentifier(c.Name)
}

func (c db2LOBColumn) units() string {
	if c.Type.Name == "BLOB" {
		return ""
	}
	return ", CODEUNITS32"
}

func (c db2LOBColumn) lengthAlias() string {
	return fmt.Sprintf("__FLOW_LOB_LENGTH_%d", c.Index)
}

func (c db2LOBColumn) lengthProjection() string {
	return "LENGTH(" + c.expression() + c.units() + ") AS " + quoteIdentifier(c.lengthAlias())
}

func recoverLOBs(ctx context.Context, conn *sql.DB, from string, key []any, columns []db2LOBColumn, lengths []sql.NullInt64, values []any, allowChanges bool) error {
	for i, column := range columns {
		if values[column.Index] != nil || !lengths[i].Valid {
			continue
		}
		if len(key) == 0 {
			return fmt.Errorf("cannot recover non-null LOB column %q from %s without a key; configure a non-null collection key or add a primary key", column.Name, from)
		}
		// go_ibm_db v0.5.4 narrows SQLLEN to int16, mistaking 65535 + n*65536 bytes for NULL.
		var data, err = column.read(ctx, conn, from, key, lengths[i].Int64, allowChanges)
		if err != nil {
			return fmt.Errorf("cannot recover non-null LOB column %q from %s (key %v): %w", column.Name, from, key, err)
		}
		if data != nil {
			values[column.Index] = data
		}
	}
	return nil
}

func (c db2LOBColumn) read(ctx context.Context, conn *sql.DB, from string, key []any, length int64, allowChanges bool) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid LOB length %d", length)
	}
	var data = make([]byte, 0)
	for start := int64(0); start < length; start += 4096 {
		// At most 4096 bytes or Unicode characters keeps every piece below 65535 bytes.
		var count = min(int64(4096), length-start)
		var query = fmt.Sprintf("SELECT SUBSTRING(%s, %d, %d%s) FROM %s", c.expression(), start+1, count, c.units(), from)
		var value any
		if err := conn.QueryRowContext(ctx, query, key...).Scan(&value); err != nil {
			if allowChanges && err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if allowChanges && value == nil {
			return nil, nil
		}
		var piece []byte
		switch value := value.(type) {
		case string:
			piece = []byte(value)
		case []byte:
			piece = value
		default:
			return nil, fmt.Errorf("unexpected LOB piece type %T at offset %d", value, start+1)
		}
		var size = len(piece)
		if c.Type.Name != "BLOB" {
			if !utf8.Valid(piece) {
				return nil, fmt.Errorf("invalid UTF-8 in LOB piece at offset %d", start+1)
			}
			size = utf8.RuneCount(piece)
		}
		if !allowChanges && int64(size) != count {
			return nil, fmt.Errorf("LOB piece at offset %d has length %d, expected %d", start+1, size, count)
		}
		data = append(data, piece...)
		if int64(size) < count {
			break
		}
	}
	return data, nil
}
