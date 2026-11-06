package backfill

import (
	"fmt"
	"strings"
	"time"

	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
)

// BuildBackfillQuery constructs the appropriate SQL query and arguments for scanning
// a table chunk based on the current backfill state. It handles both keyless backfills
// (using physical location ordering) and keyed backfills (using primary key ordering).
func BuildBackfillQuery(
	state *sqlcapture.TableState,
	schema, table string,
	columnTypes map[string]any,
	chunkSize int,
	backfillFilter string,
) (string, []any, error) {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var keyColumns = state.KeyColumns

	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		var query = keylessScanQuery(schema, table, chunkSize, backfillFilter)
		var args = []any{state.BackfilledCount}
		return query, args, nil

	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		if state.Scanned != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(state.Scanned, datatypes.DecodeKeyFDB)
			if err != nil {
				return "", nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return "", nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
			}
			for idx, colName := range keyColumns {
				var retyped, err = retypeResumeKey(resumeKey[idx], columnTypes[colName])
				if err != nil {
					return "", nil, fmt.Errorf("error preparing resume key for %q: %w", streamID, err)
				}
				resumeKey[idx] = retyped
			}
			var query = buildScanQuery(false, keyColumns, columnTypes, schema, table, chunkSize, backfillFilter)
			return query, resumeKey, nil
		}
		var query = buildScanQuery(true, keyColumns, columnTypes, schema, table, chunkSize, backfillFilter)
		return query, nil, nil

	default:
		return "", nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}
}

// retypeResumeKey converts a resume key value back into a Go type which the driver will
// bind as the column's own SQL type.
//
// EncodeKeyFDB formats datetime values with time.Format so that serialized row keys sort
// correctly as FDB tuples. Binding that string straight back into a chunk query
// leaves SQL Server comparing a datetime column against an nvarchar parameter. The
// disjunctive shape of the resume predicate combined with that implicit conversion
// stops the optimizer from recognizing that `k1 > @p1` and `k1 = @p1 AND k2 > @p2`
// share a lower bound. Unable to derive a seek range, it scans from the start of the
// index instead, and every chunk re-reads the already backfilled portion of the table.
// So, we use retypeResumeKey to convert the resume key so SQL Server does not perform
// an implicit conversion and the optimizer decides to seek instead of scan.
//
// DATETIME and TIME are deliberately excluded:
//
//   - DATETIME is a lossy-encoding problem. DatetimeKeyEncoding keeps only three
//     fractional digits and Go truncates them, but the column resolves to
//     1/300s, so a stored .003333 is persisted as .003.
//
//   - TIME is a parameter-type problem. A Go time.Time is always a date as well as a
//     time, so the driver can only send it as DATETIMEOFFSET, and SQL Server has no
//     implicit conversion between TIME and DATETIMEOFFSET.
func retypeResumeKey(value any, columnType any) (any, error) {
	var text, isText = value.(string)
	if !isText {
		return value, nil
	}

	var layout string
	switch columnType {
	case "smalldatetime":
		layout = datatypes.DatetimeKeyEncoding
	case "datetime2", "datetimeoffset", "date":
		layout = datatypes.SortableRFC3339Nano
	default:
		return value, nil
	}

	var parsed, err = time.Parse(layout, text)
	if err != nil {
		return nil, fmt.Errorf("error parsing %v resume key %q: %w", columnType, text, err)
	}
	return parsed, nil
}

// keylessScanQuery builds a query for scanning a table without a primary key.
// It uses %%physloc%% ordering and OFFSET/FETCH for pagination.
func keylessScanQuery(schemaName, tableName string, chunkSize int, backfillFilter string) string {
	// Generate a list of individual WHERE clauses which should be ANDed together
	var whereClauses []string
	if backfillFilter != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(%s)", backfillFilter))
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM [%s].[%s]", schemaName, tableName)
	if len(whereClauses) > 0 {
		fmt.Fprintf(query, " WHERE %s", strings.Join(whereClauses, " AND "))
	}
	fmt.Fprintf(query, " ORDER BY %%%%physloc%%%%")
	fmt.Fprintf(query, " OFFSET @p1 ROWS FETCH FIRST %d ROWS ONLY;", chunkSize)
	return query.String()
}

// buildScanQuery builds a query for scanning a table with a primary key.
// If start is true, scans from the beginning; otherwise resumes after the given key values.
// The columnTypes map is used to determine which columns need special text collation handling.
func buildScanQuery(start bool, keyColumns []string, columnTypes map[string]any, schemaName, tableName string, chunkSize int, backfillFilter string) string {
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = QuoteIdentifier(colName)
		if _, ok := columnTypes[colName].(*datatypes.TextColumnType); ok {
			args = append(args, fmt.Sprintf("@KeyColumn%d", idx+1))
		} else {
			args = append(args, fmt.Sprintf("@p%d", idx+1))
		}
		pkey = append(pkey, quotedName)
	}

	// Generate a list of individual WHERE clauses which should be ANDed together
	var whereClauses []string
	if !start {
		var predicate = new(strings.Builder)
		for i := range len(pkey) {
			if i == 0 {
				predicate.WriteString("(")
			} else {
				predicate.WriteString(") OR (")
			}

			for j := 0; j < i; j++ {
				fmt.Fprintf(predicate, "%s = %s AND ", pkey[j], args[j])
			}
			fmt.Fprintf(predicate, "%s > %s", pkey[i], args[i])
		}
		predicate.WriteString(")")
		whereClauses = append(whereClauses, fmt.Sprintf("(%s)", predicate.String()))
	}
	if backfillFilter != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("(%s)", backfillFilter))
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "BEGIN ")
	if !start {
		for idx, colName := range keyColumns {
			if textInfo, ok := columnTypes[colName].(*datatypes.TextColumnType); ok {
				fmt.Fprintf(query, "DECLARE @KeyColumn%[1]d AS %[2]s = @p%[1]d; ", idx+1, textInfo.FullType)
			}
		}
	}
	fmt.Fprintf(query, "SELECT * FROM [%s].[%s]", schemaName, tableName)
	if len(whereClauses) > 0 {
		fmt.Fprintf(query, " WHERE %s", strings.Join(whereClauses, " AND "))
	}
	fmt.Fprintf(query, " ORDER BY %s", strings.Join(pkey, ", "))
	fmt.Fprintf(query, " OFFSET 0 ROWS FETCH FIRST %d ROWS ONLY;", chunkSize)
	fmt.Fprintf(query, " END")
	return query.String()
}
