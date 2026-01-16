package backfill

import (
	"fmt"
	"strings"

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
) (string, []any, error) {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var keyColumns = state.KeyColumns

	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		var query = keylessScanQuery(schema, table, chunkSize)
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
			var query = buildScanQuery(false, keyColumns, columnTypes, schema, table, chunkSize)
			return query, resumeKey, nil
		}
		var query = buildScanQuery(true, keyColumns, columnTypes, schema, table, chunkSize)
		return query, nil, nil

	default:
		return "", nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}
}

// keylessScanQuery builds a query for scanning a table without a primary key.
// It uses %%physloc%% ordering and OFFSET/FETCH for pagination.
func keylessScanQuery(schemaName, tableName string, chunkSize int) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM [%s].[%s]", schemaName, tableName)
	fmt.Fprintf(query, " ORDER BY %%%%physloc%%%%")
	fmt.Fprintf(query, " OFFSET @p1 ROWS FETCH FIRST %d ROWS ONLY;", chunkSize)
	return query.String()
}

// buildScanQuery builds a query for scanning a table with a primary key.
// If start is true, scans from the beginning; otherwise resumes after the given key values.
// The columnTypes map is used to determine which columns need special text collation handling.
func buildScanQuery(start bool, keyColumns []string, columnTypes map[string]any, schemaName, tableName string, chunkSize int) string {
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
	if !start {
		for i := range len(pkey) {
			if i == 0 {
				fmt.Fprintf(query, " WHERE (")
			} else {
				fmt.Fprintf(query, ") OR (")
			}

			for j := 0; j < i; j++ {
				fmt.Fprintf(query, "%s = %s AND ", pkey[j], args[j])
			}
			fmt.Fprintf(query, "%s > %s", pkey[i], args[i])
		}
		fmt.Fprintf(query, ")")
	}
	fmt.Fprintf(query, " ORDER BY %s", strings.Join(pkey, ", "))
	fmt.Fprintf(query, " OFFSET 0 ROWS FETCH FIRST %d ROWS ONLY;", chunkSize)
	fmt.Fprintf(query, " END")
	return query.String()
}
