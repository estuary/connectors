package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	//"github.com/sirupsen/logrus"
)

var statementTimeoutRegexp = regexp.MustCompile(`canceling statement due to statement timeout`)

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *oracleDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event *sqlcapture.ChangeEvent) error) (bool, error) {
	return false, nil
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *oracleDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	return nil
}

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *oracleDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// The set of column types for which we need to specify `COLLATE "C"` to get
// proper ordering and comparison. Represented as a map[string]bool so that it can be
// combined with the "is the column typename a string" check into one if statement.
var columnBinaryKeyComparison = map[string]bool{
	"varchar": true,
	"bpchar":  true,
	"text":    true,
}

func (db *oracleDatabase) keylessScanQuery(info *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, `SELECT ctid, * FROM "%s"."%s"`, schemaName, tableName)
	fmt.Fprintf(query, ` WHERE ctid > $1`)
	fmt.Fprintf(query, ` LIMIT %d;`, db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func (db *oracleDatabase) buildScanQuery(start, isPrecise bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	return ""
}

func quoteColumnName(name string) string {
	// From https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:
	//
	//     Quoted identifiers can contain any character, except the character with code zero.
	//     (To include a double quote, write two double quotes.)
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (db *oracleDatabase) explainQuery(ctx context.Context, streamID, query string, args []interface{}) {
}
