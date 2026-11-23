package client

import "strings"

func QuoteTableName(schema, table string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}

// identifierEscaper escapes backslashes as well as backticks because BigQuery
// reads backslash escape sequences inside quoted identifiers.
var identifierEscaper = strings.NewReplacer("\\", "\\\\", "`", "\\`")

func QuoteIdentifier(name string) string {
	return "`" + identifierEscaper.Replace(name) + "`"
}
