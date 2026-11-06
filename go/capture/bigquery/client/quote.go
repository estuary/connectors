package client

import "strings"

func QuoteTableName(schema, table string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}

func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "\\`") + "`"
}
