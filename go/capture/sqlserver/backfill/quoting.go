package backfill

import "strings"

// QuoteIdentifier quotes a SQL Server identifier (column name, table name, schema name, etc.)
// by enclosing it in brackets. Any embedded `]` characters are escaped by doubling them.
// From https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers
// it is always valid to enclose an identifier in brackets.
func QuoteIdentifier(name string) string {
	return `[` + strings.ReplaceAll(name, `]`, `]]`) + `]`
}

// QuoteTableName returns a fully qualified and quoted table name in the form [schema].[table].
func QuoteTableName(schema, table string) string {
	return QuoteIdentifier(schema) + "." + QuoteIdentifier(table)
}
