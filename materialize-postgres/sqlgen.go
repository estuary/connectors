package main

import (
	"fmt"

	"github.com/estuary/flow/go/protocols/materialize/sql"
)

// PostgresSQLGenerator returns a SQLGenerator for the postgresql SQL dialect.
func PostgresSQLGenerator() sql.Generator {
	var typeMappings sql.TypeMapper = sql.NullableTypeMapping{
		NotNullText: "NOT NULL",
		Inner: sql.ColumnTypeMapper{
			sql.INTEGER: sql.RawConstColumnType("BIGINT"),
			sql.NUMBER:  sql.RawConstColumnType("DOUBLE PRECISION"),
			sql.BOOLEAN: sql.RawConstColumnType("BOOLEAN"),
			sql.OBJECT:  sql.RawConstColumnType("JSON"),
			sql.ARRAY:   sql.RawConstColumnType("JSON"),
			sql.BINARY:  sql.RawConstColumnType("BYTEA"),
			sql.STRING: sql.StringTypeMapping{
				Default: sql.RawConstColumnType("TEXT"),
			},
		},
	}

	return sql.Generator{
		CommentRenderer:    sql.LineCommentRenderer(),
		IdentifierRenderer: sql.NewRenderer(nil, sql.DoubleQuotesWrapper(), sql.DefaultUnwrappedIdentifiers),
		ValueRenderer:      sql.NewRenderer(sql.DefaultQuoteSanitizer, sql.SingleQuotesWrapper(), nil),
		Placeholder:        PostgresParameterPlaceholder,
		TypeMappings:       typeMappings,
	}
}

// PostgresParameterPlaceholder returns $N style parameters where N is the parameter number
// starting at 1.
func PostgresParameterPlaceholder(parameterIndex int) string {
	// parameterIndex starts at 0, but postgres parameters start at $1
	return fmt.Sprintf("$%d", parameterIndex+1)
}
