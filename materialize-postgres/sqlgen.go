package main

import (
	"fmt"
	"strings"
	"time"

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
				ByFormat: map[string]sql.TypeMapper{
					// This format is for RFC3339 timestamps. As of this writing, Flow's schema
					// validation does not actually validate that String values match their declared
					// format, so there's no guarantee that the values will parse successfully here.
					"date-time": sql.ConstColumnType{
						SQLType: "TIMESTAMPTZ",
						ValueConverter: func(in interface{}) (interface{}, error) {
							return time.Parse(time.RFC3339Nano, in.(string))
						},
					},
				},
			},
		},
	}

	return sql.Generator{
		CommentRenderer:    sql.LineCommentRenderer(),
		IdentifierRenderer: sql.NewRenderer(nil, sql.DoubleQuotesWrapper(), SkipWrapper),
		ValueRenderer:      sql.NewRenderer(sql.DefaultQuoteSanitizer, sql.SingleQuotesWrapper(), nil),
		Placeholder:        PostgresParameterPlaceholder,
		TypeMappings:       typeMappings,
	}
}

func SkipWrapper(identifier string) bool {
	return sql.DefaultUnwrappedIdentifiers(identifier) && !sliceContains(strings.ToLower(identifier), PG_RESERVED_WORDS)
}

func sliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}

// PostgresParameterPlaceholder returns $N style parameters where N is the parameter number
// starting at 1.
func PostgresParameterPlaceholder(parameterIndex int) string {
	// parameterIndex starts at 0, but postgres parameters start at $1
	return fmt.Sprintf("$%d", parameterIndex+1)
}
