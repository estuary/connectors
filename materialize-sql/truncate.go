package sql

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// flowPublishedAtPtr is the JSON pointer flow assigns to the projection
// that carries a document's UUID-encoded publication time.
const flowPublishedAtPtr = "/_meta/uuid"

// TruncationBoundary is the whole-second boundary a truncation statement
// compares against, so a column stored at millisecond precision cannot
// floor a row published after the true boundary to a value below it.
func TruncationBoundary(before time.Time) time.Time {
	return before.UTC().Truncate(time.Second)
}

// FlowPublishedAtColumn returns the table's column materialized from the
// document's UUID projection when that column holds a date-time, and an
// explanatory reason when it does not.
func (t *Table) FlowPublishedAtColumn() (*Column, string) {
	for _, col := range t.Columns() {
		if col.Ptr != flowPublishedAtPtr {
			continue
		}

		if col.Inference.String_ == nil || col.Inference.String_.Format != "date-time" {
			return nil, "flow_published_at column is not a timestamp"
		}

		var fc FieldConfig
		if raw, ok := t.FieldConfigJsonMap[col.Field]; ok {
			if err := json.Unmarshal(raw, &fc); err != nil {
				return nil, "flow_published_at column has an invalid field configuration"
			}
		}
		if fc.CastToString() || fc.DDL != "" {
			return nil, "flow_published_at column is not a timestamp"
		}

		return col, ""
	}

	return nil, "table has no flow_published_at column"
}

type truncateTemplateData struct {
	Table    Table
	Column   *Column
	Boundary string
}

// TruncateStatement renders a DELETE of the table's rows published before
// the truncation boundary.
func TruncateStatement(dialect Dialect, table Table, before time.Time) (string, error) {
	column, reason := table.FlowPublishedAtColumn()
	if reason != "" {
		return "", errors.New(reason)
	}

	var tpl = MustParseTemplate(dialect, "truncateStatement",
		`DELETE FROM {{ $.Table.Identifier }} WHERE {{ $.Column.Identifier }} < {{ Literal $.Boundary }};`)

	var w strings.Builder
	if err := tpl.Execute(&w, truncateTemplateData{
		Table:    table,
		Column:   column,
		Boundary: TruncationBoundary(before).Format(time.RFC3339),
	}); err != nil {
		return "", err
	}
	return w.String(), nil
}
