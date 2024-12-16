package sql

import (
	"encoding/base64"
	"fmt"
	"strings"
	"text/template"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	log "github.com/sirupsen/logrus"
)

// MustParseTemplate is a convenience which parses the template `body` and
// installs common functions for accessing Dialect behavior.
func MustParseTemplate(dialect Dialect, name, body string) *template.Template {
	var tpl = template.New(name).Funcs(template.FuncMap{
		"Literal":          dialect.Literal,
		"Base64Std":        base64.StdEncoding.EncodeToString,
		"ColumnIdentifier": dialect.Identifier,
		// Tweak signature slightly to take TablePath, as dynamic slicing is a bit tricky
		// in templates and this is most-frequently used with TablePath.Base().
		"Identifier": func(p TablePath) string { return dialect.Identifier(p...) },
		"Join":       func(s []string, delim string) string { return strings.Join(s, delim) },
		"Split":      func(s string, delim string) []string { return strings.Split(s, delim) },
		"Repeat":     func(n int) []bool { return make([]bool, n) },
		"Add":        func(a, b int) int { return a + b },
		"Contains":   func(s string, substr string) bool { return strings.Contains(s, substr) },
		"Last":       func(s []string) string { return s[len(s)-1] },
		"First":      func(s []string) string { return s[0] },
	})
	return template.Must(tpl.Parse(body))
}

// RenderTableTemplate is a simple implementation of rendering a template with a Table
// as its context. It's here for demonstration purposes mostly. Feel free to not use it.
func RenderTableTemplate(table Table, tpl *template.Template) (string, error) {
	var w strings.Builder
	if err := tpl.Execute(&w, &table); err != nil {
		return "", err
	}
	var s = w.String()
	log.WithField("rendered", s).WithField("table", table).Debug("rendered template")
	return s, nil
}

// MergeBound represents an identifier for which a merge query should use an
// equality comparison for, as well as an optional lower and upper bound for the
// range of values the merge should apply to in the target table. These value
// ranges are known for each transaction from the range of observed keys to
// store, and providing the range hints in the merge query directly may allow
// for warehouses to do additional optimizations when executing the merge.
type MergeBound struct {
	// Identifier is the identifier for the key column this bound applies to,
	// with the dialect's quoting applied.
	Identifier string
	// LiteralLower will be an empty string if no condition should be used,
	// which is the case for boolean keys.
	LiteralLower string
	// LiteralUpper will also be an empty string if no condition should be
	// used.
	LiteralUpper string
}

// MergeBoundsBuilder tracks and generates a MergeBound for each of a binding's
// key fields.
type MergeBoundsBuilder struct {
	table     Table
	literaler func(any) string

	lower tuple.Tuple
	upper tuple.Tuple
}

func NewMergeBoundsBuilder(table Table, literaler func(any) string) *MergeBoundsBuilder {
	return &MergeBoundsBuilder{
		table:     table,
		literaler: literaler,
	}
}

// NextStore updates the observed minimum and maximum key for a transaction. It
// relies on the fact that Stores are sent to materializations in ascending key
// order, so the first observed Store will have the minimum key and the final
// observed store will have the maximum key.
func (b *MergeBoundsBuilder) NextStore(key tuple.Tuple) {
	if len(key) != len(b.table.Keys) {
		panic(fmt.Sprintf("application error: %d key fields vs. %d key columns for merge query bounds", len(key), len(b.table.Keys)))
	}

	if b.lower == nil {
		b.lower = key
	}
	b.upper = key
}

// Build outputs the computed merge conditions for this transaction and resets
// the tracked values in preparation for the next transaction.
func (b *MergeBoundsBuilder) Build() ([]MergeBound, error) {
	conditions := make([]MergeBound, len(b.lower))

	convertedLower, err := b.table.ConvertKey(b.lower)
	if err != nil {
		return nil, fmt.Errorf("converting lower bound: %w", err)
	}
	convertedUpper, err := b.table.ConvertKey(b.upper)
	if err != nil {
		return nil, fmt.Errorf("converting upper bound: %w", err)
	}

	for idx, key := range b.table.Keys {
		conditions[idx] = MergeBound{
			Identifier: key.Identifier,
		}

		lower := convertedLower[idx]
		upper := convertedUpper[idx]

		if _, ok := lower.(bool); ok {
			// Boolean keys cannot reasonably support bounds for merge queries.
			// It is assumed that if the lower value is a boolean type then the
			// upper value must be as well since we do not allow keys with
			// multiple types in SQL materializations.
			continue
		}

		conditions[idx].LiteralLower = b.literaler(lower)
		conditions[idx].LiteralUpper = b.literaler(upper)
	}

	b.lower = nil
	b.upper = nil

	return conditions, nil
}
