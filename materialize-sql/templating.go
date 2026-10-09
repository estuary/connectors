package sql

import (
	"encoding/base64"
	"fmt"
	"strings"
	"text/template"
	"time"

	log "github.com/sirupsen/logrus"
)

type ColumnWithAlias struct {
	Column
	Alias string
}

func (c ColumnWithAlias) AsFlatType() FlatType {
	t, _ := c.Column.AsFlatType()
	return t
}

func (c ColumnWithAlias) Format() string {
	return c.Inference.String_.Format
}

// MustParseTemplate is a convenience which parses the template `body` and
// installs common functions for accessing Dialect behavior.
func MustParseTemplate(dialect Dialect, name, body string, extraFuncs ...template.FuncMap) *template.Template {
	funcs := template.FuncMap{
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
		"Backtick":   func() string { return "`" }, // Go string literals don't allow a ` character
		"ColumnWithAlias": func(c Column, alias string) ColumnWithAlias {
			return ColumnWithAlias{Column: c, Alias: alias}
		},
		"ChunkColumns": func(cols []*Column, size int) [][]*Column {
			if size <= 0 {
				return [][]*Column{cols}
			}
			var chunks [][]*Column
			for i := 0; i < len(cols); i += size {
				end := i + size
				if end > len(cols) {
					end = len(cols)
				}
				chunks = append(chunks, cols[i:end])
			}
			return chunks
		},
	}
	for _, extra := range extraFuncs {
		for k, v := range extra {
			funcs[k] = v
		}
	}
	return template.Must(template.New(name).Funcs(funcs).Parse(body))
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
	Column
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
	keyColumns []Column
	literaler  func(any) string
	// datetimeBounds reports whether a date-time key column gets bounds.
	datetimeBounds func(Column) bool

	lower []any
	upper []any
	// unbounded marks key columns whose observed values can't be ordered,
	// such as a date-time that doesn't parse.
	unbounded []bool
}

type MergeBoundsOption func(*MergeBoundsBuilder)

// WithDatetimeBounds enables bounds on date-time keys whose column satisfies
// fn. Date-time keys are otherwise excluded, since a value's precision can
// exceed the column's and an endpoint may compare a bound literal differently
// from the stored value.
func WithDatetimeBounds(fn func(Column) bool) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.datetimeBounds = fn }
}

func NewMergeBoundsBuilder(keyColumns []Column, literaler func(any) string, opts ...MergeBoundsOption) *MergeBoundsBuilder {
	var b = &MergeBoundsBuilder{
		keyColumns: keyColumns,
		literaler:  literaler,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// NextKey updates the observed minimum and maximum key for a transaction.
func (b *MergeBoundsBuilder) NextKey(key []any) {
	if len(key) != len(b.keyColumns) {
		panic(fmt.Sprintf("application error: %d key fields vs. %d key columns for merge query bounds", len(key), len(b.keyColumns)))
	}

	if b.lower == nil {
		// Initialize the tracked values if this is the first key observed for
		// the transaction. A new array is allocated for both the tracked upper
		// and lower values so that they can be updated separately based on the
		// observed minimum and maximum keys for subsequent Stores. Note that
		// b.lower and b.upper are set to `nil` on initialization of a
		// MergeBoundsBuilder, and also after calls to `Build`.
		b.lower = append([]any(nil), key...)
		b.upper = append([]any(nil), key...)
		b.unbounded = make([]bool, len(key))
	}

	for idx, k := range key {
		if b.isDatetime(idx) {
			// Date-time strings order by their instant, which text order
			// doesn't follow across offsets and precisions.
			if lower, upper, ok := minMaxDatetime(b.lower[idx], b.upper[idx], k); ok {
				b.lower[idx], b.upper[idx] = lower, upper
			} else {
				b.unbounded[idx] = true
			}
			continue
		}
		b.lower[idx] = minKey(b.lower[idx], k)
		b.upper[idx] = maxKey(b.upper[idx], k)
	}
}

func (b *MergeBoundsBuilder) isDatetime(idx int) bool {
	var col = b.keyColumns[idx]
	var ft, _ = col.AsFlatType()
	return ft == STRING && col.Inference.String_ != nil && col.Inference.String_.Format == "date-time" && b.datetimeBounds != nil && b.datetimeBounds(col)
}

// minMaxDatetime returns the earlier of lower and k, and the later of upper
// and k, compared as instants. It reports false if any isn't a date-time.
func minMaxDatetime(lower, upper, k any) (any, any, bool) {
	var parse = func(v any) (time.Time, bool) {
		s, ok := v.(string)
		if !ok {
			return time.Time{}, false
		}
		t, _, err := ParseRFC3339Nano(s)
		return t, err == nil
	}
	tl, okL := parse(lower)
	tu, okU := parse(upper)
	tk, okK := parse(k)
	if !okL || !okU || !okK {
		return nil, nil, false
	}
	if tk.Before(tl) {
		lower = k
	}
	if tk.After(tu) {
		upper = k
	}
	return lower, upper, true
}

func minKey(k1 any, k2 any) any {
	switch k1 := k1.(type) {
	case bool:
		// Booleans are not comparable.
		return false
	case string:
		return min(k1, k2.(string))
	case int64:
		return min(k1, k2.(int64))
	case uint64:
		return min(k1, k2.(uint64))
	case float64:
		return min(k1, k2.(float64))
	default:
		panic(fmt.Sprintf("minKey unhandled key type %T (value: %v)", k1, k1))
	}
}

func maxKey(k1 any, k2 any) any {
	switch k1 := k1.(type) {
	case bool:
		// Booleans are not comparable.
		return false
	case string:
		return max(k1, k2.(string))
	case int64:
		return max(k1, k2.(int64))
	case uint64:
		return max(k1, k2.(uint64))
	case float64:
		return max(k1, k2.(float64))
	default:
		panic(fmt.Sprintf("maxKey unhandled key type %T (value: %v)", k1, k1))
	}
}

// Build outputs the computed merge conditions for this transaction and resets
// the tracked values in preparation for the next transaction.
func (b *MergeBoundsBuilder) Build() []MergeBound {
	conditions := make([]MergeBound, len(b.lower))

	for idx, col := range b.keyColumns {
		conditions[idx] = MergeBound{Column: col}

		ft, _ := col.AsFlatType()
		if ft == BOOLEAN {
			// Boolean keys cannot reasonably support bounds for merge queries.
			continue
		} else if ft == BINARY {
			// Binary keys could in principal be used as merge query bounds, but
			// the complexity and overhead of comparing their binary values is
			// probably not worth it.
			continue
		} else if ft == STRING && col.Inference.String_ != nil && col.Inference.String_.Format == "date-time" && !b.isDatetime(idx) {
			// At least one destination (BigQuery) has known issues with keys
			// that are date-times with sub-microsecond precision when used as a
			// merge bound, so date-time keys get bounds only where the dialect
			// opts in.
			continue
		} else if b.unbounded[idx] {
			continue
		}

		conditions[idx].LiteralLower = b.literaler(b.lower[idx])
		conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
	}

	// Reset for tracking the next transaction.
	b.Reset()

	return conditions
}

// Reset clears the current bounds to begin tracking a new range.
func (b *MergeBoundsBuilder) Reset() {
	b.lower = nil
	b.upper = nil
	b.unbounded = nil
}
