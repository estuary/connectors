package sql

import (
	"encoding/base64"
	"fmt"
	"slices"
	"sort"
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
	// Ranges, when set, split LiteralLower..LiteralUpper at gaps between the
	// observed values: every observed value lies within one of them. See
	// WithDatetimeRanges.
	Ranges []MergeRange
}

// MergeRange is one of the disjoint ranges of a MergeBound.
type MergeRange struct {
	LiteralLower string
	LiteralUpper string
}

// MergeBoundsBuilder tracks and generates a MergeBound for each of a binding's
// key fields.
type MergeBoundsBuilder struct {
	keyColumns []Column
	literaler  func(any) string
	// exactDatetime reports whether a date-time key column can be bounded by
	// the observed values themselves rather than by whole dates.
	exactDatetime func(Column) bool
	// maxRanges is the most ranges an exact date-time bound splits into, and
	// rangeColumns marks the key columns it applies to.
	maxRanges    int
	rangeColumns []bool

	lower []any
	upper []any
	// unbounded marks key columns whose observed values can't be ordered,
	// such as a date-time that doesn't parse.
	unbounded []bool
	// spans are the observed values of each of rangeColumns, as sorted,
	// disjoint spans.
	spans [][]datetimeSpan
}

type MergeBoundsOption func(*MergeBoundsBuilder)

// WithExactDatetimeBounds bounds date-time keys whose column satisfies fn by
// the observed values. Other date-time keys are bounded by whole dates, one
// day either side of the observed range, which no RFC3339 spelling or
// precision of a stored value can fall outside of.
func WithExactDatetimeBounds(fn func(Column) bool) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.exactDatetime = fn }
}

// WithDatetimeRanges splits the exact bound of a date-time key into up to
// maxRanges ranges at the widest gaps between its observed values, so that a
// few outlying keys don't stretch the bound over everything in between.
func WithDatetimeRanges(maxRanges int) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.maxRanges = maxRanges }
}

func NewMergeBoundsBuilder(keyColumns []Column, literaler func(any) string, opts ...MergeBoundsOption) *MergeBoundsBuilder {
	var b = &MergeBoundsBuilder{
		keyColumns: keyColumns,
		literaler:  literaler,
	}
	for _, o := range opts {
		o(b)
	}
	if b.maxRanges > 1 && b.exactDatetime != nil {
		b.rangeColumns = make([]bool, len(keyColumns))
		for idx, col := range keyColumns {
			b.rangeColumns[idx] = isDatetimeColumn(col) && b.exactDatetime(col)
		}
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
		b.spans = make([][]datetimeSpan, len(key))
	}

	for idx, k := range key {
		if isDatetimeColumn(b.keyColumns[idx]) {
			// Date-time strings order by their instant, which text order
			// doesn't follow across offsets and precisions.
			if lower, upper, ts, ok := minMaxDatetime(b.lower[idx], b.upper[idx], k); ok {
				b.lower[idx], b.upper[idx] = lower, upper
				if b.rangeColumns != nil && b.rangeColumns[idx] && !b.unbounded[idx] {
					b.spans[idx] = addDatetimeSpan(b.spans[idx], ts, k, spansPerRange*b.maxRanges)
				}
			} else {
				b.unbounded[idx] = true
			}
			continue
		}
		b.lower[idx] = minKey(b.lower[idx], k)
		b.upper[idx] = maxKey(b.upper[idx], k)
	}
}

func isDatetimeColumn(col Column) bool {
	var ft, _ = col.AsFlatType()
	return ft == STRING && col.Inference.String_ != nil && col.Inference.String_.Format == "date-time"
}

func parseDatetime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, _, err := ParseRFC3339Nano(s)
	return t, err == nil
}

// minMaxDatetime returns the earlier of lower and k, and the later of upper
// and k, compared as instants, along with the instant of k. It reports false
// if any isn't a date-time.
func minMaxDatetime(lower, upper, k any) (any, any, time.Time, bool) {
	tl, okL := parseDatetime(lower)
	tu, okU := parseDatetime(upper)
	tk, okK := parseDatetime(k)
	if !okL || !okU || !okK {
		return nil, nil, time.Time{}, false
	}
	if tk.Before(tl) {
		lower = k
	}
	if tk.After(tu) {
		upper = k
	}
	return lower, upper, tk, true
}

// dateBounds widens lower..upper to date literals that enclose every spelling
// of an instant in range: a value's text starts with its wall-clock date,
// which its offset can move up to a day from the instant. The upper literal
// is one day further so that values on the last day, which sort after the
// bare date as text, still fall inside. The bounds are dropped if they would
// leave the years an endpoint accepts.
func dateBounds(lower, upper time.Time) (string, string, bool) {
	var lo = lower.UTC().AddDate(0, 0, -1)
	var hi = upper.UTC().AddDate(0, 0, 2)
	if lo.Year() < 1 || hi.Year() > 9999 {
		return "", "", false
	}
	return lo.Format(time.DateOnly), hi.Format(time.DateOnly), true
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
		} else if b.unbounded[idx] {
			continue
		} else if isDatetimeColumn(col) && (b.exactDatetime == nil || !b.exactDatetime(col)) {
			// Endpoints compare a date-time literal to a stored value with
			// differing precision or, for text columns, as text, so the bound
			// is widened to dates to be safe under either.
			lower, _ := parseDatetime(b.lower[idx])
			upper, _ := parseDatetime(b.upper[idx])
			if lo, hi, ok := dateBounds(lower, upper); ok {
				conditions[idx].LiteralLower = b.literaler(lo)
				conditions[idx].LiteralUpper = b.literaler(hi)
			}
			continue
		}

		conditions[idx].LiteralLower = b.literaler(b.lower[idx])
		conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
		if len(b.spans[idx]) > 1 {
			conditions[idx].Ranges = b.rangesOf(b.spans[idx])
		}
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
	b.spans = nil
}

// spansPerRange is how many spans per output range a builder tracks. Past
// that, the two spans with the narrowest gap between them merge.
const spansPerRange = 8

// minRangeGapDivisor sets the narrowest gap worth splitting a bound at, as a
// fraction of the bound's span: a narrower one excludes too little to matter.
const minRangeGapDivisor = 100

// datetimeSpan is a closed range of observed date-time keys, along with the
// keys at its ends for rendering as literals.
type datetimeSpan struct {
	lo, hi       time.Time
	loKey, hiKey any
}

// addDatetimeSpan adds the observed key ts to spans, which hold at most limit
// spans.
func addDatetimeSpan(spans []datetimeSpan, ts time.Time, key any, limit int) []datetimeSpan {
	var i = sort.Search(len(spans), func(i int) bool { return !spans[i].hi.Before(ts) })
	if i < len(spans) && !spans[i].lo.After(ts) {
		return spans
	}
	spans = slices.Insert(spans, i, datetimeSpan{lo: ts, hi: ts, loKey: key, hiKey: key})
	if len(spans) > limit {
		spans = mergeNarrowestGap(spans)
	}
	return spans
}

func mergeNarrowestGap(spans []datetimeSpan) []datetimeSpan {
	var at int
	for i := 1; i+1 < len(spans); i++ {
		if spans[i+1].lo.Sub(spans[i].hi) < spans[at+1].lo.Sub(spans[at].hi) {
			at = i
		}
	}
	return mergeSpans(spans, at)
}

// mergeSpans merges spans[at+1] into spans[at].
func mergeSpans(spans []datetimeSpan, at int) []datetimeSpan {
	spans[at].hi, spans[at].hiKey = spans[at+1].hi, spans[at+1].hiKey
	return slices.Delete(spans, at+1, at+2)
}

// rangesOf renders spans as at most maxRanges ranges split at their widest
// gaps, or nil if no gap is worth a split.
func (b *MergeBoundsBuilder) rangesOf(spans []datetimeSpan) []MergeRange {
	for len(spans) > b.maxRanges {
		spans = mergeNarrowestGap(spans)
	}
	var minGap = spans[len(spans)-1].hi.Sub(spans[0].lo) / minRangeGapDivisor
	for i := 0; i+1 < len(spans); {
		if spans[i+1].lo.Sub(spans[i].hi) < minGap {
			spans = mergeSpans(spans, i)
		} else {
			i++
		}
	}
	if len(spans) < 2 {
		return nil
	}

	var out = make([]MergeRange, len(spans))
	for i, s := range spans {
		out[i] = MergeRange{LiteralLower: b.literaler(s.loKey), LiteralUpper: b.literaler(s.hiKey)}
	}
	return out
}
