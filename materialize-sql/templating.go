package sql

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
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
	// WithDatetimeRanges and WithStringRanges.
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
	// datetimeRanges and stringRanges are the most ranges an exact date-time
	// bound and a string bound split into.
	datetimeRanges int
	stringRanges   int
	// maxRanges is the most ranges each key column's bound splits into, or
	// zero if it isn't split.
	maxRanges []int

	lower []any
	upper []any
	// unbounded marks key columns whose observed values can't be ordered,
	// such as a date-time that doesn't parse.
	unbounded []bool
	// spans are the observed values of each key column with maxRanges.
	spans []spanSet
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
	return func(b *MergeBoundsBuilder) { b.datetimeRanges = maxRanges }
}

// WithStringRanges splits the bound of a string key other than a date-time
// into up to maxRanges ranges at the widest gaps between its observed values.
// The ranges order values byte by byte, so they suit only an endpoint that
// compares the key column's values the same way.
func WithStringRanges(maxRanges int) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.stringRanges = maxRanges }
}

func NewMergeBoundsBuilder(keyColumns []Column, literaler func(any) string, opts ...MergeBoundsOption) *MergeBoundsBuilder {
	var b = &MergeBoundsBuilder{
		keyColumns: keyColumns,
		literaler:  literaler,
	}
	for _, o := range opts {
		o(b)
	}
	b.maxRanges = make([]int, len(keyColumns))
	for idx, col := range keyColumns {
		var ft, _ = col.AsFlatType()
		if isDatetimeColumn(col) {
			if b.datetimeRanges > 1 && b.exactDatetime != nil && b.exactDatetime(col) {
				b.maxRanges[idx] = b.datetimeRanges
			}
		} else if ft == STRING && b.stringRanges > 1 {
			b.maxRanges[idx] = b.stringRanges
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
		b.spans = make([]spanSet, len(key))
		for idx, col := range b.keyColumns {
			if b.maxRanges[idx] > 0 && !isDatetimeColumn(col) {
				b.spans[idx].alphabet = new(byteAlphabet)
			}
		}
	}

	for idx, k := range key {
		if isDatetimeColumn(b.keyColumns[idx]) {
			// Date-time strings order by their instant, which text order
			// doesn't follow across offsets and precisions.
			if lower, upper, ts, ok := MinMaxDatetime(b.lower[idx], b.upper[idx], k); ok {
				b.lower[idx], b.upper[idx] = lower, upper
				if b.maxRanges[idx] > 0 && !b.unbounded[idx] {
					b.spans[idx].add(datetimePosition(ts), k, spansPerRange*b.maxRanges[idx])
				}
			} else {
				b.unbounded[idx] = true
			}
			continue
		}
		b.lower[idx] = minKey(b.lower[idx], k)
		b.upper[idx] = maxKey(b.upper[idx], k)
		if s, ok := k.(string); ok && b.maxRanges[idx] > 0 {
			b.spans[idx].add(s, k, spansPerRange*b.maxRanges[idx])
		}
	}
}

func isDatetimeColumn(col Column) bool {
	var ft, _ = col.AsFlatType()
	return ft == STRING && col.Inference.String_ != nil && col.Inference.String_.Format == "date-time"
}

// ParseDatetime parses a date-time key value, reporting false if it isn't one.
func ParseDatetime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, _, err := ParseRFC3339Nano(s)
	return t, err == nil
}

// MinMaxDatetime returns the earlier of lower and k, and the later of upper
// and k, compared as instants, along with the instant of k. It reports false
// if any isn't a date-time.
func MinMaxDatetime(lower, upper, k any) (any, any, time.Time, bool) {
	tl, okL := ParseDatetime(lower)
	tu, okU := ParseDatetime(upper)
	tk, okK := ParseDatetime(k)
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
func (b *MergeBoundsBuilder) Build() MergeBounds {
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
			lower, _ := ParseDatetime(b.lower[idx])
			upper, _ := ParseDatetime(b.upper[idx])
			if lo, hi, ok := dateBounds(lower, upper); ok {
				conditions[idx].LiteralLower = b.literaler(lo)
				conditions[idx].LiteralUpper = b.literaler(hi)
			}
			continue
		}

		conditions[idx].LiteralLower = b.literaler(b.lower[idx])
		conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
		if len(b.spans[idx].spans) > 1 {
			conditions[idx].Ranges = b.rangesOf(&b.spans[idx], b.maxRanges[idx])
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

// keySpan is a closed range of observed keys, by their positions, along with
// the keys at its ends for rendering as literals. A string key is its own
// position.
type keySpan struct {
	lo, hi       string
	loKey, hiKey any
	// gap is the log2 distance from hi to the lo of the next span, if any.
	gap float64
}

// datetimePosition is the position of a date-time key: its instant as a
// big-endian count of nanoseconds, so that the distance between two positions
// is proportional to the time between their instants.
func datetimePosition(ts time.Time) string {
	var hi, lo = bits.Mul64(uint64(ts.Unix())^(1<<63), uint64(time.Second))
	var carry uint64
	lo, carry = bits.Add64(lo, uint64(ts.Nanosecond()), 0)
	var out [16]byte
	binary.BigEndian.PutUint64(out[:8], hi+carry)
	binary.BigEndian.PutUint64(out[8:], lo)
	return string(out[:])
}

// spanSet is the observed keys of a key column as sorted, disjoint spans.
type spanSet struct {
	spans []keySpan
	// alphabet is the symbols of the observed string keys, or nil if
	// positions are measured byte by byte.
	alphabet *byteAlphabet
}

// alphabetPositions is how many leading positions of a string a byteAlphabet
// tracks. Past them, every byte is a digit of its own.
const alphabetPositions = 64

// byteAlphabet is the symbols seen at each position of a set of strings,
// where a symbol is a byte or the end of the string. Reading a string as a
// number in which each position's base is the count of its symbols makes
// distances disregard symbols no string uses there.
type byteAlphabet struct {
	positions []positionSymbols
}

// positionSymbols is the set of symbols seen at one position, where the end
// of a string is symbol 0 and byte c is symbol c+1.
type positionSymbols [5]uint64

// mark adds sym to the set, reporting whether it is new.
func (p *positionSymbols) mark(sym int) bool {
	var w, b = sym / 64, uint64(1) << (sym % 64)
	if p[w]&b != 0 {
		return false
	}
	p[w] |= b
	return true
}

// rank is the number of symbols in the set below sym.
func (p *positionSymbols) rank(sym int) float64 {
	var n int
	for w := range sym / 64 {
		n += bits.OnesCount64(p[w])
	}
	n += bits.OnesCount64(p[sym/64] & (uint64(1)<<(sym%64) - 1))
	return float64(n)
}

// radix is the number of symbols in the set.
func (p *positionSymbols) radix() float64 {
	var n int
	for _, w := range p {
		n += bits.OnesCount64(w)
	}
	return float64(n)
}

func symbolAt(z string, i int) int {
	if i >= len(z) {
		return 0
	}
	return int(z[i]) + 1
}

// add adds the symbols of s, reporting whether any are new.
func (a *byteAlphabet) add(s string) bool {
	var grew bool
	for i := 0; i <= len(s) && i < alphabetPositions; i++ {
		if i == len(a.positions) {
			a.positions = append(a.positions, positionSymbols{})
		}
		if a.positions[i].mark(symbolAt(s, i)) {
			grew = true
		}
	}
	return grew
}

// digit is the digit of z at position i, and that position's base.
func (s *spanSet) digit(z string, i int) (float64, float64) {
	if s.alphabet == nil {
		if i >= len(z) {
			return 0, 256
		}
		return float64(z[i]), 256
	} else if i >= len(s.alphabet.positions) {
		return float64(symbolAt(z, i)), 257
	} else if i > len(z) {
		return 0, s.alphabet.positions[i].radix()
	}
	var pos = &s.alphabet.positions[i]
	return pos.rank(symbolAt(z, i)), pos.radix()
}

// gap is the log2 distance from position x to the greater position y,
// reading each as a fraction whose digits are those of digit, or -Inf if y
// doesn't exceed x.
func (s *spanSet) gap(x, y string) float64 {
	var p int
	for p < len(x) && p < len(y) && x[p] == y[p] {
		p++
	}
	var scale float64
	for i := range p {
		_, radix := s.digit(x, i)
		scale += math.Log2(radix)
	}
	// Digits whose weight falls below 2^-60 are beneath float64 precision.
	var v, w = 0.0, 1.0
	for i := p; i <= max(len(x), len(y)) && w > 0x1p-60; i++ {
		dx, radix := s.digit(x, i)
		dy, _ := s.digit(y, i)
		w /= radix
		v += (dy - dx) * w
	}
	if v <= 0 {
		return math.Inf(-1)
	}
	return math.Log2(v) - scale
}

// add adds the key at position pos, keeping at most limit spans.
func (s *spanSet) add(pos string, key any, limit int) {
	if s.alphabet != nil && s.alphabet.add(pos) {
		for i := 0; i+1 < len(s.spans); i++ {
			s.spans[i].gap = s.gap(s.spans[i].hi, s.spans[i+1].lo)
		}
	}

	var i = sort.Search(len(s.spans), func(i int) bool { return s.spans[i].hi >= pos })
	if i < len(s.spans) && s.spans[i].lo <= pos {
		return
	}
	s.spans = slices.Insert(s.spans, i, keySpan{lo: pos, hi: pos, loKey: key, hiKey: key})
	if i > 0 {
		s.spans[i-1].gap = s.gap(s.spans[i-1].hi, pos)
	}
	if i+1 < len(s.spans) {
		s.spans[i].gap = s.gap(pos, s.spans[i+1].lo)
	}
	if len(s.spans) > limit {
		s.spans = mergeNarrowestGap(s.spans)
	}
}

func mergeNarrowestGap(spans []keySpan) []keySpan {
	var at int
	for i := 1; i+1 < len(spans); i++ {
		if spans[i].gap < spans[at].gap {
			at = i
		}
	}
	return mergeSpans(spans, at)
}

// mergeSpans merges spans[at+1] into spans[at].
func mergeSpans(spans []keySpan, at int) []keySpan {
	spans[at].hi, spans[at].hiKey, spans[at].gap = spans[at+1].hi, spans[at+1].hiKey, spans[at+1].gap
	return slices.Delete(spans, at+1, at+2)
}

// rangesOf renders spans as at most maxRanges ranges split at their widest
// gaps, or nil if no gap is worth a split.
func (b *MergeBoundsBuilder) rangesOf(set *spanSet, maxRanges int) []MergeRange {
	var spans = set.spans
	for len(spans) > maxRanges {
		spans = mergeNarrowestGap(spans)
	}
	var minGap = set.gap(spans[0].lo, spans[len(spans)-1].hi) - math.Log2(minRangeGapDivisor)
	for i := 0; i+1 < len(spans); {
		if spans[i].gap < minGap {
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
