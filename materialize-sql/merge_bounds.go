package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"
	"time"
)

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

// maxBoundRanges is the most ranges a key's merge bound splits into.
const maxBoundRanges = 8

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
	// datetimeRanges and stringRanges are whether an exact date-time bound
	// and a string bound split into ranges.
	datetimeRanges bool
	stringRanges   bool
	// maxRanges is the most ranges a bound splits into.
	maxRanges int
	// datetime marks the date-time key columns.
	datetime []bool

	lower []any
	upper []any
	// unbounded marks key columns whose observed values can't be ordered,
	// such as a date-time that doesn't parse.
	unbounded []bool
	// spans are the observed values of each key column whose bound splits
	// into ranges, and nil for the others.
	spans []*spanSet
}

type MergeBoundsOption func(*MergeBoundsBuilder)

// WithExactDatetimeBounds bounds date-time keys whose column satisfies fn by
// the observed values. Other date-time keys are bounded by whole dates, one
// day either side of the observed range, which no RFC3339 spelling or
// precision of a stored value can fall outside of.
func WithExactDatetimeBounds(fn func(Column) bool) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.exactDatetime = fn }
}

// WithDatetimeRanges splits the exact bound of a date-time key into ranges at
// the widest gaps between its observed values, so that a few outlying keys
// don't stretch the bound over everything in between.
func WithDatetimeRanges() MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.datetimeRanges = true }
}

// WithStringRanges splits the bound of a string key other than a date-time
// into ranges at the widest gaps between its observed values. The ranges order
// values byte by byte, so they suit only an endpoint that compares the key
// column's values the same way.
func WithStringRanges() MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.stringRanges = true }
}

func NewMergeBoundsBuilder(keyColumns []Column, literaler func(any) string, opts ...MergeBoundsOption) *MergeBoundsBuilder {
	var b = &MergeBoundsBuilder{
		keyColumns: keyColumns,
		literaler:  literaler,
		maxRanges:  maxBoundRanges,
	}
	for _, o := range opts {
		o(b)
	}
	b.datetime = make([]bool, len(keyColumns))
	b.spans = make([]*spanSet, len(keyColumns))
	var limit = spansPerRange * b.maxRanges
	for idx, col := range keyColumns {
		var ft, _ = col.AsFlatType()
		switch {
		case isDatetimeColumn(col):
			b.datetime[idx] = true
			if b.datetimeRanges && b.exactDatetime != nil && b.exactDatetime(col) {
				b.spans[idx] = &spanSet{limit: limit, scale: instantScale{}}
			}
		case ft != STRING || !b.stringRanges:
		case isDateColumn(col):
			b.spans[idx] = &spanSet{limit: limit, scale: instantScale{}, position: datePosition}
		default:
			b.spans[idx] = &spanSet{limit: limit, scale: new(byteAlphabet), position: stringPosition}
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
	}

	for idx, k := range key {
		var spans = b.spans[idx]
		if b.datetime[idx] {
			// Date-time strings order by their instant, which text order
			// doesn't follow across offsets and precisions.
			if lower, upper, ts, ok := MinMaxDatetime(b.lower[idx], b.upper[idx], k); ok {
				b.lower[idx], b.upper[idx] = lower, upper
				if spans != nil && !b.unbounded[idx] {
					spans.add(datetimePosition(ts), k)
				}
			} else {
				b.unbounded[idx] = true
			}
			continue
		}
		b.lower[idx] = minKey(b.lower[idx], k)
		b.upper[idx] = maxKey(b.upper[idx], k)
		if spans != nil {
			spans.observe(k)
		}
	}
}

func isDateColumn(col Column) bool {
	var ft, _ = col.AsFlatType()
	return ft == STRING && col.Inference.String_ != nil && col.Inference.String_.Format == "date"
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
		} else if b.datetime[idx] && (b.exactDatetime == nil || !b.exactDatetime(col)) {
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
		if spans := b.spans[idx]; spans != nil {
			conditions[idx].Ranges = b.rangesOf(spans)
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
	for _, spans := range b.spans {
		if spans != nil {
			spans.reset()
		}
	}
}

// spansPerRange is how many spans per output range a builder tracks. Past
// that, the two spans with the narrowest gap between them merge.
const spansPerRange = 8

// minRangeGapDivisor is the divisor that sets the narrowest gap worth
// splitting a bound at. That gap is the bound's span over this denominator,
// so 100 means 1/100 of the span.
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

// datetimePosition is the position of a date-time key: its instant as
// big-endian seconds followed by nanoseconds.
func datetimePosition(ts time.Time) string {
	var out [12]byte
	binary.BigEndian.PutUint64(out[:8], uint64(ts.Unix())^(1<<63))
	binary.BigEndian.PutUint32(out[8:], uint32(ts.Nanosecond()))
	return string(out[:])
}

// keyScale measures the distance between the positions of a column's keys.
type keyScale interface {
	// observe takes account of the key at pos, reporting whether that changes
	// the distance between positions observed before it.
	observe(pos string) bool
	// distance is the log2 distance from position x to the greater position
	// y, and whether float64 resolves it. An unresolved distance is no less
	// than the true one.
	distance(x, y string) (float64, bool)
	// reset forgets the observed keys.
	reset()
}

// instantScale measures the positions of datetimePosition by the time between
// their instants.
type instantScale struct{}

func (instantScale) observe(string) bool { return false }

func (instantScale) reset() {}

func (instantScale) distance(x, y string) (float64, bool) {
	var seconds = binary.BigEndian.Uint64([]byte(y[:8])) - binary.BigEndian.Uint64([]byte(x[:8]))
	var nanos = float64(binary.BigEndian.Uint32([]byte(y[8:]))) - float64(binary.BigEndian.Uint32([]byte(x[8:])))
	return math.Log2(float64(seconds)*float64(time.Second) + nanos), true
}

// stringPosition is the position of a string key, which is the key itself.
func stringPosition(key any) (string, bool) {
	var s, ok = key.(string)
	return s, ok
}

// datePosition is the position of a date key: that of its first instant.
func datePosition(key any) (string, bool) {
	var s, _ = key.(string)
	var day, err = time.Parse(time.DateOnly, s)
	return datetimePosition(day), err == nil
}

// spanSet is the observed keys of a key column as sorted, disjoint spans.
type spanSet struct {
	spans []keySpan
	// limit is the most spans the set holds.
	limit int
	// position is the position of a key, reporting false if it has none.
	position func(key any) (string, bool)
	// scale measures the distance between positions.
	scale keyScale
	// unmeasured marks a set missing an observed key, which had no position.
	unmeasured bool
}

// alphabetPositions is how many leading positions of a string a byteAlphabet
// tracks. Past them, every byte is a digit of its own.
const alphabetPositions = 64

// byteAlphabet is the symbols seen at each position of a set of strings,
// where a symbol is a byte or the end of the string. Reading a string as a
// number in which each position's base is the count of its symbols makes
// distances disregard symbols no string uses there. It is the keyScale of
// strings that are their own positions.
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

// observe adds the symbols of s, reporting whether any are new.
func (a *byteAlphabet) observe(s string) bool {
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

func (a *byteAlphabet) reset() { a.positions = nil }

// digit is the digit of z at position i, and that position's base.
func (a *byteAlphabet) digit(z string, i int) (float64, float64) {
	if i >= len(a.positions) {
		return float64(symbolAt(z, i)), 257
	}
	var pos = &a.positions[i]
	return pos.rank(symbolAt(z, i)), pos.radix()
}

// distance reads x and y as fractions whose digits are those of digit. An
// unresolved distance is the least that resolves.
func (a *byteAlphabet) distance(x, y string) (float64, bool) {
	var p int
	for p < len(x) && p < len(y) && x[p] == y[p] {
		p++
	}
	var scale float64
	for i := range p {
		_, radix := a.digit(x, i)
		scale += math.Log2(radix)
	}
	// Digits whose weight falls below 2^-60 are beneath float64 precision.
	var v, w = 0.0, 1.0
	// least is the smallest sum that later digits can leave of the first
	// differing digit's term while keeping some of float64's precision.
	var least float64
	for i := p; i <= max(len(x), len(y)) && w > 0x1p-60; i++ {
		dx, radix := a.digit(x, i)
		dy, _ := a.digit(y, i)
		w /= radix
		v += (dy - dx) * w
		if i == p {
			least = v * 0x1p-40
		}
	}
	if v <= least {
		return math.Log2(least) - scale, false
	}
	return math.Log2(v) - scale, true
}

// reset empties the set.
func (s *spanSet) reset() {
	s.spans, s.unmeasured = nil, false
	s.scale.reset()
}

// gap is the log2 distance from position x to the greater position y.
func (s *spanSet) gap(x, y string) float64 {
	var d, _ = s.scale.distance(x, y)
	return d
}

// observe adds key at its position.
func (s *spanSet) observe(key any) {
	if pos, ok := s.position(key); ok {
		s.add(pos, key)
	} else {
		s.unmeasured = true
	}
}

// add adds the key at position pos.
func (s *spanSet) add(pos string, key any) {
	if s.scale.observe(pos) {
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
	if len(s.spans) > s.limit {
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
func (b *MergeBoundsBuilder) rangesOf(set *spanSet) []MergeRange {
	if len(set.spans) < 2 || set.unmeasured {
		return nil
	}
	var spans = set.spans
	for len(spans) > b.maxRanges {
		spans = mergeNarrowestGap(spans)
	}
	var whole, resolved = set.scale.distance(spans[0].lo, spans[len(spans)-1].hi)
	if !resolved {
		return nil
	}
	var minGap = whole - math.Log2(minRangeGapDivisor)
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
