package sql

import (
	"strings"
	"time"
)

// MergeBounds are the bounds of each of a binding's key fields.
type MergeBounds []MergeBound

// ProfileFields describes the bounds for the profile of a query they
// restrict: each key field's bound literals and ranges, with the time they
// span for a date-time key, or null for a field that isn't bounded.
func (bounds MergeBounds) ProfileFields() map[string]any {
	var out = make(map[string]any, len(bounds))
	for _, b := range bounds {
		if b.LiteralLower == "" {
			out[b.Field] = nil
			continue
		}

		var bound = map[string]any{"lower": b.LiteralLower, "upper": b.LiteralUpper}
		if span, ok := literalSpan(b.LiteralLower, b.LiteralUpper); ok {
			bound["span"] = span.String()
		}
		if len(b.Ranges) > 0 {
			var ranges = make([][2]string, len(b.Ranges))
			var covered time.Duration
			var timed = true
			for i, r := range b.Ranges {
				ranges[i] = [2]string{r.LiteralLower, r.LiteralUpper}
				span, ok := literalSpan(r.LiteralLower, r.LiteralUpper)
				covered += span
				timed = timed && ok
			}
			bound["ranges"] = ranges
			if timed {
				bound["rangesSpan"] = covered.String()
			}
		}
		out[b.Field] = bound
	}
	return out
}

// literalSpan is the time from one date-time literal to another.
func literalSpan(lower, upper string) (time.Duration, bool) {
	lo, okLower := literalTime(lower)
	hi, okUpper := literalTime(upper)
	return hi.Sub(lo), okLower && okUpper
}

func literalTime(literal string) (time.Time, bool) {
	var value = strings.Trim(literal, "'")
	for _, layout := range []string{time.RFC3339Nano, time.DateOnly} {
		if t, err := time.Parse(layout, value); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
