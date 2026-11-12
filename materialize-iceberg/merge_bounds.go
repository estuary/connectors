// TODO(whb): This file is mostly copied from the equivalent file in
// materialize-sql. They should be consolidated once I figure out how to make
// this feature more general.

package connector

import (
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
)

type mergeBound struct {
	boilerplate.MappedProjection[mapped]
	LiteralLower string
	LiteralUpper string
}

type mergeBoundsBuilder struct {
	keys      []boilerplate.MappedProjection[mapped]
	literaler func(any) string

	lower []any
	upper []any
	// unbounded marks key columns whose observed values can't be ordered,
	// such as a null or a date-time that doesn't parse.
	unbounded []bool
}

func newMergeBoundsBuilder(keys []boilerplate.MappedProjection[mapped]) *mergeBoundsBuilder {
	return &mergeBoundsBuilder{
		keys:      keys,
		literaler: sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "\\'")),
	}
}

func (b *mergeBoundsBuilder) nextKey(key []any) {
	if b.lower == nil {
		b.lower = append([]any(nil), key...)
		b.upper = append([]any(nil), key...)
		b.unbounded = make([]bool, len(key))
	}

	for idx, k := range key {
		if _, ok := b.keys[idx].Mapped.type_.(iceberg.TimestampTzType); ok {
			// Date-time strings order by their instant, which text order
			// doesn't follow across offsets and precisions.
			if lower, upper, _, ok := sql.MinMaxDatetime(b.lower[idx], b.upper[idx], k); ok {
				b.lower[idx], b.upper[idx] = lower, upper
			} else {
				b.unbounded[idx] = true
			}
			continue
		}
		if k == nil {
			b.unbounded[idx] = true
		}
		if b.unbounded[idx] {
			continue
		}
		b.lower[idx] = minKey(b.lower[idx], k)
		b.upper[idx] = maxKey(b.upper[idx], k)
	}
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

// timestamptzLiteral renders a date-time bound in UTC at microsecond
// precision. Spark casts this spelling, unlike some that parse as RFC3339, and
// truncates stored values to microseconds the same way.
func timestamptzLiteral(t time.Time) string {
	return t.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z07:00")
}

// quoteLiteral renders a transformed value as a single-quoted SQL string
// literal, matching every other literal this package emits.
var quoteLiteral = sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "\\'"))

func (b *mergeBoundsBuilder) build() []mergeBound {
	conditions := make([]mergeBound, len(b.lower))

	for idx, p := range b.keys {
		conditions[idx] = mergeBound{MappedProjection: p}

		switch p.Mapped.type_.(type) {
		case iceberg.BooleanType, iceberg.BinaryType:
			continue
		case iceberg.TimestampTzType:
			if b.unbounded[idx] {
				continue
			}
			lower, _ := sql.ParseDatetime(b.lower[idx])
			upper, _ := sql.ParseDatetime(b.upper[idx])
			conditions[idx].LiteralLower = b.literaler(timestamptzLiteral(lower))
			conditions[idx].LiteralUpper = b.literaler(timestamptzLiteral(upper))
			continue
		}

		conditions[idx].LiteralLower = b.literaler(b.lower[idx])
		conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
	}

	// Reset for tracking the next transaction.
	b.lower = nil
	b.upper = nil
	b.unbounded = nil

	return conditions
}
