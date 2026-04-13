// TODO(whb): This file is mostly copied from the equivalent file in
// materialize-sql. They should be consolidated once I figure out how to make
// this feature more general.

package connector

import (
	"fmt"

	"github.com/apache/iceberg-go"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
)

type mergeBound struct {
	boilerplate.MappedProjection[mapped]
	LiteralLower string
	LiteralUpper string
	// IsNull indicates that at least one observed key value for this column
	// was nil during the transaction; the merge predicate must include the
	// NULL key rows alongside any range bounds.
	IsNull bool
}

type mergeBoundsBuilder struct {
	keys      []boilerplate.MappedProjection[mapped]
	literaler func(any) string

	lower  []any
	upper  []any
	isNull []bool
}

func newMergeBoundsBuilder(keys []boilerplate.MappedProjection[mapped]) *mergeBoundsBuilder {
	return &mergeBoundsBuilder{
		keys:      keys,
		literaler: sql.ToLiteralFn(sql.QuoteTransformEscapedBackslash("'", "\\'")),
	}
}

func (b *mergeBoundsBuilder) nextKey(key []any) {
	if b.lower == nil {
		b.lower = make([]any, len(b.keys))
		b.upper = make([]any, len(b.keys))
		b.isNull = make([]bool, len(b.keys))
	}

	for idx, k := range key {
		if k == nil {
			b.isNull[idx] = true
			continue
		}
		if b.lower[idx] == nil {
			b.lower[idx] = k
			b.upper[idx] = k
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

func (b *mergeBoundsBuilder) build() []mergeBound {
	conditions := make([]mergeBound, len(b.keys))

	for idx, p := range b.keys {
		conditions[idx] = mergeBound{MappedProjection: p}

		switch p.Mapped.type_.(type) {
		case iceberg.BooleanType, iceberg.BinaryType:
			// Type-based skip mirrors the upstream materialize-sql behavior:
			// these key types are intentionally never bounded, even when null.
			continue
		}

		conditions[idx].IsNull = b.isNull[idx]
		if b.lower[idx] != nil {
			conditions[idx].LiteralLower = b.literaler(b.lower[idx])
			conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
		}
	}

	// Reset for tracking the next transaction.
	b.lower = nil
	b.upper = nil
	b.isNull = nil

	return conditions
}
