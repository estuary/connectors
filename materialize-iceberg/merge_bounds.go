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
}

type mergeBoundsBuilder struct {
	keys      []boilerplate.MappedProjection[mapped]
	literaler func(any) string

	lower []any
	upper []any
}

func newMergeBoundsBuilder(keys []boilerplate.MappedProjection[mapped]) *mergeBoundsBuilder {
	return &mergeBoundsBuilder{
		keys: keys,
		literaler: sql.ToLiteralFn(func(in string) string {
			return "'" + in + "'"
		}),
	}
}

func (b *mergeBoundsBuilder) nextKey(key []any) {
	if b.lower == nil {
		b.lower = append([]any(nil), key...)
		b.upper = append([]any(nil), key...)
	}

	for idx, k := range key {
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
	conditions := make([]mergeBound, len(b.lower))

	for idx, p := range b.keys {
		conditions[idx] = mergeBound{MappedProjection: p}

		switch p.Mapped.type_.(type) {
		case iceberg.BooleanType, iceberg.BinaryType:
			continue
		}

		conditions[idx].LiteralLower = b.literaler(b.lower[idx])
		conditions[idx].LiteralUpper = b.literaler(b.upper[idx])
	}

	// Reset for tracking the next transaction.
	b.lower = nil
	b.upper = nil

	return conditions
}
