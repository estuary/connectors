package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileBounds(t *testing.T) {
	var bound = func(field, lower, upper string, ranges ...MergeRange) MergeBound {
		var b = MergeBound{LiteralLower: lower, LiteralUpper: upper, Ranges: ranges}
		b.Field = field
		return b
	}

	require.Equal(t, map[string]any{
		"created_at": map[string]any{
			"lower":      "'2025-12-16T12:00:00Z'",
			"upper":      "'2026-10-27T13:30:00.5Z'",
			"span":       "7561h30m0.5s",
			"rangesSpan": "15m0.5s",
			"ranges": [][2]string{
				{"'2025-12-16T12:00:00Z'", "'2025-12-16T12:00:00Z'"},
				{"'2026-10-27T13:15:00Z'", "'2026-10-27T13:30:00.5Z'"},
			},
		},
		"day": map[string]any{"lower": "'2026-02-28'", "upper": "'2026-03-02'", "span": "48h0m0s"},
		"id":  map[string]any{"lower": "'018f'", "upper": "'01a1'"},
		"ok":  nil,
	}, MergeBounds{
		bound("created_at", "'2025-12-16T12:00:00Z'", "'2026-10-27T13:30:00.5Z'",
			MergeRange{LiteralLower: "'2025-12-16T12:00:00Z'", LiteralUpper: "'2025-12-16T12:00:00Z'"},
			MergeRange{LiteralLower: "'2026-10-27T13:15:00Z'", LiteralUpper: "'2026-10-27T13:30:00.5Z'"},
		),
		bound("day", "'2026-02-28'", "'2026-03-02'"),
		bound("id", "'018f'", "'01a1'"),
		bound("ok", "", ""),
	}.ProfileFields())
}
