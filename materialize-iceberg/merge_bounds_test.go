package connector

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	realBoilerplate "github.com/estuary/connectors/materialize-boilerplate"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

func TestMergeBoundsDatetimeKeys(t *testing.T) {
	keys := []realBoilerplate.MappedProjection[mapped]{
		{Projection: realBoilerplate.Projection{Projection: pf.Projection{Field: "ts"}}, Mapped: mapped{type_: iceberg.TimestampTzType{}, Name: "ts"}},
		{Projection: realBoilerplate.Projection{Projection: pf.Projection{Field: "id"}}, Mapped: mapped{type_: iceberg.StringType{}, Name: "id"}},
	}

	for _, tt := range []struct {
		name         string
		keys         []any
		lower, upper string
	}{
		{"mixed fractional precision", []any{"2024-01-01T00:00:00.123Z", "2024-01-01T00:00:00.1Z"},
			"'2024-01-01T00:00:00.100000Z'", "'2024-01-01T00:00:00.123000Z'"},
		{"bare Z and fractional seconds", []any{"2024-01-01T00:00:00.5Z", "2024-01-01T00:00:00Z"},
			"'2024-01-01T00:00:00.000000Z'", "'2024-01-01T00:00:00.500000Z'"},
		{"differing offsets", []any{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00+02:00"},
			"'2023-12-31T23:00:00.000000Z'", "'2024-01-01T00:00:00.000000Z'"},
		{"sub-microsecond digits truncate", []any{"2024-01-01T00:00:00.9999999Z", "1969-12-31T23:59:59.9999995Z"},
			"'1969-12-31T23:59:59.999999Z'", "'2024-01-01T00:00:00.999999Z'"},
		{"lowercase z", []any{"2024-01-01T00:00:00z"},
			"'2024-01-01T00:00:00.000000Z'", "'2024-01-01T00:00:00.000000Z'"},
		{"years outside 0000-9999 in UTC", []any{"9999-12-31T23:59:59-14:00", "0000-01-01T00:00:00+01:00"},
			"'-0001-12-31T23:00:00.000000Z'", "'10000-01-01T13:59:59.000000Z'"},
		{"an unparseable value drops the bound", []any{"2024-01-01T00:00:00Z", "2016-12-31T23:59:60Z", "2024-01-02T00:00:00Z"}, "", ""},
		{"a null value drops the bound", []any{"2024-01-01T00:00:00Z", nil}, "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := newMergeBoundsBuilder(keys)
			for i, k := range tt.keys {
				b.nextKey([]any{k, string(rune('a' + i))})
			}
			got := b.build()
			require.Equal(t, tt.lower, got[0].LiteralLower)
			require.Equal(t, tt.upper, got[0].LiteralUpper)
			require.Equal(t, "'a'", got[1].LiteralLower, "other keys are unaffected")

			// The next transaction is bounded afresh.
			b.nextKey([]any{"2024-01-01T00:00:00Z", "a"})
			got = b.build()
			require.Equal(t, "'2024-01-01T00:00:00.000000Z'", got[0].LiteralLower)
			require.Equal(t, "'2024-01-01T00:00:00.000000Z'", got[0].LiteralUpper)
		})
	}
}

// TestDatetimeKeyBounds runs the connector's merge and load queries for a
// date-time key on both Spark sets of the compose stack. Each instant is
// stored under one spelling, then loaded and updated under another, and
// neither set of spellings sorts as text in time order.
func TestDatetimeKeyBounds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	for name, configPath := range localConfigTemplates {
		name := strings.TrimSuffix(strings.TrimPrefix(path.Base(name), "config."), ".yaml")
		t.Run(name, func(t *testing.T) {
			raw, err := boilerplate.ReadConfigFile(configPath)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))
			runDatetimeKeyBounds(t, cfg)
		})
	}
}

func runDatetimeKeyBounds(t *testing.T, cfg config) {
	h := newMORHarness(t, cfg)
	mb, tablePath := h.createTable(fmt.Sprintf("dtkey_flow_test_%d", time.Now().UnixNano()), "date-time")

	spellings := [][2]string{
		{"2024-01-01T00:00:00Z", "2024-01-01T01:00:00+01:00"},
		{"2024-01-01T00:00:00.1Z", "2024-01-01T00:00:00.100000Z"},
		{"2024-01-01T00:00:00.123Z", "2024-01-01T00:00:00.123000000Z"},
		{"2024-01-01T00:00:00.5Z", "2023-12-31T23:30:00.5-00:30"},
		{"2024-01-01T01:00:00+02:00", "2023-12-31T23:00:00Z"},
		{"2024-01-01T00:00:00.1234567Z", "2024-01-01T00:00:00.123456Z"},
	}

	// rows are the documents of every instant under spelling s, along with
	// their ids and the documents a load should return.
	rows := func(s int, n int64) ([]morRow, []string, map[string]int64) {
		var out []morRow
		var ids []string
		want := map[string]int64{}
		for i, sp := range spellings {
			out = append(out, morRow{id: sp[s], n: n + int64(i)})
			ids = append(ids, sp[s])
			want[sp[s]] = n + int64(i)
		}
		slices.SortFunc(out, func(a, b morRow) int { return strings.Compare(a.id, b.id) })
		return out, ids, want
	}

	stored, storedIDs, want := rows(0, 0)
	h.merge(mb, stored)
	_, respelledIDs, _ := rows(1, 0)
	require.Equal(t, want, h.load(mb, respelledIDs), "load under other spellings")

	// Updates under the other spellings replace each row rather than adding
	// one.
	updated, _, want := rows(1, 100)
	h.merge(mb, updated)
	require.Equal(t, want, h.load(mb, storedIDs), "load after update")
	require.Len(t, h.query(fmt.Sprintf("SELECT `id` FROM %s", morFQN(tablePath))), len(spellings))
}
