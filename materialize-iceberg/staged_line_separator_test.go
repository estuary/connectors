package connector

import (
	"fmt"
	"testing"
	"time"

	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

// TestStagedFileLineSeparator merges a staged file whose first line break is a
// CRLF inside a quoted value, followed by rows that end in an unquoted value.
// Every row must still be read as its own record, and the CRLF must reach the
// table unchanged.
func TestStagedFileLineSeparator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ensureDockerUp(t)

	for template, out := range localConfigTemplates {
		t.Run(template, func(t *testing.T) {
			raw, err := testutil.ReadConfigFile(out)
			require.NoError(t, err)
			var cfg config
			require.NoError(t, json.Unmarshal(raw, &cfg))

			h := newMORHarness(t, cfg)
			mb, tablePath := h.createTable(fmt.Sprintf("line_sep_flow_test_%d", time.Now().UnixNano()), "")

			h.merge(mb, []morRow{{id: "c", n: 1}})

			crlf := "a\r\nb"
			h.merge(mb, []morRow{
				{id: crlf, n: 2},
				{id: "c", delete: true},
				{id: "d", n: 3},
				{id: "e", n: 4},
			})

			got := map[string]int64{}
			for _, r := range h.query(fmt.Sprintf("SELECT `id`, `n` FROM %s", morFQN(tablePath))) {
				var id string
				var n int64
				require.NoError(t, json.Unmarshal(r[0].(json.RawMessage), &id))
				require.NoError(t, json.Unmarshal(r[1].(json.RawMessage), &n))
				got[id] = n
			}
			require.Equal(t, map[string]int64{crlf: 2, "d": 3, "e": 4}, got)
		})
	}
}
