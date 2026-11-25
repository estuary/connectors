package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/estuary/connectors/go/common"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/stretchr/testify/require"
)

// TestKeylessScanQueryResumeAcrossChangedRanges checks where a keyless backfill
// resumes when the ROWID ranges differ from the ones its checkpoint was made with,
// as happens when keylessBackfillChunkMiB changes mid-backfill.
func TestKeylessScanQueryResumeAcrossChangedRanges(t *testing.T) {
	var db = &oracleDatabase{}
	var info = &sqlcapture.DiscoveryInfo{}
	var ranges = []string{
		"AAAR3sAABAAAZHZAAA",
		"AAAR3sAABAAAZIAAAA",
		"AAAR3sAABAAAZMAAAA",
		"AAAR3sAABAAAZQAAAA",
	}

	for _, tc := range []struct {
		name       string
		after      string
		wantStart  string
		wantEndIdx int
	}{
		{"exact boundary", ranges[1], ranges[1], 2},
		{"old boundary inside a new range", "AAAR3sAABAAAZKAAAA", ranges[1], 2},
		{"old boundary inside the first range", "AAAR3sAABAAAZHaAAA", ranges[0], 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var query, end = db.keylessScanQuery(info, "S", "T", tc.after, ranges, "")
			require.Contains(t, query, fmt.Sprintf("ROWID >= '%s'", tc.wantStart))
			require.Equal(t, ranges[tc.wantEndIdx], end)
		})
	}
}

// TestKeylessBackfillChunkSize checks that keylessBackfillChunkMiB sets how many
// ROWID ranges a keyless table is split into, and that a backfill over many small
// ranges still captures every row.
func TestKeylessBackfillChunkSize(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "73120944"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(id INTEGER, data VARCHAR(2000))")

	// About 4 MiB of rows, so a 1 MiB chunk size gives several ranges.
	const rowCount = 2000
	tb.Query(ctx, t, true, fmt.Sprintf(
		`INSERT INTO %s SELECT level, RPAD('x', 1900, 'x') FROM dual CONNECT BY level <= %d`,
		tableName, rowCount))

	var schema, table = strings.ToUpper(tb.config.User), "T" + uniqueID
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var countRanges = func(t *testing.T, chunkMiB int) int {
		setKeylessChunkMiB(t, chunkMiB)
		var cfgJSON, err = json.Marshal(tb.config)
		require.NoError(t, err)
		db, err := connectOracle(ctx, "test", cfgJSON, common.CreatedAt{})
		require.NoError(t, err)
		defer db.Close(ctx)
		var odb = db.(*oracleDatabase)
		require.NoError(t, odb.fetchBackfillRowIDRange(ctx, schema, table))
		return len(odb.backfillRowIDRanges[streamID]) - 1
	}

	t.Run("ranges", func(t *testing.T) {
		require.Equal(t, 1, countRanges(t, 64))
		require.Greater(t, countRanges(t, 1), 2)
	})

	t.Run("capture", func(t *testing.T) {
		var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))
		setKeylessChunkMiB(t, 1)
		var validator = &keylessIDValidator{ids: make(map[string]bool)}
		cs.Validator = validator
		tests.RunCapture(ctx, t, cs)
		require.Empty(t, cs.Errors)
		// Distinct IDs, not documents: the row at each range boundary is read by
		// both neighbouring chunks.
		require.Equal(t, rowCount, len(validator.ids))
	})
}

func setKeylessChunkMiB(t *testing.T, mib int) {
	var prev = keylessBackfillChunkMiB
	keylessBackfillChunkMiB = mib
	t.Cleanup(func() { keylessBackfillChunkMiB = prev })
}

// keylessIDValidator records the distinct ID values of captured documents.
type keylessIDValidator struct{ ids map[string]bool }

func (v *keylessIDValidator) Checkpoint(json.RawMessage)            {}
func (v *keylessIDValidator) SourcedSchema(string, json.RawMessage) {}
func (v *keylessIDValidator) Summarize(io.Writer) error             { return nil }
func (v *keylessIDValidator) Reset()                                {}
func (v *keylessIDValidator) Output(_ string, data json.RawMessage) {
	// NUMBER columns are captured as strings.
	var doc struct {
		ID string `json:"ID"`
	}
	if err := json.Unmarshal(data, &doc); err == nil && doc.ID != "" {
		v.ids[doc.ID] = true
	}
}
