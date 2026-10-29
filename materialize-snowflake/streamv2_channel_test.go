package connector

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// TestStreamV2BatchStaysUnderTheCap covers the API's limit on an append body: a row
// that would carry a batch over the byte cap starts the next batch instead, so no
// append of more than one row exceeds the cap.
func TestStreamV2BatchStaysUnderTheCap(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)
	defer func(n int) { streamV2BatchBytes = n }(streamV2BatchBytes)
	streamV2BatchBytes = 1000

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/cap",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "cap.v1",
	}, nil)

	fakeSnowpipeLargestAppend.Store(0)
	for i := range 20 {
		require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("k%d", i), strings.Repeat("v", 300)}))
	}
	var _, err = m.flush(ctx)
	require.NoError(t, err)
	require.LessOrEqual(t, fakeSnowpipeLargestAppend.Load(), int64(streamV2BatchBytes))
}

// TestStreamV2NoAppendAfterAFailedAppend covers the order of a channel's appends:
// once an append fails, the append queued behind it must not run. It would present
// the continuation token the failed append left unchanged, and Snowflake would
// commit an offset beyond rows it never stored.
func TestStreamV2NoAppendAfterAFailedAppend(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/failed",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "failed.v1",
	}, nil)

	require.NoError(t, testWriteRow(ctx, m, 0, []any{"a", 1}))
	var c = m.bindings[0].activeChannels[0]
	// An empty limiter holds the first append back, so the second is queued
	// behind it while it is still pending.
	c.limiter = rate.NewLimiter(100, 100)
	c.limiter.AllowN(time.Now(), 100)
	// The fake rejects a row that is not a JSON object.
	for i, b := range c.batch.payload {
		if b != '\n' {
			c.batch.payload[i] = 'x'
		}
	}
	require.NoError(t, c.sendBatch(ctx))
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"b", 2}))
	require.NoError(t, c.sendBatch(ctx))
	require.ErrorContains(t, c.wait(), "row is not a JSON object")

	var status, err = m.client.ChannelStatus(ctx, c.channelName)
	require.NoError(t, err)
	require.Nil(t, status.CommittedToken, "an append queued behind a failed append was committed")
}

// TestStreamV2OversizedAppendUsesFileMode covers a document too large for one
// append request even after compression: it is uploaded to the pipe's stage and
// registered with the channel instead, and commits like any other append.
func TestStreamV2OversizedAppendUsesFileMode(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/oversized",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "oversized.v1",
	}, nil)

	// Random letters compress to about three quarters of their size, so 6 MiB of
	// them stays over the 4 MiB limit.
	var val = make([]byte, 6*1024*1024)
	for i := range val {
		val[i] = 'a' + byte(rand.IntN(26))
	}
	fakeSnowpipeFileAppends.Store(0)
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"small", "v"}))
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"big", string(val)}))
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"after", "v"}))
	var entries, err = m.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), fakeSnowpipeFileAppends.Load(), "only the oversized row goes through file mode")
	require.Equal(t, int64(3), soleCheckpointItem(t, entries, 0).Routed)

	status, err := m.client.ChannelStatus(ctx, m.bindings[0].activeChannels[0].channelName)
	require.NoError(t, err)
	require.NotNil(t, status.CommittedToken)
	require.Equal(t, streamV2FormatOffsetToken(3, fullKeyRange), *status.CommittedToken)
}

// TestStreamV2AppendLargerThanTheBurst covers a row larger than the channel
// limiter's burst: it is paced through the limiter and committed.
func TestStreamV2AppendLargerThanTheBurst(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/burst",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "burst.v1",
	}, nil)

	require.NoError(t, testWriteRow(ctx, m, 0, []any{"small", "v"}))
	var c = m.bindings[0].activeChannels[0]
	c.limiter = rate.NewLimiter(1<<30, 1<<20)
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"big", strings.Repeat("a", 3<<20)}))
	var _, err = m.flush(ctx)
	require.NoError(t, err)

	status, err := m.client.ChannelStatus(ctx, c.channelName)
	require.NoError(t, err)
	require.NotNil(t, status.CommittedToken)
	require.Equal(t, streamV2FormatOffsetToken(2, fullKeyRange), *status.CommittedToken)
}

// TestStreamV2SendFailsOnceAnAppendHasFailed covers a send after an append of the
// same transaction has failed: it returns that append's error at once.
func TestStreamV2SendFailsOnceAnAppendHasFailed(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/sendfails",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "sendfails.v1",
	}, nil)

	require.NoError(t, testWriteRow(ctx, m, 0, []any{"a", 1}))
	var c = m.bindings[0].activeChannels[0]
	// The fake rejects a row that is not a JSON object.
	for i, b := range c.batch.payload {
		if b != '\n' {
			c.batch.payload[i] = 'x'
		}
	}
	require.NoError(t, c.sendBatch(ctx))
	<-c.appendsCtx.Done()

	require.NoError(t, testWriteRow(ctx, m, 0, []any{"b", 2}))
	require.ErrorContains(t, c.sendBatch(ctx), "row is not a JSON object")
}
