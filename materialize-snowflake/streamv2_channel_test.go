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

// TestStreamV2SendsReleaseEveryAcquiredByte covers the accounting of heldBytes: a
// send releases exactly the bytes its batch's rows acquired. Releasing more panics,
// and releasing less leaves the ceiling short once the transaction is flushed.
func TestStreamV2SendsReleaseEveryAcquiredByte(t *testing.T) {
	// A leak exhausts the ceiling, and a write would then wait on it forever.
	var ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var restore = streamV2MaxHeldBytes
	t.Cleanup(func() { streamV2MaxHeldBytes = restore })
	streamV2MaxHeldBytes = 200

	var m = newFakeStreamV2Manager(t, ctx, &config{Credentials: &snowflake_auth.CredentialConfig{}}, "test/release",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	m.addBinding("DB", "SCH", "TBL", sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "TBL",
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `VAL`}},
		StateKey:   "release.v1",
	}, nil)

	var requireCeilingFree = func() {
		t.Helper()
		require.True(t, m.heldBytes.TryAcquire(int64(streamV2MaxHeldBytes)), "a flushed transaction left bytes acquired")
		m.heldBytes.Release(int64(streamV2MaxHeldBytes))
	}

	for txn := range 2 {
		for i := range 40 {
			// Every tenth row is larger than the whole ceiling, so it acquires a
			// clamped share, and the rest cross the ceiling every few rows.
			var val = strings.Repeat("v", 20)
			if i%10 == 0 {
				val = strings.Repeat("v", 500)
			}
			require.NoError(t, testWriteRow(ctx, m, 0, []any{fmt.Sprintf("t%d-k%d", txn, i), val}))
		}
		var _, err = m.flush(ctx)
		require.NoError(t, err)
		requireCeilingFree()
	}
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

// TestStreamV2RejectsAnOversizedAppend covers a document too large for one append
// even after compression. The failure names the table, the document's compressed
// size and Snowflake's limit, and says that a restart cannot get past it.
func TestStreamV2RejectsAnOversizedAppend(t *testing.T) {
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
	require.NoError(t, testWriteRow(ctx, m, 0, []any{"k", string(val)}))
	_, err := m.flush(ctx)
	require.ErrorContains(t, err, "TBL")
	require.Regexp(t, `an append of 1 document\(s\) totalling \d+ bytes compresses to \d+ bytes`, err.Error())
	require.ErrorContains(t, err, "Snowflake's limit of 4194304 bytes (4 MiB) per compressed Snowpipe Streaming request")
	require.ErrorContains(t, err, "the task will fail the same way on every restart")
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
// same transaction has failed: it returns that append's error at once, and every
// byte held for the batches is released.
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
	require.True(t, m.heldBytes.TryAcquire(int64(streamV2MaxHeldBytes)), "held bytes were not released")
}
