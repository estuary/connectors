package connector

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// A channel sends its batch when its rows hold streamV2BatchSizeLimit bytes before
// compression. Snowflake refuses a file fragment of more than 512 MiB uncompressed,
// a limit found by experiment since Snowflake does not document it. A batch is cut
// after the row that reaches the limit, so the limit undershoots 512 MiB by half to
// leave room for that row.
//
// The limit is not checkpoint state. Recovery resumes from the offset that Snowflake
// reports as committed, whatever batch boundaries the interrupted attempt cut.
var streamV2BatchSizeLimit = 256 * 1024 * 1024

// streamV2PaceBytesPerSecond is the uncompressed byte rate this write path allows
// itself on one channel.
//
// The value is the documented per-channel ceiling.
const streamV2PaceBytesPerSecond = 20 * 1000 * 1000

// streamV2Channel is a handle for one Snowpipe channel of one connector binding.
type streamV2Channel struct {
	// binding is the binding whose table the channel writes to.
	binding *streamV2Binding
	client  *streamV2Client
	// channelName is the name of the Snowpipe channel.
	channelName string
	// keyRange is the key range this channel covers. The key range is embedded in
	// the channel's name and in every offset token it appends with.
	keyRange streamV2Range

	// progress tracks the channel's offsets. A document's offset is its 1-based
	// position among the documents the shard has routed to the channel, over the
	// whole life of the channel, so the offset of a channel is that of its last
	// document, and zero means none. Offsets never reset.
	//
	// committed exceeds routed only while a replayed transaction routes documents
	// that an interrupted attempt already committed.
	progress struct {
		// routed leads progress. It is the offset of the most recent document that
		// the shard routed to this channel.
		routed int64
		// checkpointed is the offset that the last checkpoint item carried.
		checkpointed int64
		// committed is the highest offset that Snowflake durably holds.
		committed int64
	}

	// batch is the append under construction.
	batch streamV2Batch

	// appends runs the channel's appends in the background, one at a time and in
	// order. It is nil between transactions, and appendsCtx is cancelled once an
	// append has failed.
	appends    *errgroup.Group
	appendsCtx context.Context

	// limiter meters the uncompressed bytes this channel hands to Snowflake. One limiter
	// per channel, because Snowflake meters each channel independently.
	limiter *rate.Limiter
}

// newStreamV2Channel builds the in-memory state of one opened, reconciled channel.
func newStreamV2Channel(b *streamV2Binding, client *streamV2Client, channelName string, keyRange streamV2Range, routed, committed int64) *streamV2Channel {
	var c = &streamV2Channel{
		binding:     b,
		client:      client,
		channelName: channelName,
		keyRange:    keyRange,
		batch:       streamV2Batch{columns: b.columns},
		limiter:     rate.NewLimiter(streamV2PaceBytesPerSecond, 2*streamV2PaceBytesPerSecond),
		progress: struct {
			routed       int64
			checkpointed int64
			committed    int64
		}{
			routed:       routed,
			checkpointed: routed,
			committed:    committed,
		},
	}
	return c
}

// offsetToken renders the offset token for an offset on this channel.
func (c *streamV2Channel) offsetToken(offset int64) string {
	return streamV2FormatOffsetToken(offset, c.keyRange)
}

// wait waits for the channel's in-flight sendBatch and returns the first error
// of the appends since the last wait.
func (c *streamV2Channel) wait() error {
	if c.appends == nil {
		return nil
	}
	var err = c.appends.Wait()
	c.appends, c.appendsCtx = nil, nil
	return err
}

// sendBatch finishes a non-empty batch and hands its file to the channel's appends,
// which remove the file once its append returns. Once an append of the channel has
// failed, sendBatch returns that append's error.
func (c *streamV2Channel) sendBatch(ctx context.Context) error {
	if c.batch.empty() {
		return nil
	}
	// The batch's rows hold consecutive offsets starting at firstOffset. The
	// channel's routed count can be one ahead of the batch's last row, because a
	// row is counted as routed before it is added to a batch.
	var startToken, endToken = c.offsetToken(c.batch.firstOffset), c.offsetToken(c.batch.firstOffset + int64(c.batch.rows) - 1)
	file, err := c.batch.finish()
	if err != nil {
		return fmt.Errorf("finishing batch of channel %q: %w", c.channelName, err)
	}

	if c.appends == nil {
		c.appends, c.appendsCtx = errgroup.WithContext(ctx)
		c.appends.SetLimit(1)
	} else if c.appendsCtx.Err() != nil {
		os.Remove(file.path)
		return fmt.Errorf("appending to channel %q of %s: %w", c.channelName, c.binding.table, c.appends.Wait())
	}

	var appendsCtx = c.appendsCtx
	c.appends.Go(func() error {
		defer os.Remove(file.path)
		if err := streamV2Pace(appendsCtx, c.limiter, file.uncompressedBytes); err != nil {
			return fmt.Errorf("pacing channel %q: %w", c.channelName, err)
		}
		return c.client.AppendRows(appendsCtx, c.channelName, startToken, endToken, file)
	})
	return nil
}

// streamV2Pace blocks until limiter lets n bytes through. The limiter refills at a
// steady rate and holds at most its burst size, and a single WaitN call fails if it
// asks for more than the burst. So a payload larger than the burst is waited for in
// pieces no larger than the burst, one after another.
func streamV2Pace(ctx context.Context, limiter *rate.Limiter, n int) error {
	for n > 0 {
		var chunk = min(n, limiter.Burst())
		if err := limiter.WaitN(ctx, chunk); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

// writeRow buffers one converted document under the channel's next offset. It sends
// the channel's batch once the batch reaches its size limit.
//
// A document at or below the channel's committed offset was already committed by an
// interrupted attempt of this transaction. It still consumes its offset, so that later
// offsets stay aligned with Snowflake's, but it is not written again.
func (c *streamV2Channel) writeRow(ctx context.Context, converted []any) error {
	c.progress.routed++
	var offset = c.progress.routed
	if offset <= c.progress.committed {
		return nil
	}

	if c.batch.empty() {
		c.batch.firstOffset = offset
	}
	if err := c.batch.writeRow(converted); err != nil {
		return fmt.Errorf("table %s: %w", c.binding.table, err)
	}

	if c.batch.size() >= streamV2BatchSizeLimit {
		return c.sendBatch(ctx)
	}
	return nil
}
