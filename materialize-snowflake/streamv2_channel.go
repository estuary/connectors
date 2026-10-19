package connector

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

// A channel sends its batch when it holds streamV2BatchRows documents or
// streamV2BatchBytes bytes. These caps are a performance knob, not checkpoint state.
// Recovery resumes from the offset that Snowflake reports as committed,
// whatever batch boundaries the interrupted attempt cut.
var (
	streamV2BatchRows = 10_000
	// Stays under streamV2MaxAppendBytes even for rows that compression cannot shrink.
	streamV2BatchBytes = int(3.5 * 1024 * 1024)
)

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
	// bufBytes is the total of bytes that the batches of every channel sharing it
	// buffer.
	bufBytes *int
	// sendAllBatches sends the buffered batch of every channel that shares bufBytes.
	sendAllBatches func(context.Context) error
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
func newStreamV2Channel(b *streamV2Binding, client *streamV2Client, bufBytes *int, sendAllBatches func(context.Context) error, channelName string, keyRange streamV2Range, routed, committed int64) *streamV2Channel {
	var c = &streamV2Channel{
		binding:        b,
		client:         client,
		bufBytes:       bufBytes,
		sendAllBatches: sendAllBatches,
		channelName:    channelName,
		keyRange:       keyRange,
		limiter:        rate.NewLimiter(streamV2PaceBytesPerSecond, 2*streamV2PaceBytesPerSecond),
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

// sendBatch closes the payload of a non-empty batch and hands it to the append pipe
// of the channel, and takes the payload's bytes off bufBytes.
// Ownership of the buffer passes to the append, so the next batch starts a buffer of
// its own.
func (c *streamV2Channel) sendBatch(ctx context.Context) error {
	if c.batch.empty() {
		return nil
	}
	var startToken, endToken = c.offsetToken(c.batch.firstOffset), c.offsetToken(c.progress.routed)
	var payload = c.batch.finish()
	*c.bufBytes -= len(payload)

	if c.appends == nil {
		c.appends, c.appendsCtx = errgroup.WithContext(ctx)
		c.appends.SetLimit(1)
	} else if c.appendsCtx.Err() != nil {
		return fmt.Errorf("appending to channel %q of %s: %w", c.channelName, c.binding.table, c.appends.Wait())
	}

	var appendsCtx = c.appendsCtx
	c.appends.Go(func() error {
		if err := streamV2Pace(appendsCtx, c.limiter, len(payload)); err != nil {
			return fmt.Errorf("pacing channel %q: %w", c.channelName, err)
		}
		return c.client.AppendRows(appendsCtx, c.channelName, startToken, endToken, payload)
	})
	return nil
}

// streamV2Pace waits until limiter allows n bytes. WaitN refuses an n larger than
// the limiter's burst, so a larger n is waited for one burst at a time.
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
// the channel's batch once the batch reaches its row or byte cap, and every channel's
// batch once bufBytes reaches streamV2MaxBufferedBytes.
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

	var row, err = c.binding.encoder.encodeRow(c.batch.scratch[:0], converted)
	if err != nil {
		return fmt.Errorf("encoding row for %s: %w", c.binding.table, err)
	}
	c.batch.scratch = row
	// A row that would carry the batch over its byte cap starts the next one, so
	// a batch is only ever over the cap when it holds a single row.
	if c.batch.size()+len(row) > streamV2BatchBytes {
		if err := c.sendBatch(ctx); err != nil {
			return err
		}
	}
	if c.batch.rows == 0 {
		c.batch.start()
		c.batch.firstOffset = offset
	}
	c.batch.payload = append(c.batch.payload, row...)
	c.batch.rows++
	*c.bufBytes += len(row)

	if c.batch.rows >= streamV2BatchRows || c.batch.size() >= streamV2BatchBytes {
		return c.sendBatch(ctx)
	} else if *c.bufBytes >= streamV2MaxBufferedBytes {
		return c.sendAllBatches(ctx)
	}
	return nil
}
