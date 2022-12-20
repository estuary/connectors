package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata"
	marketdataStream "github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	emptyCheckpoint = `{}`
	defaultCurrency = "usd"
)

var streamerLoggingInterval = 1 * time.Minute

type tickDocument struct {
	ID         int64     `json:"ID" jsonschema:"title=ID,description=Trade ID"`
	Symbol     string    `json:"Symbol" jsonschema:"title=Symbol,description=Symbol"`
	Exchange   string    `json:"Exchange" jsonschema:"title=Exchange,description=Exchange where the trade happened"`
	Price      float64   `json:"Price" jsonschema:"title=Price,description=Trade price"`
	Size       uint32    `json:"Size" jsonschema:"title=Size,description=Trade size"`
	Timestamp  time.Time `json:"Timestamp" jsonschema:"title=Timestamp,description=Timestamp in RFC-3339 format with nanosecond precision"`
	Conditions []string  `json:"Conditions" jsonschema:"title=Conditions,description=Trade conditions"`
	Tape       string    `json:"Tape" jsonschema:"title=Tape,description=Tape"`
}

type alpacaClient struct {
	mu           sync.Mutex
	output       *boilerplate.PullOutput
	bindingIdx   int
	dataClient   marketdata.Client
	streamClient marketdataStream.StocksClient
	resourceName string
	symbols      []string
	feed         string
	freePlan     bool
}

func (c *alpacaClient) handleDocuments(docs <-chan tickDocument, checkpointJSON []byte, mergeCheckpoint bool) error {
	// We may be emitting documents to the same binding from either the streamer or the backfiller,
	// so make sure they don't get mixed up.
	c.mu.Lock()
	defer c.mu.Unlock()

	for doc := range docs {
		if docJSON, err := json.Marshal(doc); err != nil {
			return fmt.Errorf("error serializing document %q: %w", doc.Symbol, err) // TODO: This needs work.
		} else if err := c.output.Documents(uint32(c.bindingIdx), docJSON); err != nil {
			return err
		}
	}

	// Empty checkpoint should simply be merged. This would come from the streamer. The batcher
	// should do an actual checkpoint though.
	return c.output.Checkpoint(json.RawMessage(checkpointJSON), mergeCheckpoint)
}

func (c *alpacaClient) doStream(ctx context.Context) error {
	streamedCount := 0
	logTimer := time.NewTimer(streamerLoggingInterval)

	streamedTradeHandler := func(t marketdataStream.Trade) {
		docsChan := make(chan tickDocument)
		eg, _ := errgroup.WithContext(ctx) // TODO: Handle returned context
		eg.Go(func() error {
			return c.handleDocuments(docsChan, []byte(emptyCheckpoint), true)
		})

		// Send only a single document a time for these.
		docsChan <- tickDocument{
			ID:         t.ID,
			Symbol:     t.Symbol,
			Exchange:   t.Exchange,
			Price:      t.Price,
			Size:       t.Size,
			Timestamp:  t.Timestamp,
			Conditions: t.Conditions,
			Tape:       t.Tape,
		}
		close(docsChan)

		eg.Wait() // TODO: Handle the error from here also.

		select {
		case <-logTimer.C:
			log.WithFields(log.Fields{
				"handled":       streamedCount,
				"lastSymbol":    t.Symbol,
				"lastTimestamp": t.Timestamp,
			}).Info("trade streaming in progress")

			logTimer.Reset(streamerLoggingInterval)
		default:
		}
		streamedCount++
	}

	log.Info("starting streaming client")

	if err := c.streamClient.Connect(ctx); err != nil {
		return err // TODO: Better errors
	}

	if err := c.streamClient.SubscribeToTrades(streamedTradeHandler, c.symbols...); err != nil {
		return err
	}

	select {
	case err := <-c.streamClient.Terminated():
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *alpacaClient) doBackfill(ctx context.Context, start, end time.Time, maxInterval, minInterval time.Duration, caughtUp chan struct{}) error {
	var once sync.Once

	for {
		var err error
		if start, err = c.backfill(ctx, start, getEndDate(c.freePlan, end), maxInterval, minInterval); err != nil {
			return err
		}

		// If it took a long time to backfill we might not need to wait
		nextEnd := getEndDate(c.freePlan, end)
		if nextEnd.Sub(start) < minInterval {
			log.WithField("waitTime", minInterval).Info("waiting before backfilling historical trade data")

			// Send notification that the backfill is caught up if we haven't already.
			once.Do(func() { close(caughtUp) })

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(minInterval):
				// Fallthrough.
			}

		}
	}
}

func (c *alpacaClient) backfill(ctx context.Context, startLimit, endLimit time.Time, maxInterval, minInterval time.Duration) (time.Time, error) {
	// Sanity check, this should never happen.
	if startLimit.After(endLimit) {
		return time.Time{}, fmt.Errorf("start can't be after end: %s (start) vs %s (end)", startLimit, endLimit)
	}

	// Guard against start and end being so close that there is nothing to do.
	if endLimit.Sub(startLimit) < minInterval {
		return endLimit, nil
	}

	// For logging purposes.
	countedTrades := 0

	start := startLimit
	var end time.Time

	for {
		// If the group context has been cancelled, fail fast instead of continuing to complete a
		// possibly lengthy backfill.
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		default:
		}

		// Compute the ending time for this pass.
		if start.Add(maxInterval).Before(endLimit) {
			// Take the largest interval possible if that would not exceed our end time.
			end = start.Add(maxInterval)
		} else if endLimit.Sub(start) >= minInterval {
			// We have enough to do something, even though it is not up to the max interval. Read up to the end time.
			end = endLimit
		} else {
			// The difference between where we want to start and where the end limit is must not be
			// larger than the minimum interval, so we're done.
			log.WithFields(log.Fields{
				"start": startLimit,
				"end":   end,
				"count": countedTrades,
			}).Info("backfilled trades")

			return end, nil
		}

		params := marketdata.GetTradesParams{
			Start:    start,
			End:      end,
			Feed:     c.feed,
			Currency: defaultCurrency,
		}

		tChan := c.dataClient.GetMultiTradesAsync(c.symbols, params)

		// This is an unbounded list. We need to pass the channel to the document handler.
		docsChan := make(chan tickDocument)

		eg, _ := errgroup.WithContext(ctx)
		eg.Go(func() error {
			checkpoint := captureState{
				BackfilledUntil: map[string]time.Time{
					c.resourceName: end,
				},
			}

			serialized, err := json.Marshal(checkpoint)
			if err != nil {
				return fmt.Errorf("error serializing checkpoint %q: %w", checkpoint, err)
			}

			return c.handleDocuments(docsChan, serialized, true)
		})

		for item := range tChan {
			if err := item.Error; err != nil {
				return time.Time{}, err
			}

			if item.Trade.Update != "" {
				// Only capture "normal" trades.
				continue
			}

			docsChan <- tickDocument{
				ID:         item.Trade.ID,
				Symbol:     item.Symbol,
				Exchange:   item.Trade.Exchange,
				Price:      item.Trade.Price,
				Size:       item.Trade.Size,
				Timestamp:  item.Trade.Timestamp,
				Conditions: item.Trade.Conditions,
				Tape:       item.Trade.Tape,
			}

			countedTrades++
		}
		close(docsChan)

		// Wait until the checkpoint has been successfully committed.
		if err := eg.Wait(); err != nil {
			return time.Time{}, err
		}

		// Start the next pass where we left off
		start = end
	}
}

func getEndDate(freePlan bool, endDate time.Time) time.Time {
	// If an end date was explicitly provided, use that.
	if !endDate.IsZero() {
		return endDate
	}

	now := time.Now()

	// Free plans can't get data from within the last 15 minutes.
	if freePlan {
		now = now.Add(-1 * 15 * time.Minute)
	}

	return now
}
