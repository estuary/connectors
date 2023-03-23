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
	emptyCheckpoint         = `{}`
	defaultCurrency         = "usd"
	streamerLoggingInterval = 1 * time.Minute
	backfillDely            = 1 * time.Minute
)

type tradeDocument struct {
	ID         int64     `json:"ID" jsonschema:"title=ID,description=Trade ID"`
	Symbol     string    `json:"Symbol" jsonschema:"title=Symbol,description=Symbol"`
	Exchange   string    `json:"Exchange" jsonschema:"title=Exchange,description=Exchange where the trade happened"`
	Price      float64   `json:"Price" jsonschema:"title=Price,description=Trade price"`
	Size       uint32    `json:"Size" jsonschema:"title=Size,description=Trade size"`
	Timestamp  time.Time `json:"Timestamp" jsonschema:"title=Timestamp,description=Timestamp in RFC-3339 format with nanosecond precision"`
	Conditions []string  `json:"Conditions" jsonschema:"title=Conditions,description=Trade conditions"`
	Tape       string    `json:"Tape" jsonschema:"title=Tape,description=Tape"`
}

type alpacaWorker struct {
	mu           sync.Mutex
	flowStream   *boilerplate.PullOutput
	bindingIdx   int
	dataClient   marketdata.Client
	streamClient marketdataStream.StocksClient
	resourceName string
	symbols      []string
	feed         string
	freePlan     bool
}

func (c *alpacaWorker) handleDocuments(docs <-chan tradeDocument, checkpointJSON []byte) error {
	// We may be emitting documents to the same binding from either the streamer or the backfiller,
	// so this synchronization makes sure checkpoints do not get interleaved between the two.
	c.mu.Lock()
	defer c.mu.Unlock()

	for doc := range docs {
		if docJSON, err := json.Marshal(doc); err != nil {
			return fmt.Errorf("error serializing document: %w", err)
		} else if err := c.flowStream.Documents(c.bindingIdx, docJSON); err != nil {
			return err
		}
	}

	return c.flowStream.Checkpoint(json.RawMessage(checkpointJSON), true)
}

func (c *alpacaWorker) streamTrades(ctx context.Context) error {
	streamedCount := 0
	logTimer := time.NewTimer(streamerLoggingInterval)

	// Communicate errors encountered inside the async trade handler callback so we can respond
	// appropriately in the main thread.
	handlerErr := make(chan error)

	streamedTradeHandler := func(t marketdataStream.Trade) {
		docsChan := make(chan tradeDocument)
		eg := errgroup.Group{}
		eg.Go(func() error {
			return c.handleDocuments(docsChan, []byte(emptyCheckpoint))
		})

		// For streaming trades, we send only a single document at a time. The channel gymnastics
		// here are a bit cumbersome, but necessary so that the document handler is also compatible
		// with the many documents produced at a time by the backfiller.
		docsChan <- tradeDocument{
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

		if err := eg.Wait(); err != nil {
			handlerErr <- err
		}

		streamedCount++

		// Log progress of the streaming client at reasonable time intervals to provide evidence
		// that it is still running while not spamming the logs overly.
		select {
		case <-logTimer.C:
			log.WithFields(log.Fields{
				"binding":       c.resourceName,
				"handled":       streamedCount,
				"lastSymbol":    t.Symbol,
				"lastTimestamp": t.Timestamp,
			}).Info("trade streaming in progress")

			logTimer.Reset(streamerLoggingInterval)
		default:
		}
	}

	log.WithField("binding", c.resourceName).Info("starting streaming client")

	if err := c.streamClient.Connect(ctx); err != nil {
		return fmt.Errorf("streaming client connect: %w", err)
	}

	if err := c.streamClient.SubscribeToTrades(streamedTradeHandler, c.symbols...); err != nil {
		return fmt.Errorf("streaming client subscribe to trades: %w", err)
	}

	select {
	case err := <-handlerErr:
		return fmt.Errorf("emitting documents: %w", err)
	case err := <-c.streamClient.Terminated():
		return fmt.Errorf("streaming client terminated: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *alpacaWorker) backfillTrades(ctx context.Context, start, end time.Time, maxInterval, minInterval time.Duration, caughtUp chan struct{}) error {
	var once sync.Once

	// This loop manages the cycle of catching up the backfill to the present time and then waiting
	// for a short duration before performing smaller incremental backfills.
	for {
		var err error
		thisEnd := getEndDate(c.freePlan, end)
		dataDurationCovered := thisEnd.Sub(start)

		now := time.Now()
		if start, err = c.doBackfill(ctx, start, thisEnd, maxInterval, minInterval); err != nil {
			return err
		}
		dataCollectionTime := time.Since(now)

		nextEnd := getEndDate(c.freePlan, end)
		if dataCollectionTime > dataDurationCovered && dataCollectionTime > minInterval {
			log.WithFields(log.Fields{
				"binding":             c.resourceName,
				"dataDurationCovered": dataDurationCovered.String(),
				"dataCollectionTime":  dataCollectionTime.String(),
			}).Warn("backfilling previous time period took longer than trades spanned by that time period - backfill may be falling behind")
		}

		if nextEnd.Sub(start) < minInterval {
			log.WithFields(log.Fields{
				"binding":  c.resourceName,
				"waitTime": minInterval.String(),
			}).Info("waiting before backfilling historical trade data")

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

// doBackfill will backfill trade documents starting at startLimit and ending within minInterval of
// endLimit. On success, it will always return how far up to the current time the backfill
// completed. This may not be exactly equal to the provided endLimit if the last chunk of time is
// less than minInterval.
func (c *alpacaWorker) doBackfill(ctx context.Context, startLimit, endLimit time.Time, maxInterval, minInterval time.Duration) (time.Time, error) {
	// Sanity check, this should never happen.
	if startLimit.After(endLimit) {
		return time.Time{}, fmt.Errorf("start can't be after end: %s (start) vs %s (end)", startLimit, endLimit)
	}

	// Guard against start and end being so close that there is nothing to do.
	if endLimit.Sub(startLimit) < minInterval {
		return endLimit, nil
	}

	// For logging purposes.
	totalTrades := 0

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
				"binding": c.resourceName,
				"start":   startLimit,
				"end":     end,
				"count":   totalTrades,
			}).Info("finished backfilling trades")

			return end, nil
		}

		params := marketdata.GetTradesParams{
			Start:    start,
			End:      end,
			Feed:     c.feed,
			Currency: defaultCurrency,
		}

		tChan := c.dataClient.GetMultiTradesAsync(c.symbols, params)

		docsChan := make(chan tradeDocument)

		eg := errgroup.Group{}
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

			return c.handleDocuments(docsChan, serialized)
		})

		theseTrades := 0
		for item := range tChan {
			if err := item.Error; err != nil {
				return time.Time{}, fmt.Errorf("trade item error: %w", err)
			}

			if item.Trade.Update != "" {
				// Only capture "normal" trades.
				continue
			}

			docsChan <- tradeDocument{
				ID:         item.Trade.ID,
				Symbol:     item.Symbol,
				Exchange:   item.Trade.Exchange,
				Price:      item.Trade.Price,
				Size:       item.Trade.Size,
				Timestamp:  item.Trade.Timestamp,
				Conditions: item.Trade.Conditions,
				Tape:       item.Trade.Tape,
			}

			theseTrades++
			totalTrades++
		}
		close(docsChan)

		// Wait until the checkpoint has been successfully committed.
		if err := eg.Wait(); err != nil {
			return time.Time{}, err
		}

		log.WithFields(log.Fields{
			"binding": c.resourceName,
			"start":   start,
			"end":     end,
			"count":   theseTrades,
		}).Info("backfilling trades in progress")

		// Start the next pass where we left off.
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
	} else {
		// Slight delay on how recent the backfill can query for to account for instability in very
		// recently received data on Alpaca's servers.
		now = now.Add(-1 * backfillDely)
	}

	return now
}
