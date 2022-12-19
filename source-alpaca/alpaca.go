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

const emptyCheckpoint string = `{}`

type tickDocument struct {
	ID         int64
	Symbol     string
	Exchange   string
	Price      float64
	Size       uint32
	Timestamp  time.Time
	Conditions []string
	Tape       string
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
	currency     string
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
	streamedTradeHandler := func(t marketdataStream.Trade) {
		log.WithFields(log.Fields{
			"exchange":  t.Exchange,
			"timestamp": t.Timestamp,
			"symbol":    t.Symbol,
			"id":        t.ID,
		}).Debug("got streamed trade")

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
	}

	if err := c.streamClient.SubscribeToTrades(streamedTradeHandler, c.symbols...); err != nil {
		return err
	}

	// TODO: Error handling
	return c.streamClient.Connect(ctx)
}

func (c *alpacaClient) doBackfill(ctx context.Context, start time.Time, maxInterval, minInterval time.Duration) error {
	for {
		var err error
		if start, err = c.backfill(ctx, start, getEndDate(c.freePlan), maxInterval, minInterval); err != nil {
			return err
		}
		// If it took a long time to backfill we might not need to wait
		nextEnd := getEndDate(c.freePlan)
		if nextEnd.Sub(start) < minInterval {
			log.WithField("start", start).Debug("time interval wait starting")
			time.Sleep(minInterval)
		}
	}
}

func (c *alpacaClient) backfill(ctx context.Context, start, end time.Time, maxInterval, minInterval time.Duration) (time.Time, error) {
	// diagnostic
	countedTrades := 0

	// Sanity check
	if start.After(end) {
		return time.Time{}, fmt.Errorf("start can't be after end: %s (start) vs %s (end)", start, end)
	}

	var backfilledUntil time.Time

	// Loop until we are done
	for {
		var thisEnd time.Time

		if end.Sub(start) >= maxInterval {
			// Take the largest interval possible if that would not exceed our end time
			thisEnd = start.Add(maxInterval)
		} else if end.Sub(start) >= minInterval {
			// We have enough to do something, even though it is not up to the max interval. Read up to the end time.
			thisEnd = end
		} else {
			// We must be done if we got here.
			log.WithField("count", countedTrades).Debug("backfilled trades")
			return backfilledUntil, nil
		}

		params := marketdata.GetTradesParams{
			Start:    start,
			End:      thisEnd,
			Feed:     c.feed,     // default is IEX for free plans
			Currency: c.currency, // defaults to USD
			// asOf would probably be good for individual symbols
		}

		log.WithFields(log.Fields{
			"start": start,
			"end":   thisEnd,
		}).Debug("backfilling trades")

		tChan := c.dataClient.GetMultiTradesAsync(c.symbols, params)

		// This is an unbounded list. We need to pass the channel to the document handler.
		docsChan := make(chan tickDocument)
		eg, _ := errgroup.WithContext(ctx) // TODO: Handle returned context
		eg.Go(func() error {
			checkpoint := captureState{
				BackfilledUntil: make(map[string]string),
			}
			checkpoint.BackfilledUntil[c.resourceName] = thisEnd.String() // TODO: Timezones...

			serialized, err := json.Marshal(checkpoint)
			if err != nil {
				return fmt.Errorf("error serializing checkpoint %q: %w", checkpoint, err)
			}

			return c.handleDocuments(docsChan, serialized, true) // TODO: Figure out checkpoint
		})

		for item := range tChan {
			if err := item.Error; err != nil {
				return backfilledUntil, err
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
			log.WithFields(log.Fields{
				"exchange":  item.Trade.Exchange,
				"timestamp": item.Trade.Timestamp,
				"symbol":    item.Symbol,
				"id":        item.Trade.ID,
			}).Debug("got backfilled trade")
		}
		close(docsChan)

		// Wait until the checkpoint has been successfully committed.
		if err := eg.Wait(); err != nil {
			return backfilledUntil, err
		}

		// Start the next pass where we left off
		start = end
		backfilledUntil = thisEnd
	}
}

// Free plan has limits on how recent the data can be. Can't be from within the last 15 minutes.
func getEndDate(freePlan bool) time.Time {
	now := time.Now()

	if freePlan {
		now = now.Add(-1 * 15 * time.Minute)
	}

	return now
}
