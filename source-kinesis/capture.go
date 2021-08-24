package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/protocols/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// readStream starts reading the given kinesis stream, delivering both records and errors from all
// kinsis shards over the given `resultsCh`. If `stopAt` is non-nil, then the this will only read
// records up through _approximately_ that time, or until millisBehindLatest indicates we're caught
// up to the tip of the stream. Otherwise, this will continue to read indefinitely.
// The `wg` is expected to have been incremented once _prior_ to calling this function. It will be
// further incremented and decremented behind the scenes, but will be decremented back down to the
// prior value when the read is finished.
func readStream(ctx context.Context, shardRange airbyte.Range, client *kinesis.Kinesis, stream string, state map[string]string, resultsCh chan<- readResult, stopAt *time.Time, wg *sync.WaitGroup) {

	var kc = &streamReader{
		client:         client,
		ctx:            ctx,
		stream:         stream,
		shardRange:     shardRange,
		dataCh:         resultsCh,
		readingShards:  make(map[string]bool),
		shardSequences: state,
		stopAt:         stopAt,
		waitGroup:      wg,
	}
	var err = kc.startReadingStream()
	// The waitGroup had 1 added to it prior to this function being called, and we decrement it now,
	// only after startReadingStream is done, because startReadingStream will synchronously
	// increment the waitGroup for each shard it reads.
	wg.Done()
	if err != nil {
		select {
		case resultsCh <- readResult{
			source: &recordSource{
				stream: stream,
			},
			err: err,
		}:
		case <-ctx.Done():
		}
	} else {
		log.WithField("stream", stream).Infof("Started reading kinesis stream")
	}
}

// Represents an ongoing read of a kinesis stream.
type streamReader struct {
	client             *kinesis.Kinesis
	ctx                context.Context
	stream             string
	shardRange         airbyte.Range
	dataCh             chan<- readResult
	stopAt             *time.Time
	waitGroup          *sync.WaitGroup
	readingShards      map[string]bool
	readingShardsMutex sync.Mutex
	// shardSequences is a copy of the capture state, which just tracks the sequenceID for each
	// kinesis shard. We keep this as a struct field so that we can ensure that all reads will use
	// the same state, regardless of whether they're triggered by the initial shard listing or
	// returned as a child shard id when reaching the end of an existing shard.
	shardSequences map[string]string
}

type recordSource struct {
	stream  string
	shardID string
}

// readResult is the message that's sent on the channel to the main thread. It will either contain
// records or an error.
type readResult struct {
	// Identifies the kinesis stream and shard that the records came from.
	source *recordSource
	err    error
	// A batch of records from kinesis.
	records []json.RawMessage
	// The highest sequence number in the batch, which should be added to the state.
	sequenceNumber string
}

// startReadingStream synchronously lists kinesis shards and begins background reads of the ones that overlap this capture shard range.
func (kc *streamReader) startReadingStream() error {
	initialShards, err := kc.listInitialShards()
	if err != nil {
		return fmt.Errorf("listing kinesis shards: %w", err)
	} else if len(initialShards) == 0 {
		return fmt.Errorf("no kinesis shards found for the given stream")
	}
	log.WithFields(log.Fields{
		"kinesisStream":        kc.stream,
		"initialKinesisShards": initialShards,
	}).Infof("Will start reading from %d kinesis shards", len(initialShards))

	for _, kinesisShard := range initialShards {
		var reader, err = kc.newShardReader(*kinesisShard.ShardId, func(_ string) (*kinesis.Shard, error) {
			return kinesisShard, nil
		})
		if err != nil {
			return err
		} else if reader != nil {
			kc.waitGroup.Add(1)
			go func() {
				reader.readShard()
				kc.waitGroup.Done()
			}()
		}
		// If reader == nil, then we should not read this shard
	}
	return nil
}

// We don't return _all_ shards here, but only the oldest parent shards. This is because child
// shards will be read automatically after reaching the end of the parents, so if we start reading
// child shards immediately then people may see events ingested out of order if they've merged or
// split shards recently. This function will only return the set of shards that should be read
// initially. It will omit shards that meet *all* of the following conditions:
// - Has a parent id
// - The parent shard still has data that is within the retention period.
// - This capture shard has not previously started reading that shard, as indicated by the state.
//
// This function will return the parents of shards that we've already started reading. This is
// intentional, and important, in order to handle the case where the Flow capture shard has split,
// and we need to also start reading shards that are siblings of those that are included in the
// state. We also don't do any filtering based on shard ranges here, in order to keep a single code
// path for doing that.
func (kc *streamReader) listInitialShards() ([]*kinesis.Shard, error) {
	var shardListing = make(map[string]*kinesis.Shard)
	var nextToken = ""
	for {
		var listShardsReq = kinesis.ListShardsInput{}
		if nextToken != "" {
			listShardsReq.NextToken = &nextToken
		} else {
			listShardsReq.StreamName = &kc.stream
		}
		listShardsResp, err := kc.client.ListShardsWithContext(kc.ctx, &listShardsReq)
		if err != nil {
			return nil, fmt.Errorf("listing shards: %w", err)
		}
		for _, shard := range listShardsResp.Shards {
			shardListing[*shard.ShardId] = shard
		}

		if listShardsResp.NextToken != nil && (*listShardsResp.NextToken) != "" {
			nextToken = *listShardsResp.NextToken
		} else {
			break
		}
	}

	// Now iterate the map and return all shards in the oldest generation.
	// Reading those shards will yield the child shards once we reach the end of each parent.
	var shards []*kinesis.Shard
	for shardID, shard := range shardListing {
		// Does the shard have a parent
		if shard.ParentShardId != nil {
			// Was the parent included in the ListShards output, meaning it still contains data that
			// falls inside the retention period.
			if _, parentIsListed := shardListing[*shard.ParentShardId]; parentIsListed {
				// And finally, have we already started reading from this shard? If so, then we'll
				// want to continue reading from it, even if it might otherwise be excluded.
				if _, ok := kc.shardSequences[shardID]; !ok {
					log.WithFields(log.Fields{
						"kinesisStream":        kc.stream,
						"kinesisShardId":       shardID,
						"kinesisParentShardId": *shard,
					}).Debug("Skipping shard for now since we will start reading a parent of this shard")
					continue
				}
			}
		}
		shards = append(shards, shard)
	}
	return shards, nil
}

// getShard returns the kinesis shard with the given id.
func (kc *streamReader) getShard(id string) (*kinesis.Shard, error) {
	var input = kinesis.ListShardsInput{
		StreamName: &kc.stream,
		ShardFilter: &kinesis.ShardFilter{
			ShardId: &id,
		},
	}
	var out, err = kc.client.ListShardsWithContext(kc.ctx, &input)
	if err != nil {
		return nil, err
	}
	if len(out.Shards) != 1 {
		return nil, fmt.Errorf("expected one shard with id: '%s' to be returned, got %d", id, len(out.Shards))
	}
	return out.Shards[0], nil
}

func (kc *streamReader) startReadingShardByID(shardID string) {
	kc.waitGroup.Add(1)
	go func() {
		var reader, err = kc.newShardReader(shardID, kc.getShard)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("reading kinesis shard failed")
			select {
			case <-kc.ctx.Done():
			case kc.dataCh <- readResult{
				source: &recordSource{stream: kc.stream, shardID: shardID},
				err:    err,
			}:
			}
		} else if reader != nil {
			reader.readShard()
		}
		// If reader is nil, then it's just because we're either already reading this shard, or it's
		// outside of our assigned range, so we just decrement the waitGroup
		kc.waitGroup.Done()
	}()
}

func (kc *streamReader) newShardReader(shardID string, getShard func(string) (*kinesis.Shard, error)) (*shardReader, error) {
	var shard, err = getShard(shardID)
	if err != nil {
		return nil, fmt.Errorf("getting shard: %w", err)
	}
	var logEntry = log.WithFields(log.Fields{
		"kinesisStream":     kc.stream,
		"kinesisShardId":    *shard.ShardId,
		"captureRangeStart": kc.shardRange.Begin,
		"captureRangeEnd":   kc.shardRange.End,
	})
	var source = &recordSource{
		stream:  kc.stream,
		shardID: *shard.ShardId,
	}
	kinesisRange, err := parseKinesisShardRange(*shard.HashKeyRange.StartingHashKey, *shard.HashKeyRange.EndingHashKey)
	if err != nil {
		return nil, fmt.Errorf("parsing kinesis shard range: %w", err)
	}
	var rangeResult = kc.shardRange.Overlaps(kinesisRange)
	if rangeResult == airbyte.NoRangeOverlap {
		logEntry.Info("Will not read kinesis shard because it falls outside of our hash range")
		return nil, nil
	}

	// Kinesis shards can merge or split, forming new child shards. We need to guard against reading
	// the same kinesis shard multiple times. This could happen if a shard is merged with another,
	// since they would both return the same child shard id. It could also happen on initialization
	// if we start reading an old shard that returns a child shard ID that we have already started
	// reading. To guard against this, we use a mutex around a map that tracks which shards we've
	// already started reading.
	kc.readingShardsMutex.Lock()
	var isReadingShard = kc.readingShards[*shard.ShardId]
	if !isReadingShard {
		kc.readingShards[*shard.ShardId] = true
	}
	kc.readingShardsMutex.Unlock()
	if isReadingShard {
		logEntry.Debug("A read for this kinesis shard is already in progress")
		return nil, nil
	}

	return &shardReader{
		rangeOverlap:      rangeResult,
		kinesisShardRange: kinesisRange,
		parent:            kc,
		source:            source,
		lastSequenceID:    kc.shardSequences[*shard.ShardId],
		noDataBackoff: noDataBackoff{
			initial:    time.Millisecond * 200,
			max:        time.Second,
			multiplier: 1.5,
		},
		// The maximum number of records to return in a single GetRecords request.
		// This starting value is somewhat arbitrarily based on records averaging roughly 500
		// bytes each. This limit will get adjusted automatically based on the actual average record
		// sizes, so this value is merely a reasonable starting point.
		limitPerReq: 2000,
		logEntry:    logEntry,
	}, nil
}

func isMissingResource(err error) bool {
	switch err.(type) {
	case *kinesis.ResourceNotFoundException:
		return true
	default:
		return false
	}
}

// A reader of an individual kinesis shard.
type shardReader struct {
	rangeOverlap airbyte.RangeOverlap
	// The key hash range of the kinesis shard, which has been translated from 128bit to the 32bit
	// space.
	kinesisShardRange airbyte.Range
	parent            *streamReader
	source            *recordSource
	lastSequenceID    string
	noDataBackoff     noDataBackoff
	limitPerReq       int64
	logEntry          *log.Entry
}

func (r *shardReader) readShard() {
	r.logEntry.WithField("RangeOverlap", r.rangeOverlap).Info("Starting read")
	defer r.logEntry.Info("Finished reading kinesis shard")

	for {
		var shardIter, err = r.getShardIterator()
		if err == nil {
			// We were able to obtain a shardIterator, so now we drive it as far as we can.
			// Only return if the error from readShardIterator is nil or indicates the context was
			// cancelled. For everything else, we'll retry with a new shard iterator. The everything
			// else here is most likely to be due to the stream being temporarily unavailable due to
			// re-sharding.
			if err = r.readShardIterator(shardIter); err == nil || isContextCanceled(err) {
				return
			} else {
				// Don't wait before retrying, since the previous failure was from GetRecords and
				// the next call will be to GetShardIterator, which have separate rate limits.
				r.logEntry.WithField("error", err).Warn("reading kinesis shardIterator returned error (will retry)")
			}
		} else if isMissingResource(err) {
			// This means that the shard fell off the end of the kinesis retention period sometime
			// after we started trying to read it. This is probably not indicative of any problem,
			// but rather just peculiar timing where we start reading the shard right before the
			// last record in the shard expires. Log it just because it's expected to be exceedingly
			// rare, and I want to know if it happens with any frequency.
			r.logEntry.Info("Stopping read of kinesis shard because it has been deleted")
			return
		} else {
			// oh well, we tried. Time to call it a day

			return
		}
	}
}

// Continuously loops and reads records until it encounters an error that requires acquisition of a new shardIterator.
func (r *shardReader) readShardIterator(iteratorID string) error {
	var shardIter = &iteratorID

	// GetRecords will immediately return a response without any records if there are none available
	// immediately. This means that this loop is executed very frequently, even when there is no
	// data available. The rate limiter helps prevent sending requests that would likely result in a
	// rate limit error anyway. But we still expect and handle such errors.
	var limiter = rate.NewLimiter(rate.Every(time.Second), 5)
	for shardIter != nil && (*shardIter) != "" {
		if err := limiter.Wait(r.parent.ctx); err != nil {
			return err
		}
		var getRecordsReq = kinesis.GetRecordsInput{
			ShardIterator: shardIter,
			Limit:         &r.limitPerReq,
		}
		var getRecordsResp, err = r.parent.client.GetRecordsWithContext(r.parent.ctx, &getRecordsReq)
		if err != nil {
			r.logEntry.WithField("error", err).Warn("reading kinesis shard iterator failed")
			return err
		}

		// If the response includes ChildShards, then this means that we've reached the end of the
		// shard because it has been either split or merged, so we need to start new reads of the
		// child shards.
		for _, childShard := range getRecordsResp.ChildShards {
			r.parent.startReadingShardByID(*childShard.ShardId)
		}

		if len(getRecordsResp.Records) > 0 {
			r.noDataBackoff.reset()
			r.updateRecordLimit(getRecordsResp)

			var lastSequenceID = *getRecordsResp.Records[len(getRecordsResp.Records)-1].SequenceNumber
			var msg = readResult{
				source:         r.source,
				records:        r.extractRecords(getRecordsResp),
				err:            err,
				sequenceNumber: lastSequenceID,
			}
			select {
			case r.parent.dataCh <- msg:
				r.lastSequenceID = lastSequenceID
			case <-r.parent.ctx.Done():
				return nil
			}
		} else {
			// Are we behind the tip of the shard? If so, then we'll make another request as soon as
			// we can. If we're caught up with the tip of the shard and there's still no data, then
			// we'll start applying a backoff so that we can releive some pressure on the network
			// when a kinesis shard has a lower data volume.
			if getRecordsResp.MillisBehindLatest != nil && *getRecordsResp.MillisBehindLatest > 0 {
				r.noDataBackoff.reset()
			} else {
				<-r.noDataBackoff.nextBackoff()
			}
		}

		// If the connector is not in tailing mode, then we'll check to see if we've read all the
		// records up through the timestamp of the desired stop point.
		if r.parent.stopAt != nil {
			if *getRecordsResp.MillisBehindLatest < 1000 {
				// Regardless of whether the response contains records or not, we can be done if
				// millisBehindLatest approaches 0, because that indicates that we've read all the
				// records in the stream, at least for now. We allow for some slippage here because
				// the returned value is relative to the tip of the _stream_, and so we may not ever
				// read millisBehindLatest of 0 if there are many shards, since it only takes one
				// record added to one shard to make that value non-zero.
				r.logEntry.Info("stopping because millisBehindLatest < 1000")
				return nil
			} else if len(getRecordsResp.Records) > 0 {
				var lastRecordTS = getRecordsResp.Records[len(getRecordsResp.Records)-1].ApproximateArrivalTimestamp
				if lastRecordTS.After(*r.parent.stopAt) {
					r.logEntry.Infof("Finished reading records through: %v", lastRecordTS)
					return nil
				}
			}
		}

		// A new ShardIterator will be returned even when there's no records returned. We need to
		// pass this value in the next GetRecords call. If we've reached the end of a shard, then
		// NextShardIterator will be empty, causing us to exit this loop and finish the read.
		shardIter = getRecordsResp.NextShardIterator
	}
	return nil
}

// Extracts the records from a response, filtering the records if necessary due to claiming partial
// ownership over the kinesis shard.
func (r *shardReader) extractRecords(resp *kinesis.GetRecordsOutput) []json.RawMessage {
	if r.rangeOverlap == airbyte.PartialRangeOverlap {
		var result []json.RawMessage
		for _, rec := range resp.Records {
			var keyHash = hashPartitionKey(*rec.PartitionKey)
			if isRecordWithinRange(r.parent.shardRange, r.kinesisShardRange, keyHash) {
				result = append(result, json.RawMessage(rec.Data))
			}
		}
		return result
	} else {
		var result = make([]json.RawMessage, len(resp.Records))
		for i, rec := range resp.Records {
			result[i] = json.RawMessage(rec.Data)
		}
		return result
	}
}

// Updates the Limit used for GetRecords requests. The goal is to always set the limit such that we
// can get about 1MiB of data returned on each request. This target is somewhat arbitrary, but
// seemed reasonable based on kinesis service limits here:
// https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
// This function will adjust the limit slowly so we don't end up going back and forth rapidly
// between wildly different limits.
func (r *shardReader) updateRecordLimit(resp *kinesis.GetRecordsOutput) {
	// update some stats, which we'll use to determine the Limit used in GetRecords requests
	var totalBytes int
	for _, rec := range resp.Records {
		totalBytes += len(rec.Data)
	}
	var avg int = totalBytes / len(resp.Records)
	var desiredLimit = (1024 * 1024) / avg
	var diff = int64(desiredLimit) - r.limitPerReq

	// Only move a fraction of the distance toward the desiredLimit. This helps even out big jumps
	// in average data size from request to request. If the average is fairly consistent, then we'll
	// still converge on the desiredLimit pretty quickly. The worst-case scenario where the average
	// record size approaches the 1MiB limit still converges (from 5000 to 2) in 20 iterations.
	var newLimit = r.limitPerReq + (diff / 3)
	if newLimit < 100 {
		newLimit = 100
	} else if newLimit > 10000 {
		newLimit = 10000
	}
	r.limitPerReq = newLimit
}

func (r *shardReader) getShardIterator() (string, error) {
	var shardIterReq = kinesis.GetShardIteratorInput{
		StreamName: &r.parent.stream,
		ShardId:    &r.source.shardID,
	}
	if r.lastSequenceID != "" {
		shardIterReq.StartingSequenceNumber = &r.lastSequenceID
		shardIterReq.ShardIteratorType = &START_AFTER_SEQ
	} else {
		shardIterReq.ShardIteratorType = &START_AT_BEGINNING
	}

	shardIterResp, err := r.parent.client.GetShardIteratorWithContext(r.parent.ctx, &shardIterReq)
	if err != nil {
		return "", err
	}
	return *shardIterResp.ShardIterator, nil
}

var (
	START_AFTER_SEQ    = "AFTER_SEQUENCE_NUMBER"
	START_AT_BEGINNING = "TRIM_HORIZON"
)

// isContextCanceled returns true if the error is due to a context cancelation.
// The AWS SDK will wrap context.Canceled errors, sometimes in multiple layers,
// which is what this function is meant to deal with.
func isContextCanceled(err error) bool {
	switch typed := err.(type) {
	case awserr.Error:
		return typed.Code() == "RequestCanceled" || isContextCanceled(typed.OrigErr())
	default:
		return err == context.Canceled
	}
}

// noDataBackoff is a simple exponential noDataBackoff implementation for use exclusively when the kinesis
// GetRecords response is empty AND we are caught up with the head of the stream. Since kinesis is
// polling based, it doesn't make a ton of sense for us to make 5 requests per second just in case
// some data may get added to a slow stream. So in that case we will wait a little longer in between
// requests.
type noDataBackoff struct {
	initial    time.Duration
	max        time.Duration
	multiplier float64
	next       time.Duration
}

func (b *noDataBackoff) nextBackoff() <-chan time.Time {
	if b.next == 0 {
		b.reset()
	}
	var ch = time.After(b.next)
	b.next = time.Millisecond * time.Duration(int64(float64(b.next.Milliseconds())*b.multiplier))
	if b.next > b.max {
		b.next = b.max
	}
	return ch
}

func (b *noDataBackoff) reset() {
	b.next = b.initial
}
