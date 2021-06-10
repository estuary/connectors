package main

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/json"
	"fmt"
	//"math"
	"math/big"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/go/airbyte"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type kinesisCapture struct {
	client             *kinesis.Kinesis
	ctx                context.Context
	stream             string
	config             Config
	dataCh             chan<- readResult
	readingShards      map[string]bool
	readingShardsMutex sync.Mutex
	// shardSequences is a copy of the capture state, which just tracks the sequenceID for each
	// kinesis shard. We keep this as a struct field so that we can ensure that all reads will use
	// the same state, regardless of whether they're triggered by the initial shard listing or
	// returned as a child shard id when reaching the end of an existing shard.
	shardSequences map[string]string
	shardListing   map[string]*kinesis.Shard
}

type recordSource struct {
	stream  string
	shardID string
}

type readResult struct {
	Source         *recordSource
	Error          error
	Records        []json.RawMessage
	SequenceNumber string
}

func readStream(ctx context.Context, config Config, client *kinesis.Kinesis, stream string, state map[string]string, dataCh chan<- readResult) {
	var kc = &kinesisCapture{
		client:         client,
		ctx:            ctx,
		stream:         stream,
		config:         config,
		dataCh:         dataCh,
		readingShards:  make(map[string]bool),
		shardListing:   make(map[string]*kinesis.Shard),
		shardSequences: state,
	}
	var err = kc.startReadingStream()
	if err != nil {
		select {
		case dataCh <- readResult{
			Source: &recordSource{
				stream: stream,
			},
			Error: err,
		}:
		case <-ctx.Done():
		}
	} else {
		log.WithField("stream", stream).Infof("Started reading kinesis stream")
	}
}

func (kc *kinesisCapture) startReadingStream() error {
	initialShardIDs, err := kc.listInitialShards()
	if err != nil {
		return fmt.Errorf("listing kinesis shards: %w", err)
	} else if len(initialShardIDs) == 0 {
		return fmt.Errorf("No kinesis shards found for the given stream")
	}
	log.WithFields(log.Fields{
		"kinesisStream":        kc.stream,
		"initialKinesisShards": initialShardIDs,
	}).Infof("Will start reading from %d kinesis shards", len(initialShardIDs))

	// Start reading from all the known shards.
	for _, kinesisShardID := range initialShardIDs {
		go kc.startReadingShard(kinesisShardID)
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
func (kc *kinesisCapture) listInitialShards() ([]string, error) {
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
			kc.shardListing[*shard.ShardId] = shard
		}

		if listShardsResp.NextToken != nil && (*listShardsResp.NextToken) != "" {
			nextToken = *listShardsResp.NextToken
		} else {
			break
		}
	}

	// Now iterate the map and return all shards in the oldest generation.
	// Reading those shards will yield the child shards once we reach the end of each parent.
	var shards []string
	for shardID, shard := range kc.shardListing {
		// Does the shard have a parent
		if shard.ParentShardId != nil {
			// Was the parent included in the ListShards output, meaning it still contains data that
			// falls inside the retention period.
			if _, parentIsListed := kc.shardListing[*shard.ParentShardId]; parentIsListed {
				// And finally, have we already started reading from this shard? If so, then we'll
				// want to continue reading from it, even if it might otherwise be excluded.
				if _, ok := kc.shardSequences[shardID]; !ok {
					log.WithFields(log.Fields{
						"kinesisStream":        kc.stream,
						"kinesisShardId":       shardID,
						"kinesisParentShardId": *shard,
					}).Info("Skipping shard for now since we will start reading a parent of this shard")
					continue
				}
			}
		}
		shards = append(shards, shardID)
	}
	return shards, nil
}

// getShard returns the kinesis shard with the given id. If the shard was part of the original
// listing, then this will return that prior copy.
func (kc *kinesisCapture) getShard(id string) (*kinesis.Shard, error) {
	if shard := kc.shardListing[id]; shard != nil {
		return shard, nil
	}
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

func (kc *kinesisCapture) sendErr(err error, source *recordSource) {
	select {
	case kc.dataCh <- readResult{
		Source: source,
		Error:  err,
	}:
	case <-kc.ctx.Done():
	}
}

func parseKinesisShardRange(begin, end string) (airbyte.PartitionRange, error) {
	var r = airbyte.PartitionRange{}
	var begin128, ok = new(big.Int).SetString(begin, 10)
	if !ok {
		return r, fmt.Errorf("failed to parse kinesis shard range begin: '%s'", begin)
	}
	r.Begin = uint32(begin128.Rsh(begin128, 96).Uint64())

	end128, ok := new(big.Int).SetString(end, 10)
	if !ok {
		return r, fmt.Errorf("failed to parse kinesis shard range end: '%s'", end)
	}
	r.End = uint32(end128.Rsh(end128, 96).Uint64())

	return r, nil
}

func (kc *kinesisCapture) overlapsHashRange(shard *kinesis.Shard) (airbyte.ShardRangeResult, error) {
	var kinesisRange, err = parseKinesisShardRange(*shard.HashKeyRange.StartingHashKey, *shard.HashKeyRange.EndingHashKey)
	if err != nil {
		return airbyte.NoOverlap, err
	}
	return kc.config.PartitionRange.Overlaps(kinesisRange), nil
}

// startReadingShard is always intented to be called within its own new goroutine. It will first
// check whether the shard is already being read and return early if so.
func (kc *kinesisCapture) startReadingShard(shardID string) {
	var logEntry = log.WithFields(log.Fields{
		"kinesisStream":     kc.stream,
		"kinesisShardId":    shardID,
		"captureRangeStart": kc.config.PartitionRange.Begin,
		"captureRangeEnd":   kc.config.PartitionRange.End,
	})
	var shard, err = kc.getShard(shardID)
	if err != nil {
		kc.sendErr(err, &recordSource{stream: kc.stream, shardID: shardID})
		return
	}
	kinesisRange, err := parseKinesisShardRange(*shard.HashKeyRange.StartingHashKey, *shard.HashKeyRange.EndingHashKey)
	if err != nil {
		kc.sendErr(err, &recordSource{stream: kc.stream, shardID: shardID})
		return
	}
	rangeResult, err := kc.config.PartitionRange.Overlaps(kinesisRange), nil
	if err != nil {
		kc.sendErr(err, &recordSource{stream: kc.stream, shardID: shardID})
		return
	}
	if rangeResult == airbyte.NoOverlap {
		logEntry.Info("Will not read kinesis shard because it falls outside of our hash range")
		return
	}

	// Kinesis shards can merge or split, forming new child shards. We need to guard against reading
	// the same kinesis shard multiple times. This could happen if a shard is merged with another,
	// since they would both return the same child shard id. It could also happen on initialization
	// if we start reading an old shard that returns a child shard ID that we have already started
	// reading. To guard against this, we use a mutex around a map that tracks which shards we've
	// already started reading.
	kc.readingShardsMutex.Lock()
	var isReadingShard = kc.readingShards[shardID]
	if !isReadingShard {
		kc.readingShards[shardID] = true
	}
	kc.readingShardsMutex.Unlock()
	if isReadingShard {
		logEntry.Debug("A read for this kinesis shard is already in progress")
		return
	}
	defer logEntry.Info("Finished reading kinesis shard")
	logEntry.WithField("RangeOverlap", rangeResult).Info("Starting read")

	var source = &recordSource{
		stream:  kc.stream,
		shardID: shardID,
	}
	var shardReader = kinesisShardReader{
		filterRecords:     rangeResult == airbyte.PartialOverlap,
		kinesisShardRange: kinesisRange,
		parent:            kc,
		source:            source,
		lastSequenceID:    kc.shardSequences[shardID],
		noDataBackoff: backoff{
			initialMillis: 200,
			maxMillis:     1000,
			multiplier:    1.5,
		},
		errorBackoff: backoff{
			initialMillis: 200,
			maxMillis:     5000,
			multiplier:    1.5,
		},
		// The maximum number of records to return in a single GetRecords request.
		// This starting value is somewhat arbitrarily based on records averaging roughly 500
		// bytes each. This limit will get adjusted automatically based on the actual average record
		// sizes, so this value is merely a reasonable starting point.
		limitPerReq: 2000,
		logEntry:    logEntry,
	}

	for {
		var shardIter, err = kc.getShardIterator(shardID, shardReader.lastSequenceID)
		if err == nil {
			// We were able to obtain a shardIterator, so now we drive it as far as we can.
			if err = shardReader.readShardIterator(shardIter); err == nil || isContextCanceled(err) {
				return
			} else {
				// Don't wait before retrying, since the previous failure was from GetRecords and
				// the next call will be to GetShardIterator, which have separate rate limits.
				logEntry.WithField("error", err).Warn("reading kinesis shardIterator returned error (will retry)")
			}
		} else if request.IsErrorRetryable(err) {
			select {
			case <-shardReader.errorBackoff.nextBackoff():
				err = nil
				// loop around and try again
			case <-kc.ctx.Done():
				return
			}
		} else if isMissingResource(err) {
			// This means that the shard fell off the end of the kinesis retention period sometime
			// after we started trying to read it. This is probably not indicative of any problem,
			// but rather just peculiar timing where we start reading the shard right before the
			// last record in the shard expires. Log it just because it's expected to be exceedingly
			// rare, and I want to know if it happens with any frequency.
			logEntry.Info("Stopping read of kinesis shard because it has been deleted")
			return
		} else if isContextCanceled(err) {
			return
		} else {
			// oh well, we tried. Time to call it a day
			logEntry.WithField("error", err).Error("reading kinesis shard failed")
			var message = readResult{
				Error:  err,
				Source: source,
			}
			select {
			case kc.dataCh <- message:
				return
			case <-kc.ctx.Done():
				return
			}
		}
	}
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
type kinesisShardReader struct {
	// filterRecords indicates that this capture shard is only partially responsible for  the given
	// kinesis shard. This happens when the flow capture shard key range only partially overlaps the
	// key hash range of the kinesis shard. If false, then this capture shard is exclusively
	// responsible for all the records in the kinesis shard.
	filterRecords bool
	// The key hash range of the kinesis shard, which has been translated from 128bit to the 32bit
	// space.
	kinesisShardRange airbyte.PartitionRange
	parent            *kinesisCapture
	source            *recordSource
	lastSequenceID    string
	noDataBackoff     backoff
	errorBackoff      backoff
	limitPerReq       int64
	logEntry          *log.Entry
}

// Continuously loops and reads records until it encounters an error that requires acquisition of a new shardIterator.
func (r *kinesisShardReader) readShardIterator(iteratorID string) error {
	var shardIter = &iteratorID

	var errorBackoff = backoff{
		initialMillis: 250,
		maxMillis:     5000,
		multiplier:    2.0,
	}
	// This separate backoff is used only for cases where GetRecords returns no data.
	// The initialMillis is set to match the 5 TPS rate limit of the api.
	var noDataBackoff = backoff{
		initialMillis: 200,
		maxMillis:     1000,
		multiplier:    1.5,
	}
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
		if err == nil {
			errorBackoff.reset()
		} else if request.IsErrorRetryable(err) {
			r.logEntry.WithField("error", err).Warn("got kinesis error (will retry)")
			err = nil
			select {
			case <-errorBackoff.nextBackoff():
			case <-r.parent.ctx.Done():
				return r.parent.ctx.Err()
			}
		} else {
			r.logEntry.WithField("error", err).Warn("reading kinesis shard iterator failed")
			return err
		}

		// If the response includes ChildShards, then this means that we've reached the end of the
		// shard because it has been either split or merged, so we need to start new reads of the
		// child shards.
		for _, childShard := range getRecordsResp.ChildShards {
			go r.parent.startReadingShard(*childShard.ShardId)
		}

		if len(getRecordsResp.Records) > 0 {
			noDataBackoff.reset()
			r.updateRecordLimit(getRecordsResp)

			var lastSequenceID = *getRecordsResp.Records[len(getRecordsResp.Records)-1].SequenceNumber
			var msg = readResult{
				Source:         r.source,
				Records:        r.copyRecords(getRecordsResp),
				Error:          err,
				SequenceNumber: lastSequenceID,
			}
			select {
			case r.parent.dataCh <- msg:
				r.lastSequenceID = lastSequenceID
			case <-r.parent.ctx.Done():
				return nil
			}
		} else {
			// If there were no records in the response then we'll wait at least a while before
			// making another request. The amount of time we wait depends on whether we're caught up
			// or not. If the response indicates that there is more data in the shard, then we'll
			// wait the minimum amount of time so that we don't overflow the 5 TPS rate limit on
			// GetRecords. If we're caught up, then we'll increase the backoff a bit.
			if getRecordsResp.MillisBehindLatest != nil && *getRecordsResp.MillisBehindLatest > 0 {
				noDataBackoff.reset()
			}
			<-noDataBackoff.nextBackoff()
		}

		// A new ShardIterator will be returned even when there's no records returned. We need to
		// pass this value in the next GetRecords call. If we've reached the end of a shard, then
		// NextShardIterator will be empty, causing us to exit this loop and finish the read.
		shardIter = getRecordsResp.NextShardIterator
	}
	return nil
}

func isRecordWithinRange(flowRange airbyte.PartitionRange, kinesisRange airbyte.PartitionRange, keyHash uint32) bool {
	var rangeOverlap = flowRange.Intersection(kinesisRange)

	// Normally, the kinesis range will always include the key hash because that's normally how the
	// record would have been written to this kinesis shard in the first place. But kinesis also
	// allows supplying an `ExplicitHashKey`, which overrides the default hashing of the partition
	// key to allow for manually selecting which shard a record will be written to. If the
	// `ExplicitHashKey` was used, then the md5 hash of the partition key may fall outside of the
	// kinesis hash key range. If so, then we may still claim that record in the second or third
	// condition.
	if kinesisRange.Includes(keyHash) {
		log.Infof("PartitionKey hashes into normal kinesis shard range (yay)")
		return rangeOverlap.Includes(keyHash)
	} else if flowRange.Begin <= kinesisRange.Begin && keyHash < kinesisRange.Begin {
		log.Infof("PartitionKey hashes below kinesis shard range (booo)")
		return true
	} else if flowRange.End >= kinesisRange.End && keyHash >= kinesisRange.End {
		log.Infof("PartitionKey hashes above kinesis shard range (booo)")
		return true
	}
	return false
}

func (r *kinesisShardReader) copyRecords(resp *kinesis.GetRecordsOutput) []json.RawMessage {
	if r.filterRecords {
		var result []json.RawMessage
		for _, rec := range resp.Records {
			var keyHash = hashPartitionKey(rec.PartitionKey)
			if isRecordWithinRange(*r.parent.config.PartitionRange, r.kinesisShardRange, keyHash) {
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

// Applies the same hash operation that kinesis uses to determine which shard a given record should
// map to, and then truncates the lower 96 bits to translate it into the uint32 hashed key space.
func hashPartitionKey(key *string) uint32 {
	var sum = md5.Sum([]byte(*key))
	// This is meant to be equivalent to sum >> 96
	// TODO: verify that the right byte order with actual kinesis. This works with localstack, so
	// hopefully the two are the same
	return binary.BigEndian.Uint32(sum[:4])
}

// Updates the Limit used for GetRecords requests. The goal is to always set the limit such that we
// can get about 1MiB of data returned on each request. This target is somewhat arbitrary, but
// seemed reasonable based on kinesis service limits here:
// https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
// This function will adjust the limit slowly so we don't end up going back and forth rapidly
// between wildly different limits.
func (r *kinesisShardReader) updateRecordLimit(resp *kinesis.GetRecordsOutput) {
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

func (kc *kinesisCapture) getShardIterator(shardID, sequenceID string) (string, error) {
	var shardIterReq = kinesis.GetShardIteratorInput{
		StreamName: &kc.stream,
		ShardId:    &shardID,
	}
	if sequenceID != "" {
		shardIterReq.StartingSequenceNumber = &sequenceID
		shardIterReq.ShardIteratorType = &START_AFTER_SEQ
	} else {
		shardIterReq.ShardIteratorType = &START_AT_BEGINNING
	}

	shardIterResp, err := kc.client.GetShardIteratorWithContext(kc.ctx, &shardIterReq)
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
