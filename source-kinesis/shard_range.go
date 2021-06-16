package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/estuary/connectors/go-types/shardrange"
)

// Applies the same hash operation that kinesis uses to determine which shard a given record should
// map to, and then truncates the lower 96 bits to translate it into the uint32 hashed key space.
func hashPartitionKey(key string) uint32 {
	var sum = md5.Sum([]byte(key))
	// This is meant to be equivalent to sum >> 96
	return binary.BigEndian.Uint32(sum[:4])
}

// Parses the kinesis shard range and translates it into a 32 bit PartitionRange, suitable for
// comparison with the Flow shard range.
func parseKinesisShardRange(begin, end string) (shardrange.Range, error) {
	var r = shardrange.Range{}
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

// Determines whether a record with the given `partitionKeyHash` should be processed by a Flow shard
// with the given `flowRange`. The `partitionKeyHash` is expected to have been computed using
// `hashPartitionKey`. Under normal circumstances, the `partitionKeyHash` will be within the
// `kinesisRange`, but this may not always be true if an "ExplicitHashKey" was used when adding the
// record. In that case, this function will always produce a consistent result that guarantees that
// exactly one Flow shard will process each record.
func isRecordWithinRange(flowRange shardrange.Range, kinesisRange shardrange.Range, partitionKeyHash uint32) bool {
	var rangeOverlap = flowRange.Intersection(kinesisRange)

	// Normally, the kinesis range will always include the key hash because that's normally how the
	// record would have been written to this kinesis shard in the first place. But kinesis also
	// allows supplying an `ExplicitHashKey`, which overrides the default hashing of the partition
	// key to allow for manually selecting which shard a record will be written to. If the
	// `ExplicitHashKey` was used, then the md5 hash of the partition key may fall outside of the
	// kinesis hash key range. If so, then we may still claim that record in the second or third
	// condition.
	if kinesisRange.Includes(partitionKeyHash) {
		return rangeOverlap.Includes(partitionKeyHash)
	} else if flowRange.Begin <= kinesisRange.Begin && partitionKeyHash < kinesisRange.Begin {
		return true
	} else if flowRange.End >= kinesisRange.End && partitionKeyHash >= kinesisRange.End {
		return true
	}
	return false
}
