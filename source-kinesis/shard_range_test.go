package main

import (
	"math"
	"testing"

	"github.com/estuary/connectors/go-types/shardrange"
	"github.com/stretchr/testify/require"
)

// max u128
const maxKinesisHash = "340282366920938463463374607431768211455"

// Ideally, our behavior will match theirs exactly, although technically our results will be correct
// even if it is different. If our behavior doesn't match, then there could potentially be
// pathalogical edge cases where records from partially overlapping kinesis shards get hashed
// disproportionately into a flow capture shard. The AWS docs don't document the particulars around
// hashing partition keys, other than the fact that they use md5. This test exists to verify that
// the given partition keys hash into the same kinesis shard that kinsis itself determines. Kinesis
// doesn't expose the actual hashed values, only the unhashed partition key and the shard id that it
// went into. So this was setup by manually creating a kinesis stream with 2 shards, adding records
// with the given partition keys and seeing which shard they went into. This can, and possibly
// should, be automated later, but for now it seems to give a level of confidence that's high
// enough.
func TestHashPartitionKeyMatchesKinesisHashing(t *testing.T) {
	// The hash key range of the first of 2 kinesis shards.
	var shard0Range, err = parseKinesisShardRange("0", "170141183460469231731687303715884105727")
	require.NoError(t, err)

	var testCases = []struct {
		// The partition key that was provided in put-record
		partitionKey string
		// Whether the record was put into shard 0. False means it was put into shard 1
		included bool
	}{
		{"canary", true},
		{"fooo", false},
		{"barticus", false},
		{"snapple", false},
		{"joseph", false},
		{"jessica", false},
		{"jeebus", true},
		{"daffy", true},
		{"fartition", true},
		{"pancakes", false},
		{"waffles", false},
		{"crepes", false},
	}
	for _, tc := range testCases {
		var keyHash = hashPartitionKey(tc.partitionKey)
		require.Equalf(t, tc.included, shard0Range.Includes(keyHash), "TC: %#v", tc)
	}
}

func TestShardRangeTranslation(t *testing.T) {
	var result, err = parseKinesisShardRange("0", maxKinesisHash)
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), result.End)
	require.Equal(t, uint32(0), result.Begin)

	result, err = parseKinesisShardRange("65536", "1584563250654221633430969384980")
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.Begin)
	require.Equal(t, uint32(20), result.End)
}

func TestShardRangeOverlaps(t *testing.T) {
	testRangeOverlap(t, shardrange.FullyInclusive, 0, math.MaxUint32, "0", maxKinesisHash)
	testRangeOverlap(t, shardrange.FullyInclusive, 0, math.MaxUint32, "1", maxKinesisHash)
	testRangeOverlap(t, shardrange.PartialOverlap, 1, math.MaxUint32, "0", maxKinesisHash)
	// This huge number is equivalent to 5 << 96, so this case is testing the boundary where the
	// kinesis range begin is the same as the flow range exclusive end.
	testRangeOverlap(t, shardrange.PartialOverlap, 0, 5, "396140812571321687967719751680", maxKinesisHash)
	testRangeOverlap(t, shardrange.NoOverlap, 0, 5, "475368975085586025561263702016", maxKinesisHash)

	// This huge number is equivalent to 20 << 96, so should partially overlap
	testRangeOverlap(t, shardrange.PartialOverlap, 10, 20, "1584563250285286751870879006720", maxKinesisHash)
	testRangeOverlap(t, shardrange.FullyInclusive, 20, 20, "1584563250285286751870879006720", "1584563250285286751870879006721")
}

func testRangeOverlap(t *testing.T, expected shardrange.OverlapResult, flowBegin, flowEnd uint32, kinesisBegin, kinesisEnd string) {
	var flowRange = &shardrange.Range{
		Begin: flowBegin,
		End:   flowEnd,
	}
	var kinesisRange, err = parseKinesisShardRange(kinesisBegin, kinesisEnd)
	require.NoError(t, err, "failed to convert kinesis hash key range")
	var result = flowRange.Overlaps(kinesisRange)

	require.Equalf(t, expected, result, "flowRange: %#v, kinesisRange: %s-%s, translatedKinesisRange: %#v", flowRange, kinesisBegin, kinesisEnd, kinesisRange)
}
