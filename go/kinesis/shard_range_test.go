package main

import (
	"math"
	//"math/big"
	"testing"

	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/go/airbyte"
	"github.com/stretchr/testify/require"
)

// max u128
const maxKinesisHash = "340282366920938463463374607431768211455"

func TestShardRangeTranslation(t *testing.T) {
	var result, err = parseKinesisShardRange("0", maxKinesisHash)
	require.NoError(t, err)
	require.Equal(t, uint32(math.MaxUint32), result.EndExclusive)
	require.Equal(t, uint32(0), result.BeginInclusive)

	result, err = parseKinesisShardRange("65536", "1584563250654221633430969384980")
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.BeginInclusive)
	require.Equal(t, uint32(20), result.EndExclusive)
}

func TestShardRangeOverlaps(t *testing.T) {
	testRangeOverlap(t, airbyte.FullyInclusive, 0, math.MaxUint32, "0", maxKinesisHash)
	testRangeOverlap(t, airbyte.FullyInclusive, 0, math.MaxUint32, "1", maxKinesisHash)
	testRangeOverlap(t, airbyte.PartialOverlap, 1, math.MaxUint32, "0", maxKinesisHash)
	// This huge number is equivalent to 5 << 96, so this case is testing the boundary where the
	// kinesis range begin is the same as the flow range exclusive end.
	testRangeOverlap(t, airbyte.NoOverlap, 0, 5, "396140812571321687967719751680", maxKinesisHash)

	// This huge number is equivalent to 20 << 96, so should partially overlap
	testRangeOverlap(t, airbyte.PartialOverlap, 10, 21, "1584563250285286751870879006720", maxKinesisHash)
	testRangeOverlap(t, airbyte.FullyInclusive, 20, 21, "1584563250285286751870879006720", "1584563250285286751870879006721")
}

func testRangeOverlap(t *testing.T, expected airbyte.ShardRangeResult, flowBegin, flowEnd uint32, kinesisBegin, kinesisEnd string) {
	var flowRange = &airbyte.PartitionRange{
		BeginInclusive: flowBegin,
		EndExclusive:   flowEnd,
	}
	var kinesisRange, err = parseKinesisShardRange(kinesisBegin, kinesisEnd)
	require.NoError(t, err, "failed to convert kinesis hash key range")
	var result = flowRange.Overlaps(kinesisRange)

	require.Equalf(t, expected, result, "flowRange: %#v, kinesisRange: %s-%s, translatedKinesisRange: %#v", flowRange, kinesisBegin, kinesisEnd, kinesisRange)
}
