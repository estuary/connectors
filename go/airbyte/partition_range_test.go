package airbyte

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionRangeOverlap(t *testing.T) {
	for _, testCase := range []struct {
		expected                 ShardRangeResult
		flowStart, flowEnd       uint32
		kinesisStart, kinesisEnd uint32
	}{
		{FullyInclusive, 0, math.MaxUint32, 0, math.MaxUint32},
		{FullyInclusive, 0, math.MaxUint32, 5, 5},
		{PartialOverlap, 5, 6, 4, 5},
		{FullyInclusive, 4, 6, 6, 6},
		{NoOverlap, 0, 5, 9, 10},
		{NoOverlap, 6, 8, 0, 0},
	} {
		var flowRange = PartitionRange{
			Begin: testCase.flowStart,
			End:   testCase.flowEnd,
		}
		var kinesisRange = PartitionRange{
			Begin: testCase.kinesisStart,
			End:   testCase.kinesisEnd,
		}
		require.Equalf(t, testCase.expected, flowRange.Overlaps(kinesisRange), "testCase: %#v", testCase)
	}
}
