package airbyte

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionRangeOverlap(t *testing.T) {
	assertOverlapResult(t, FullyInclusive, 0, math.MaxUint32, 0, math.MaxUint32)
	assertOverlapResult(t, FullyInclusive, 0, math.MaxUint32, 5, 6)
	assertOverlapResult(t, NoOverlap, 5, 6, 4, 5)
	assertOverlapResult(t, NoOverlap, 4, 6, 7, 8)

	// There's a number of special cases for dealing with empty ranges, so these are are to cover
	// those.
	assertOverlapResult(t, NoOverlap, 0, 5, 5, 5)
	assertOverlapResult(t, NoOverlap, 6, 8, 5, 5)
	assertOverlapResult(t, FullyInclusive, 0, 6, 5, 5)
	assertOverlapResult(t, FullyInclusive, 5, 6, 5, 5)
	assertOverlapResult(t, FullyInclusive, 0, math.MaxUint32, 0, 0)
	assertOverlapResult(t, FullyInclusive, 0, math.MaxUint32, math.MaxUint32, math.MaxUint32)
	assertOverlapResult(t, NoOverlap, 0, math.MaxUint32-1, math.MaxUint32, math.MaxUint32)
	assertOverlapResult(t, NoOverlap, 0, 0, 0, math.MaxUint32)
	assertOverlapResult(t, NoOverlap, 5, 5, 5, 5)
	assertOverlapResult(t, FullyInclusive, math.MaxUint32, math.MaxUint32, math.MaxUint32, math.MaxUint32)

}

func assertOverlapResult(t *testing.T, expected ShardRangeResult, aBegin, aEnd, bBegin, bEnd uint32) {
	var a = PartitionRange{
		BeginInclusive: aBegin,
		EndExclusive:   aEnd,
	}
	var b = PartitionRange{
		BeginInclusive: bBegin,
		EndExclusive:   bEnd,
	}
	require.Equalf(t, expected, a.Overlaps(b), "a: %#v, b: %#v", a, b)
}
