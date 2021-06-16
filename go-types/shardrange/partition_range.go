package shardrange

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/minio/highwayhash"
)

// Range is the parsed shard labels that determine the range of partitions that this shard
// will be responsible for. The range is inclusive on both sides.
type Range struct {
	Begin uint32
	End   uint32
}

// NewFullRange returns a Range that covers the entire uint32 space.
func NewFullRange() Range {
	return Range{
		Begin: 0,
		End:   math.MaxUint32,
	}
}

func (pr *Range) UnmarshalJSON(bytes []byte) error {
	var tmp = struct{ Begin, End string }{}
	if err := json.Unmarshal(bytes, &tmp); err != nil {
		return err
	}

	if tmp.Begin != "" {
		begin, err := strconv.ParseUint(tmp.Begin, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'begin': %w", err)
		}
		pr.Begin = uint32(begin)
	}
	if tmp.End != "" {
		end, err := strconv.ParseUint(tmp.End, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'end': %w", err)
		}
		pr.End = uint32(end)
	}
	return nil
}

func (r Range) Includes(hash uint32) bool {
	return hash >= r.Begin && hash <= r.End
}

// Intersection returns the intersection of two overlapping PartitionRanges. If the ranges do not
// overlap, this function will panic.
func (r Range) Intersection(other Range) Range {
	var result = r
	if other.Begin > r.Begin {
		result.Begin = other.Begin
	}
	if other.End < r.End {
		result.End = other.End
	}
	if result.Begin > result.End {
		panic("intersected partition ranges that do not overlap")
	}
	return result
}

// IncludesHwHash determines whether the given partition id is included in this partition range.
// This uses a stable hash function (Highway hash) that is guaranteed never to change.
func (r Range) IncludesHwHash(partitionID []byte) bool {
	var hashed = hwHashPartition(partitionID)
	return r.Includes(hashed)
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
// DO NOT MODIFY this value, as it is required to have consistent hash results.
var highwayHashKey, _ = hex.DecodeString("332757d16f0fb1cf2d4f676f85e34c6a8b85aa58f42bb081449d8eb2e4ed529f")

func hwHashPartition(partitionId []byte) uint32 {
	return uint32(highwayhash.Sum64(partitionId, highwayHashKey) >> 32)
}

// OverlapResult is the result of checking whether one Range overlaps another.
type OverlapResult int

const (
	NoOverlap      OverlapResult = 0
	PartialOverlap OverlapResult = 1
	FullyInclusive OverlapResult = 2
)

func (rr OverlapResult) String() string {
	switch rr {
	case NoOverlap:
		return "NoOverlap"
	case PartialOverlap:
		return "PartialOverlap"
	case FullyInclusive:
		return "FullyInclusive"
	default:
		return fmt.Sprintf("invalid ShardRangeResult(%d)", int(rr))
	}
}

// Overlaps checks whether `other` overlaps this Range. Note that this is *not* reflexive. For example:
// [1-10].Overlaps([4-6]) == FullyInclusive
// But [4-6].Overlaps([1-10]) == PartialOverlap
func (pr Range) Overlaps(other Range) OverlapResult {
	var includesBegin = pr.Includes(other.Begin)
	var includesEnd = pr.Includes(other.End)
	if includesBegin && includesEnd {
		return FullyInclusive
	} else if includesBegin != includesEnd {
		return PartialOverlap
	} else {
		return NoOverlap
	}
}
