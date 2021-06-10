package airbyte

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/minio/highwayhash"
)

// PartitionRange is the parsed shard labels that determine the range of partitions that this shard
// will be responsible for. Note that this is not actually a part of the airbyte spec. This is
// included in the airbyte package because we might eventually propose adding it to the spec, and
// for lack of a better place to put it.
type PartitionRange struct {
	BeginInclusive uint32
	EndExclusive   uint32
}

func NewFullPartitionRange() PartitionRange {
	return PartitionRange{
		BeginInclusive: 0,
		EndExclusive:   math.MaxUint32,
	}
}

func (pr *PartitionRange) UnmarshalJSON(bytes []byte) error {
	var tmp = struct{ Begin, End string }{}
	if err := json.Unmarshal(bytes, &tmp); err != nil {
		return err
	}

	if tmp.Begin != "" {
		begin, err := strconv.ParseUint(tmp.Begin, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'begin': %w", err)
		}
		pr.BeginInclusive = uint32(begin)
	}
	if tmp.End != "" {
		end, err := strconv.ParseUint(tmp.End, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'end': %w", err)
		}
		pr.EndExclusive = uint32(end)
	}
	return nil
}

func (r PartitionRange) Includes(hash uint32) bool {
	if hash >= r.BeginInclusive {
		if hash < r.EndExclusive {
			return true
		} else if hash == math.MaxUint32 && r.EndExclusive == math.MaxUint32 {
			return true
		}
	}
	return false
}

// Intersection returns the intersection of two overlapping PartitionRanges. If the ranges do not
// overlap, this function will panic.
func (r PartitionRange) Intersection(other PartitionRange) PartitionRange {
	var result = PartitionRange{}
	if r.BeginInclusive > other.BeginInclusive {
		result.BeginInclusive = r.BeginInclusive
	} else {
		result.BeginInclusive = other.BeginInclusive
	}

	if r.EndExclusive < other.EndExclusive {
		result.EndExclusive = r.EndExclusive
	} else {
		result.EndExclusive = other.EndExclusive
	}
	if result.BeginInclusive >= result.EndExclusive {
		panic("intersected partition ranges that do not overlap")
	}
	return result
}

// IncludesHwHash determines whether the given partition id is included in this partition range.
// This uses a stable hash function (Highway hash) that is guaranteed never to change.
func (r PartitionRange) IncludesHwHash(partitionID []byte) bool {
	var hashed = hwHashPartition(partitionID)
	return hashed >= r.BeginInclusive && hashed <= r.EndExclusive
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
// DO NOT MODIFY this value, as it is required to have consistent hash results.
var highwayHashKey, _ = hex.DecodeString("332757d16f0fb1cf2d4f676f85e34c6a8b85aa58f42bb081449d8eb2e4ed529f")

func hwHashPartition(partitionId []byte) uint32 {
	return uint32(highwayhash.Sum64(partitionId, highwayHashKey) >> 32)
}

type ShardRangeResult int

const (
	NoOverlap      ShardRangeResult = 0
	PartialOverlap ShardRangeResult = 1
	FullyInclusive ShardRangeResult = 2
)

func (rr ShardRangeResult) String() string {
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

func (pr PartitionRange) Overlaps(other PartitionRange) ShardRangeResult {
	var includesBegin = pr.Includes(other.BeginInclusive)

	var lastIncluded = other.EndExclusive
	if lastIncluded != 0 && lastIncluded != math.MaxUint32 && lastIncluded > other.BeginInclusive {
		lastIncluded--
	}
	var includesEnd = pr.Includes(lastIncluded)
	if includesBegin && includesEnd {
		return FullyInclusive
	} else if includesBegin != includesEnd {
		return PartialOverlap
	} else {
		return NoOverlap
	}
	// Is the other range empty? If so, then we will need to slightly adjust our comparison to
	// account for the case where other.BeginInclusive == other.EndExclusive == pr.BeginInclusive
	/*
		if other.BeginInclusive == other.EndExclusive &&
			pr.BeginInclusive <= other.BeginInclusive && pr.EndExclusive > other.EndExclusive {
			return FullyInclusive
		}
		if pr.BeginInclusive < other.EndExclusive && pr.EndExclusive > other.BeginInclusive {
			if pr.BeginInclusive <= other.BeginInclusive && pr.EndExclusive >= other.EndExclusive {
				return FullyInclusive
			} else {
				return PartialOverlap
			}
		} else {
			return NoOverlap
		}
	*/
}
