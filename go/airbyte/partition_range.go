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
	EndInclusive   uint32
}

func NewFullPartitionRange() PartitionRange {
	return PartitionRange{
		BeginInclusive: 0,
		EndInclusive:   math.MaxUint32,
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
		pr.EndInclusive = uint32(end)
	}
	return nil
}

// IncludesHwHash determines whether the given partition id is included in this partition range.
// This uses a stable hash function (Highway hash) that is guaranteed never to change.
func (r PartitionRange) IncludesHwHash(partitionID []byte) bool {
	var hashed = hwHashPartition(partitionID)
	return hashed >= r.BeginInclusive && hashed <= r.EndInclusive
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
// DO NOT MODIFY this value, as it is required to have consistent hash results.
var highwayHashKey, _ = hex.DecodeString("332757d16f0fb1cf2d4f676f85e34c6a8b85aa58f42bb081449d8eb2e4ed529f")

func hwHashPartition(partitionId []byte) uint32 {
	return uint32(highwayhash.Sum64(partitionId, highwayHashKey) >> 32)
}
