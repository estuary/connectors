package protocol

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/minio/highwayhash"
)

// PartitionRange is the parsed shard labels that determine the range of partitions that this shard
// will be responsible for.
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

// TODO: this is gross, but I'm not sure if there's a better way
func (pr *PartitionRange) UnmarshalJSON(bytes []byte) error {
	// This map will be the whole object if
	var tmp = make(map[string]string)
	var err = json.Unmarshal(bytes, &tmp)
	if err != nil {
		return err
	}
	if begin, ok := tmp["begin"]; ok {
		b, err := strconv.ParseUint(begin, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'begin': %w", err)
		}
		pr.BeginInclusive = uint32(b)
	}
	if end, ok := tmp["end"]; ok {
		b, err := strconv.ParseUint(end, 16, 32)
		if err != nil {
			return fmt.Errorf("parsing partition range 'end': %w", err)
		}
		pr.EndInclusive = uint32(b)
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
