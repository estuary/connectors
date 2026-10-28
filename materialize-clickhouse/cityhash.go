package connector

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/go-faster/city"
)

// cityHash64 computes cityHash64(args...) as ClickHouse does for the column
// types this connector materializes keys into: Int64, Int128, Int256, Bool,
// Float64, String, Date32 and DateTime64(6, 'UTC').
func cityHash64(args ...any) uint64 {
	var one = func(a any) uint64 {
		switch v := a.(type) {
		case int64:
			return intHash64(uint64(v))
		case int128:
			// Wide integers are not plain PODs to ClickHouse, so they hash
			// their little-endian bytes like a string would.
			return city.CH64(twosComplementLE(v.Int, 16))
		case int256:
			return city.CH64(twosComplementLE(v.Int, 32))
		case date32:
			// Date32 is an Int32 day count, zero-extended to 64 bits.
			var days = int32(v.Unix() / 86400)
			return intHash64(uint64(uint32(days)))
		case bool:
			if v {
				return intHash64(1)
			}
			return intHash64(0)
		case float64:
			return intHash64(math.Float64bits(v))
		case string:
			return city.CH64([]byte(v))
		case time.Time:
			// DateTime64 is a decimal type, hashed as the little-endian bytes
			// of its Int64 tick count rather than through intHash64.
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(v.UnixMicro()))
			return city.CH64(buf[:])
		default:
			panic(fmt.Sprintf("unsupported key type %T", a))
		}
	}
	var h = one(args[0])
	for _, a := range args[1:] {
		h = hash128to64(h, one(a))
	}
	return h
}

// intHash64 is ClickHouse's IntHash64Impl, a salted intHash64, which
// cityHash64 applies to every plain integer, float, Bool, Date and DateTime
// argument in place of hashing the value's bytes. Narrow types are
// zero-extended to 64 bits first.
func intHash64(v uint64) uint64 {
	v ^= 0x4CF2D2BAAE6DA887
	v ^= v >> 33
	v *= 0xff51afd7ed558ccd
	v ^= v >> 33
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> 33
	return v
}

// hash128to64 is CityHash's Hash128to64, which cityHash64 uses to fold each
// further argument's hash into the running hash.
func hash128to64(lo, hi uint64) uint64 {
	const mul = uint64(0x9ddfea08eb382d69)
	var a = (lo ^ hi) * mul
	a ^= a >> 47
	var b = (hi ^ a) * mul
	b ^= b >> 47
	b *= mul
	return b
}

// int128, int256 and date32 mark a value with the column type it is stored
// in, because the hash depends on the column's width and not on the Go value
// alone.
type int128 struct{ *big.Int }
type int256 struct{ *big.Int }
type date32 struct{ time.Time }

// twosComplementLE renders v as a little-endian two's complement integer of
// the given byte width.
func twosComplementLE(v *big.Int, width int) []byte {
	var n = new(big.Int).Set(v)
	if n.Sign() < 0 {
		n.Add(n, new(big.Int).Lsh(big.NewInt(1), uint(width*8)))
	}
	var out = make([]byte, width)
	var be = n.Bytes()
	for i, b := range be {
		out[len(be)-1-i] = b
	}
	return out
}

// shardFor walks cumulative shard weights the way a Distributed table does:
// the hash modulo the total weight selects a slot, and slots are dealt to
// shards in order, `weight` apiece. Returns the 1-based shard number.
func shardFor(hash uint64, weights []uint32) int {
	var total uint64
	for _, w := range weights {
		total += uint64(w)
	}
	if total == 0 {
		panic("every shard has weight 0")
	}
	var slot = hash % total
	for i, w := range weights {
		if slot < uint64(w) {
			return i + 1
		}
		slot -= uint64(w)
	}
	panic("unreachable")
}
