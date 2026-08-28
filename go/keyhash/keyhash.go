// Package keyhash reproduces the packed-key hash that the Flow runtime uses to
// route documents to shards. Connectors which mirror that routing, for example
// to derive per-key message ordering identifiers, must compute the same hash
// bit for bit as the runtime.
//
// Reference implementations in the flow repo:
//   - Go: go/flow/mapping.go PackedKeyHash_HH64
//   - Rust: crates/doc/src/extractor.rs Extractor::packed_hash
package keyhash

import (
	"encoding/hex"

	"github.com/minio/highwayhash"
)

// PackedKeyHash_HH64 builds a packed key hash from the top 32-bits of a
// HighwayHash 64-bit checksum computed using a fixed key.
func PackedKeyHash_HH64(packedKey []byte) uint32 {
	return uint32(highwayhash.Sum64(packedKey, highwayHashKey) >> 32)
}

// highwayHashKey is a fixed 32 bytes (as required by HighwayHash) read from /dev/random.
var highwayHashKey, _ = hex.DecodeString("ba737e89155238d47d8067c35aad4d25ecdd1c3488227e011ffa480c022bd3ba")
