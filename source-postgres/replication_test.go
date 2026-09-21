package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecodeStrayStreamAbort verifies that a STREAM ABORT message is ignored
// rather than treated as a fatal decoding error. PostgreSQL 18 can send one
// for a subtransaction of a large, already rolled back transaction even when
// the client never enabled streaming.
func TestDecodeStrayStreamAbort(t *testing.T) {
	var s = &replicationStream{}

	// Byte1('A'), Int32 top-level XID, Int32 subtransaction XID.
	var msg = []byte{'A', 0x00, 0xa8, 0x73, 0x99, 0x00, 0xa8, 0x73, 0x9a}
	event, err := s.decodeMessage(0, msg)
	require.NoError(t, err)
	require.Nil(t, event)

	// A truncated message is still an error.
	_, err = s.decodeMessage(0, msg[:5])
	require.Error(t, err)

	// Other unknown message types are still rejected.
	_, err = s.decodeMessage(0, []byte{'S', 0x00, 0xa8, 0x73, 0x99})
	require.ErrorContains(t, err, "unsupported replication message type 'S'")
}
