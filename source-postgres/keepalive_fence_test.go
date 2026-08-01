package main

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

// The replication protocol framing needed to drive relayChanges directly. Only the message
// shapes these tests send are implemented; see the PostgreSQL protocol message formats and
// the pgoutput logical replication message formats for the layouts.

// copyData wraps a payload in a CopyData message: a type byte, a big-endian length which
// counts itself but not the type byte, and the payload.
func copyData(payload []byte) []byte {
	var msg = []byte{MessageTypeCopyData, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(msg[1:5], uint32(4+len(payload)))
	return append(msg, payload...)
}

func keepaliveMessage(serverWALEnd pglogrepl.LSN, replyRequested bool) []byte {
	var payload = make([]byte, 18)
	payload[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	binary.BigEndian.PutUint64(payload[1:9], uint64(serverWALEnd))
	binary.BigEndian.PutUint64(payload[9:17], 0) // ServerTime
	if replyRequested {
		payload[17] = 1
	}
	return copyData(payload)
}

// xlogData wraps pgoutput message bytes in an XLogData message.
func xlogData(walStart pglogrepl.LSN, walData []byte) []byte {
	var payload = make([]byte, 25, 25+len(walData))
	payload[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(payload[1:9], uint64(walStart))
	binary.BigEndian.PutUint64(payload[9:17], uint64(walStart)) // ServerWALEnd
	binary.BigEndian.PutUint64(payload[17:25], 0)               // ServerTime
	return copyData(append(payload, walData...))
}

func beginMessage(finalLSN pglogrepl.LSN) []byte {
	var msg = make([]byte, 21)
	msg[0] = byte(pglogrepl.MessageTypeBegin)
	binary.BigEndian.PutUint64(msg[1:9], uint64(finalLSN))
	binary.BigEndian.PutUint64(msg[9:17], 0)  // CommitTime
	binary.BigEndian.PutUint32(msg[17:21], 1) // Xid
	return msg
}

func commitMessage(commitLSN, txnEndLSN pglogrepl.LSN) []byte {
	var msg = make([]byte, 26)
	msg[0] = byte(pglogrepl.MessageTypeCommit)
	msg[1] = 0 // Flags
	binary.BigEndian.PutUint64(msg[2:10], uint64(commitLSN))
	binary.BigEndian.PutUint64(msg[10:18], uint64(txnEndLSN))
	binary.BigEndian.PutUint64(msg[18:26], 0) // CommitTime
	return msg
}

// newFenceTestStream returns a replicationStream reading from an in-memory connection, plus
// the server end of that connection to write messages into.
//
// The stream is only equipped for the keepalive, BEGIN and COMMIT messages these tests send.
// Change events would need typeMap, the tables maps and db, none of which are set here.
func newFenceTestStream(t *testing.T) (*replicationStream, net.Conn) {
	t.Helper()
	var serverConn, clientConn = net.Pipe()
	t.Cleanup(func() { serverConn.Close(); clientConn.Close() })

	var stream = &replicationStream{
		conn:  &pgconn.HijackedConn{Conn: clientConn},
		rxbuf: make([]byte, 0, 4096),
	}
	stream.reused.commitEvent = new(postgresCommitEvent)
	return stream, serverConn
}

// writeScript sends each message in order. The connection is deliberately left open: closing
// either end of a net.Pipe makes SetReadDeadline fail with ErrClosedPipe rather than letting
// the relay see a clean EOF, so tests which expect the relay to keep running assert on their
// context deadline instead.
func writeScript(t *testing.T, conn net.Conn, script ...[]byte) {
	t.Helper()
	go func() {
		for _, msg := range script {
			if _, err := conn.Write(msg); err != nil {
				return // The relay stopped reading; the assertions will catch it.
			}
		}
	}()
}

// drain consumes anything the connector writes back. Without it a standby status update,
// which is written with no deadline, would block forever on the unbuffered pipe.
func drain(conn net.Conn) {
	go func() { io.Copy(io.Discard, conn) }()
}

// fenceTestContext bounds a relay so that a regression fails the test rather than hanging the
// package. No test should ever reach this deadline.
func fenceTestContext(t *testing.T) context.Context {
	t.Helper()
	var ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// unknownMessage is an unrecognised CopyData message, which relayChanges rejects outright. Ending
// a script with it proves the messages before it were consumed: a test which expects the relay to
// keep running can assert on this error rather than on a timeout, which would also pass if nothing
// were ever delivered.
func unknownMessage() []byte { return copyData([]byte{'Z'}) }

// TestKeepaliveFenceGuard drives relayChanges over an in-memory connection to check which
// keepalives are allowed to satisfy a fence.
//
// A keepalive proves the server has decoded past the position it reports, but says nothing
// about whether we are between transactions, and the server does deliver keepalives partway
// through a transaction's output. Completing a fence there would checkpoint a cursor inside
// a transaction. This asserts that the mid-transaction keepalive is ignored: the fence is
// satisfied only by the one which follows the commit, and the commit event reaches the
// callback first. Drop the nextTxnFinalLSN term from the guard and no commit is ever seen.
//
// The satisfying keepalive reports exactly the fence position, which is the case that occurs
// in production: the fence comes from pg_current_wal_flush_lsn() and on a quiet server the
// server's decoding position converges to precisely that. Weaken the comparison to a strict
// greater-than and this fence is never reached.
func TestKeepaliveFenceGuard(t *testing.T) {
	const fenceLSN = pglogrepl.LSN(0x1000)
	const txnCommitLSN = pglogrepl.LSN(0x2000)
	const txnEndLSN = txnCommitLSN + 0x40 // Distinct, so the cursor can't accidentally match.

	var stream, serverConn = newFenceTestStream(t)
	drain(serverConn)
	writeScript(t, serverConn,
		keepaliveMessage(fenceLSN-1, false),                            // below the fence
		xlogData(txnCommitLSN, beginMessage(txnCommitLSN)),             // transaction opens
		keepaliveMessage(fenceLSN+1, false),                            // past the fence, mid-transaction
		xlogData(txnCommitLSN, commitMessage(txnCommitLSN, txnEndLSN)), // transaction closes
		keepaliveMessage(fenceLSN, false),                              // exactly at the fence
	)

	var events []postgresCommitEvent
	var err = stream.relayChanges(fenceTestContext(t), fenceLSN, func(event sqlcapture.DatabaseEvent) error {
		// Commit events are a reused object, so record a copy rather than the pointer.
		if commit, ok := event.(*postgresCommitEvent); ok {
			events = append(events, *commit)
		}
		return nil
	})

	require.ErrorIs(t, err, errFenceReachedByKeepalive)
	require.Len(t, events, 1, "the fence should have been satisfied by the keepalive after the commit, not the one during the transaction")
	require.Equal(t, txnEndLSN, events[0].CommitLSN, "the cursor should be the transaction's end LSN")
}

// TestKeepaliveRepliesBeforeSatisfyingFence checks that a reply-requested keepalive which also
// satisfies the fence is answered before the relay stops. Reordering the guard above the reply
// handling would leave the server waiting on a reply we never sent.
func TestKeepaliveRepliesBeforeSatisfyingFence(t *testing.T) {
	const fenceLSN = pglogrepl.LSN(0x1000)

	var stream, serverConn = newFenceTestStream(t)
	var replies = make(chan []byte, 4)
	go func() {
		var buf = make([]byte, 256)
		for {
			var n, err = serverConn.Read(buf)
			if n > 0 {
				replies <- append([]byte(nil), buf[:n]...)
			}
			if err != nil {
				return
			}
		}
	}()
	writeScript(t, serverConn, keepaliveMessage(fenceLSN, true))

	var err = stream.relayChanges(fenceTestContext(t), fenceLSN, func(event sqlcapture.DatabaseEvent) error {
		return nil
	})
	require.ErrorIs(t, err, errFenceReachedByKeepalive)

	select {
	case reply := <-replies:
		require.GreaterOrEqual(t, len(reply), 6, "short read of the standby status update")
		require.Equal(t, byte(MessageTypeCopyData), reply[0])
		require.Equal(t, byte(pglogrepl.StandbyStatusUpdateByteID), reply[5], "expected a Standby Status Update")
	case <-time.After(5 * time.Second):
		t.Fatal("the reply-requested keepalive was not answered before the fence completed")
	}
}

// TestKeepaliveBelowFenceDoesNotSatisfyIt checks that a keepalive short of the fence leaves the
// relay running rather than completing the fence.
func TestKeepaliveBelowFenceDoesNotSatisfyIt(t *testing.T) {
	const fenceLSN = pglogrepl.LSN(0x1000)

	var stream, serverConn = newFenceTestStream(t)
	drain(serverConn)
	writeScript(t, serverConn,
		keepaliveMessage(fenceLSN-1, false),
		keepaliveMessage(fenceLSN-0x800, false),
		unknownMessage(),
	)

	var err = stream.relayChanges(fenceTestContext(t), fenceLSN, func(event sqlcapture.DatabaseEvent) error {
		return nil
	})
	require.ErrorContains(t, err, "unknown CopyData message", "the relay should have consumed both keepalives and kept running")
	require.NotErrorIs(t, err, errFenceReachedByKeepalive)
}

// TestZeroFenceIgnoresKeepalives covers the timed streaming phase, which passes a zero fence
// because it has no fence to reach yet and ends on its own deadline. Dropping the fenceLSN > 0
// term from the guard would turn every keepalive into a fence completion there, which the timed
// phase's error handling does not expect.
func TestZeroFenceIgnoresKeepalives(t *testing.T) {
	var stream, serverConn = newFenceTestStream(t)
	drain(serverConn)
	writeScript(t, serverConn,
		keepaliveMessage(0x4000, false),
		keepaliveMessage(0x8000, false),
		unknownMessage(),
	)

	var err = stream.relayChanges(fenceTestContext(t), 0, func(event sqlcapture.DatabaseEvent) error {
		return nil
	})
	require.ErrorContains(t, err, "unknown CopyData message", "the relay should have consumed both keepalives and kept running")
	require.NotErrorIs(t, err, errFenceReachedByKeepalive, "a zero fence must never be satisfied by a keepalive")
}
