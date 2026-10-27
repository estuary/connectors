package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestIgnoreQueries(t *testing.T) {
	var cases = map[string]bool{
		`BEGIN`:                     true,
		`COMMIT`:                    true,
		`CREATE DEFINER`:            true,
		`CREATE OR REPLACE DEFINER`: true,
		`CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER`: true,
		`CREATE ALGORITHM = TEMPTABLE DEFINER`:          true,

		`CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234'`: true,

		"# This is a comment\n ALTER TABLE foobar ADD COLUMN x INTEGER;":        false,
		`/* This is also a comment */ ALTER TABLE foobar ADD COLUMN x INTEGER;`: false,

		`CREATE DATABASE IF NOT EXISTS test`:     false,
		`INSERT INTO foobar VALUES (1, 'hello')`: false,
		`DROP TABLE foobar`:                      false,
	}
	for input, expect := range cases {
		if ignoreQueriesRe.MatchString(input) != expect {
			t.Errorf("ignore result mismath for %q (expected %v)", input, expect)
		}
	}
}

// TestImplicitFlushWithinTransaction replays a binlog event sequence through the
// replication loop and checks which positions it reports as commits. The
// sequence includes the artificial rotate event which begins a re-sync after a
// dropped connection, placed inside a transaction, where no commit may be
// reported.
func TestImplicitFlushWithinTransaction(t *testing.T) {
	var streamer = replication.NewBinlogStreamer()
	var rs = &mysqlReplicationStream{
		streamer: streamer,
		events:   make(chan sqlcapture.DatabaseEvent, 1024),
	}
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	var runErr = make(chan error, 1)
	go func() { runErr <- rs.run(ctx, mysql.Position{Name: "binlog.000001", Pos: 100}) }()

	var feed = func(header replication.EventHeader, event replication.Event) {
		require.NoError(t, streamer.AddEventToStreamer(&replication.BinlogEvent{Header: &header, Event: event}))
	}
	var at = func(logPos uint32) replication.EventHeader {
		return replication.EventHeader{LogPos: logPos}
	}
	var artificial = replication.EventHeader{Flags: replication.LOG_EVENT_ARTIFICIAL_F}
	var heartbeat = replication.EventHeader{EventType: replication.HEARTBEAT_EVENT, Flags: replication.LOG_EVENT_ARTIFICIAL_F}

	// The table is not active, so its row events are reported as keepalives
	// and the transaction guard is tested without any captured changes.
	var table = &replication.TableMapEvent{TableID: 985, Schema: []byte("test"), Table: []byte("foo")}
	var rows = &replication.RowsEvent{Table: table, TableID: 985}
	var begin = &replication.QueryEvent{Query: []byte("BEGIN")}
	var commit = &replication.QueryEvent{Query: []byte("COMMIT")}

	// An uninterrupted transaction commits at its XID event, and the heartbeat
	// which follows while the server is idle re-reports that position.
	feed(at(150), begin)
	feed(at(200), table)
	feed(at(250), rows)
	feed(at(300), &replication.XIDEvent{})
	feed(heartbeat, &replication.GenericEvent{})

	// The connection drops between a table map event and its row event. The
	// re-sync begins at the position after the table map, and the server opens
	// the new dump with an artificial rotate and a format description event.
	feed(at(350), begin)
	feed(at(400), table)
	feed(artificial, &replication.RotateEvent{NextLogName: []byte("binlog.000001"), Position: 400})
	feed(at(0), &replication.FormatDescriptionEvent{})
	feed(at(450), rows)
	feed(at(500), &replication.XIDEvent{})
	feed(heartbeat, &replication.GenericEvent{})

	// On MariaDB with GTIDs enabled there is no BEGIN query event, so the
	// table map event opens the transaction. A transaction on non-transactional
	// tables ends with a COMMIT query event.
	feed(at(550), table)
	feed(artificial, &replication.RotateEvent{NextLogName: []byte("binlog.000001"), Position: 550})
	feed(at(600), rows)
	feed(at(650), commit)
	feed(heartbeat, &replication.GenericEvent{})

	// A genuine rotate ends any transaction which appears to be open.
	feed(at(700), begin)
	feed(at(750), table)
	feed(at(800), &replication.RotateEvent{NextLogName: []byte("binlog.000002"), Position: 4})
	feed(heartbeat, &replication.GenericEvent{})

	var expected = []string{
		"KeepaliveEvent",
		"Commit(binlog.000001:300)",
		"Commit(binlog.000001:300)",
		"KeepaliveEvent",
		"Commit(binlog.000001:500)",
		"Commit(binlog.000001:500)",
		"KeepaliveEvent",
		"Commit(binlog.000001:650)",
		"Commit(binlog.000001:650)",
		"Commit(binlog.000002:4)",
		"Commit(binlog.000002:4)",
	}
	var actual []string
	for len(actual) < len(expected) {
		select {
		case event := <-rs.events:
			actual = append(actual, fmt.Sprint(event))
		case err := <-runErr:
			t.Fatalf("replication loop exited after %d events: %v", len(actual), err)
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out after %d events: %v", len(actual), actual)
		}
	}
	require.Equal(t, expected, actual)
}
