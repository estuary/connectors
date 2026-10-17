package connector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// gateEvent is one step the scripted sidecar observed: an append or a commit wait
// it received, or a commit it reported, for a table.
type gateEvent struct {
	op    string // "append", "wait_commit", or "committed"
	table string
}

// gateSidecar is a sidecar scripted in-process, so a test can hold a table's
// commit for as long as it likes and observe what the write path does meanwhile.
// It reports a commit wait as committed at exactly the token it was asked for.
type gateSidecar struct {
	t    *testing.T
	conn net.Conn

	writeMu sync.Mutex

	mu sync.Mutex
	// tables maps each opened channel to its table.
	tables map[string]string
	// appended and committed are each channel's last appended and last reported
	// committed tokens.
	appended  map[string]string
	committed map[string]string
	// held are the commit waits not yet answered, by table.
	held map[string][]heldWait
	// released are the tables whose commit waits are answered as they arrive.
	released map[string]bool
	// autoCommit answers every commit wait after a short random delay.
	autoCommit bool

	events       []gateEvent
	maxUnflushed int
	changed      chan struct{}
}

type heldWait struct {
	id      uint64
	channel string
	token   string
}

// newGateManager builds a streaming v2 manager for an unsplit task, wired to a
// scripted sidecar instead of a process, with one delta-updates binding per table.
func newGateManager(t *testing.T, ctx context.Context, autoCommit bool, tables []string) (*streamV2Manager, *gateSidecar) {
	t.Helper()
	t.Setenv("FAKE_SIDECAR_STATE", "")

	// Every row is its own append, so the sidecar sees each one as Store writes it.
	var restoreRows = streamV2BatchRows
	t.Cleanup(func() { streamV2BatchRows = restoreRows })
	streamV2BatchRows = 1

	var cfg = &config{Credentials: &snowflake_auth.CredentialConfig{}}
	sm, _ := fakeStreamManager(t, "test/gate")
	var m = newStreamV2Manager(ctx, cfg, fakeSnowflakeDB(t), testDialect, sm, "test/gate", "acct",
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})

	conn, server := net.Pipe()
	var s = &gateSidecar{
		t:          t,
		conn:       server,
		tables:     make(map[string]string),
		appended:   make(map[string]string),
		committed:  make(map[string]string),
		held:       make(map[string][]heldWait),
		released:   make(map[string]bool),
		autoCommit: autoCommit,
		changed:    make(chan struct{}, 1),
	}
	m.client = newSidecarClient(conn, make(chan struct{}), func() error { return nil })
	t.Cleanup(func() { conn.Close(); server.Close() })
	t.Cleanup(m.stop)
	go s.serve()

	for i, table := range tables {
		m.addBinding("DB", "SCH", table, sql.Table{
			TableShape: sql.TableShape{Binding: i, DeltaUpdates: true},
			Identifier: table,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   table + ".v1",
		}, nil)
	}
	return m, s
}

func (s *gateSidecar) serve() {
	var reader = bufio.NewReader(s.conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var req struct {
			ID     uint64 `json:"id"`
			Op     string `json:"op"`
			Params struct {
				Table      string `json:"table"`
				Channel    string `json:"channel"`
				EndToken   string `json:"end_token"`
				Token      string `json:"token"`
				PayloadLen int    `json:"payload_len"`
			} `json:"params"`
		}
		if err := json.Unmarshal(line, &req); err != nil {
			return
		}

		switch req.Op {
		case "open_channel":
			s.mu.Lock()
			s.tables[req.Params.Channel] = req.Params.Table
			s.mu.Unlock()
			s.reply(req.ID, `{"committed_token":null,"rows_error_count":0,"last_error_message":""}`)
		case "append":
			if _, err := io.ReadFull(reader, make([]byte, req.Params.PayloadLen)); err != nil {
				return
			}
			s.mu.Lock()
			s.appended[req.Params.Channel] = req.Params.EndToken
			s.recordLocked("append", s.tables[req.Params.Channel])
			s.mu.Unlock()
			s.reply(req.ID, `{"appended":1}`)
		case "wait_commit":
			var w = heldWait{id: req.ID, channel: req.Params.Channel, token: req.Params.Token}
			s.mu.Lock()
			var table = s.tables[w.channel]
			s.recordLocked("wait_commit", table)
			var answer = s.released[table]
			if !answer && !s.autoCommit {
				s.held[table] = append(s.held[table], w)
			}
			s.mu.Unlock()

			if s.autoCommit {
				go func() {
					time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
					s.commit(w)
				}()
			} else if answer {
				s.commit(w)
			}
		default:
			s.reply(req.ID, "")
		}
	}
}

// release answers the table's held commit waits, and every later one as it arrives.
func (s *gateSidecar) release(table string) {
	s.mu.Lock()
	var waits = s.held[table]
	delete(s.held, table)
	s.released[table] = true
	s.mu.Unlock()

	for _, w := range waits {
		s.commit(w)
	}
}

// commit reports the wait's token as committed. The event is recorded before the
// reply is written, so it precedes anything the write path does in response.
func (s *gateSidecar) commit(w heldWait) {
	s.mu.Lock()
	s.committed[w.channel] = w.token
	s.recordLocked("committed", s.tables[w.channel])
	s.mu.Unlock()
	s.reply(w.id, fmt.Sprintf(`{"committed_token":%q,"rows_error_count":0,"last_error_message":""}`, w.token))
}

func (s *gateSidecar) reply(id uint64, result string) {
	var line = fmt.Sprintf(`{"id":%d,"ok":true}`, id)
	if result != "" {
		line = fmt.Sprintf(`{"id":%d,"ok":true,"result":%s}`, id, result)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, _ = s.conn.Write([]byte(line + "\n"))
}

// recordLocked logs an event and tracks how many tables hold appended rows that
// Snowflake has not reported committed.
func (s *gateSidecar) recordLocked(op, table string) {
	s.events = append(s.events, gateEvent{op: op, table: table})

	var unflushed = make(map[string]bool)
	for channel, token := range s.appended {
		if s.committed[channel] != token {
			unflushed[s.tables[channel]] = true
		}
	}
	s.maxUnflushed = max(s.maxUnflushed, len(unflushed))

	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *gateSidecar) count(op, table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	for _, e := range s.events {
		if e == (gateEvent{op, table}) {
			n++
		}
	}
	return n
}

// first reports the position of the first matching event, or -1.
func (s *gateSidecar) first(op, table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Index(s.events, gateEvent{op, table})
}

func (s *gateSidecar) peak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxUnflushed
}

// awaitEvents blocks until cond holds over the events observed so far.
func (s *gateSidecar) awaitEvents(cond func() bool) {
	s.t.Helper()
	var deadline = time.After(20 * time.Second)
	for !cond() {
		select {
		case <-s.changed:
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			s.mu.Lock()
			defer s.mu.Unlock()
			s.t.Fatalf("timed out awaiting sidecar events; observed %v", s.events)
		}
	}
}

// storeAll writes rowsPerTable rows to each binding in turn and then flushes, as
// one transaction's Store and StartCommit would, reporting the flush's outcome.
func storeAll(ctx context.Context, m *streamV2Manager, bindings []int, rowsPerTable int) <-chan error {
	var done = make(chan error, 1)
	go func() {
		for _, binding := range bindings {
			for row := range rowsPerTable {
				var key = fmt.Sprintf("b%d-r%d", binding, row)
				if err := testWriteRow(ctx, m, binding, []any{key, "v"}); err != nil {
					done <- err
					return
				}
			}
		}
		_, err := m.flush(ctx)
		done <- err
	}()
	return done
}

// TestStreamV2StoreGateSchedule pins the schedule of one table appending and one
// table flushing. Store moving from A to B flushes A while B appends, and C does
// not start until A's committed offset reaches the last row appended to it.
func TestStreamV2StoreGateSchedule(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)
	var m, s = newGateManager(t, ctx, false, []string{"A", "B", "C"})
	var done = storeAll(ctx, m, []int{0, 1, 2}, 2)

	// A flushes once Store reaches B, and B appends meanwhile.
	s.awaitEvents(func() bool {
		return s.count("append", "A") == 2 && s.count("wait_commit", "A") > 0 && s.count("append", "B") == 2
	})
	require.Zero(t, s.count("append", "C"), "C appended while A's flush held both slots")
	require.Zero(t, s.count("wait_commit", "B"), "B flushed while A held the only flush slot")

	// A's commit frees its flush slot, so B flushes and C appends.
	s.release("A")
	s.awaitEvents(func() bool {
		return s.count("wait_commit", "B") > 0 && s.count("append", "C") == 2
	})
	s.release("B")
	s.awaitEvents(func() bool { return s.count("wait_commit", "C") > 0 })
	s.release("C")
	require.NoError(t, <-done)

	var committedA = s.first("committed", "A")
	require.Less(t, s.first("append", "B"), committedA, "B did not append while A flushed")
	require.Greater(t, s.first("append", "C"), committedA)
	require.Greater(t, s.first("wait_commit", "B"), committedA)
	require.LessOrEqual(t, s.peak(), 2)
}

// TestStreamV2StoreGateBoundsUnflushedTables stores into many tables of four
// channels each, and checks that at most two tables hold uncommitted rows at once.
func TestStreamV2StoreGateBoundsUnflushedTables(t *testing.T) {
	var ctx = context.Background()
	var tables = []string{"T0", "T1", "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9"}
	var bindings = make([]int, len(tables))
	for i := range bindings {
		bindings[i] = i
	}
	var m, s = newGateManager(t, ctx, true, tables)

	require.NoError(t, <-storeAll(ctx, m, bindings, 40))
	require.LessOrEqual(t, s.peak(), 2)
	for _, table := range tables {
		require.Equal(t, 40, s.count("append", table))
	}
}

// TestStreamV2StoreGateReturningBinding covers a binding that Store returns to
// within one transaction. Its flush must complete before it appends again, so
// the commit it awaited is not overtaken by rows appended after it.
func TestStreamV2StoreGateReturningBinding(t *testing.T) {
	var ctx = context.Background()
	singleChannelLayout(t)
	var m, s = newGateManager(t, ctx, true, []string{"A", "B"})

	require.NoError(t, <-storeAll(ctx, m, []int{0, 1, 0}, 2))
	require.Equal(t, 4, s.count("append", "A"))

	// A's third append is the first of its return, and follows its first commit.
	s.mu.Lock()
	var events = slices.Clone(s.events)
	s.mu.Unlock()
	var appends, committedBeforeReturn = 0, false
	for _, e := range events {
		if e.table != "A" {
			continue
		} else if e.op == "committed" && appends == 2 {
			committedBeforeReturn = true
		} else if e.op == "append" {
			appends++
			if appends == 3 {
				require.True(t, committedBeforeReturn, "A appended again before its flush completed: %v", events)
			}
		}
	}
}
