package main

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

// cockroachdbCommitEvent represents a changefeed `resolved` checkpoint. Its cursor payload is the
// resolved HLC timestamp, which is what we persist in PersistentState.Cursor and resume from.
type cockroachdbCommitEvent struct {
	Resolved string // The resolved HLC timestamp, e.g. "1781626537709301005.0000000000".
}

func (cockroachdbCommitEvent) IsDatabaseEvent() {}
func (cockroachdbCommitEvent) IsCommitEvent()   {}

func (evt *cockroachdbCommitEvent) String() string {
	return fmt.Sprintf("Commit(%s)", evt.Resolved)
}

func (evt *cockroachdbCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	// The cursor is serialized as a JSON-quoted string.
	return json.AppendEscape(buf, evt.Resolved, 0), nil
}

// cockroachdbSource is the source metadata attached to every captured document.
type cockroachdbSource struct {
	sqlcapture.SourceCommon

	// Cursor is the CockroachDB HLC timestamp at which this change became visible (the
	// changefeed's `updated` timestamp), or the snapshot timestamp for backfilled rows — the
	// CockroachDB analogue of a PostgreSQL LSN, emitted as "loc" to mirror the other SQL CDC
	// connectors.
	Cursor string `json:"loc,omitempty" jsonschema:"description=The HLC timestamp at which this change occurred, or the snapshot timestamp for backfilled rows."`

	Tag string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

func (s *cockroachdbSource) Common() sqlcapture.SourceCommon { return s.SourceCommon }

// cockroachdbChangeEvent is the sqlcapture.ChangeEvent implementation for CockroachDB. Unlike
// PostgreSQL and MySQL, column values are never decoded: both the backfill (`to_jsonb(row)`) and
// the changefeed (`after` value) hand us byte-for-byte identical CockroachDB-native JSON, so
// `document` is spliced directly rather than unmarshalled, preserving INT8 values too large for
// a float64.
type cockroachdbChangeEvent struct {
	stream   sqlcapture.StreamID
	op       sqlcapture.ChangeOp
	source   cockroachdbSource
	document json.RawMessage // The row as a CockroachDB-native JSON object. For deletes, a key-only object.
	rowKey   []byte          // FDB-serialized row key, used as the backfill resume position.
}

func (cockroachdbChangeEvent) IsDatabaseEvent() {}
func (cockroachdbChangeEvent) IsChangeEvent()   {}

func (e *cockroachdbChangeEvent) String() string {
	switch e.op {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.stream)
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.stream)
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.stream)
	}
	return fmt.Sprintf("UnknownChange(%s)", e.stream)
}

func (e *cockroachdbChangeEvent) StreamID() sqlcapture.StreamID { return e.stream }

func (e *cockroachdbChangeEvent) GetRowKey() []byte { return e.rowKey }

// AppendJSON serializes the change event as the row document with an added `_meta` property. We
// splice the row document and hand-roll the metadata rather than round-tripping through
// json.Marshal, to avoid a per-event allocation on the hot streaming path.
func (e *cockroachdbChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var doc = bytes.TrimSpace(e.document)
	if len(doc) < 2 || doc[0] != '{' || doc[len(doc)-1] != '}' {
		return nil, fmt.Errorf("expected a JSON object document but got %q", e.document)
	}

	// Emit `{<row fields>,"_meta":{...}}`, splicing the metadata into the row document.
	buf = append(buf, '{')
	var interior = bytes.TrimSpace(doc[1 : len(doc)-1])
	if len(interior) > 0 {
		buf = append(buf, interior...)
		buf = append(buf, ',')
	}
	buf = append(buf, `"_meta":{"op":"`...)
	buf = append(buf, e.op...)
	buf = append(buf, `","source":`...)
	buf = e.source.appendJSON(buf)
	buf = append(buf, '}') // Close _meta.
	return append(buf, '}'), nil
}

// appendJSON hand-serializes the source metadata. The field order and omitempty semantics match
// what reflection-based marshalling of cockroachdbSource would produce.
func (s *cockroachdbSource) appendJSON(buf []byte) []byte {
	buf = append(buf, '{')
	if s.Millis != 0 {
		buf = append(buf, `"ts_ms":`...)
		buf = strconv.AppendInt(buf, s.Millis, 10)
		buf = append(buf, ',')
	}
	buf = append(buf, `"schema":`...)
	buf = json.AppendEscape(buf, s.Schema, 0)
	if s.Snapshot {
		buf = append(buf, `,"snapshot":true`...)
	}
	buf = append(buf, `,"table":`...)
	buf = json.AppendEscape(buf, s.Table, 0)
	if s.Cursor != "" {
		buf = append(buf, `,"loc":`...)
		buf = json.AppendEscape(buf, s.Cursor, 0)
	}
	if s.Tag != "" {
		buf = append(buf, `,"tag":`...)
		buf = json.AppendEscape(buf, s.Tag, 0)
	}
	return append(buf, '}')
}
