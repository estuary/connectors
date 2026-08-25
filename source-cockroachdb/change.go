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

	// Cursor is the CockroachDB HLC (hybrid logical clock) MVCC timestamp at which this change
	// became visible (the changefeed's `updated` timestamp), or the snapshot timestamp for
	// backfilled rows. It is the CockroachDB analogue of a PostgreSQL LSN: a monotonic position
	// in the change history. It's emitted under the property name "loc" to mirror the other SQL
	// CDC connectors, and is the basis of the fallback collection key for keyless tables.
	Cursor string `json:"loc,omitempty" jsonschema:"description=The HLC timestamp at which this change occurred, or the snapshot timestamp for backfilled rows."`

	Tag string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

func (s *cockroachdbSource) Common() sqlcapture.SourceCommon { return s.SourceCommon }

// cockroachdbChangeEvent is the sqlcapture.ChangeEvent implementation for CockroachDB.
//
// Unlike the PostgreSQL and MySQL connectors we don't decode individual column values: both the
// backfill (`to_jsonb(row)`) and the changefeed (`format=json` `after` value) hand us the row
// already encoded as CockroachDB-native JSON, and those two encodings are byte-for-byte identical.
// So `Document` holds the row as a raw JSON object and we splice in the `_meta` property when
// serializing. Splicing (rather than unmarshalling into a map) preserves exact numeric values,
// which matters for INT8 columns whose magnitude exceeds what a float64 can represent.
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

// AppendJSON serializes the change event as the row document with an added `_meta` property,
// appending directly to the caller's (reused) buffer. We hand-roll the metadata and splice the row
// document rather than round-tripping through a map or json.Marshal, both to preserve exact numeric
// values and to avoid a per-event allocation on the hot streaming path.
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
