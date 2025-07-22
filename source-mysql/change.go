package main

import (
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/invopop/jsonschema"
	"github.com/segmentio/encoding/json"
)

type mysqlCommitEvent struct {
	Position mysql.Position
}

func (mysqlCommitEvent) IsDatabaseEvent() {}
func (mysqlCommitEvent) IsCommitEvent()   {}

func (evt *mysqlCommitEvent) String() string {
	return fmt.Sprintf("Commit(%s:%d)", evt.Position.Name, evt.Position.Pos)
}

func (evt *mysqlCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	var cursorString = fmt.Sprintf("%s:%d", evt.Position.Name, evt.Position.Pos)
	return json.AppendEscape(buf, cursorString, 0), nil
}

type mysqlChangeEvent struct {
	Info *mysqlChangeSharedInfo // Information about the source table structure which is shared across events.

	Meta   mysqlChangeMetadata // Document metadata object representing the _meta property
	RowKey []byte
	Values map[string]any
}

type mysqlChangeSharedInfo struct {
	StreamID sqlcapture.StreamID // StreamID of the table this change event came from.
}

type mysqlChangeMetadata struct {
	Info *mysqlChangeSharedInfo `json:"-"` // Information about the source table structure which is shared across events. Copied here to simplify before-value serialization.

	Operation sqlcapture.ChangeOp `json:"op"`
	Source    mysqlSourceInfo     `json:"source"`
	Before    map[string]any      `json:"before,omitempty"`
}

// mysqlSourceInfo is source metadata for data capture events.
type mysqlSourceInfo struct {
	sqlcapture.SourceCommon
	Cursor mysqlChangeEventCursor `json:"cursor" jsonschema:"description=Cursor value representing the current position in the binlog."`
	TxID   string                 `json:"txid,omitempty" jsonschema:"description=The global transaction identifier associated with a change by MySQL. Only set if GTIDs are enabled."`
}

type mysqlChangeEventCursor struct {
	BinlogFile   string // Cursor representing the binlog file containing this event
	BinlogOffset uint64 // Cursor representing the estimated binlog offset of the rows change event
	RowIndex     int    // Index of the current row within the binlog event
}

func (c mysqlChangeEventCursor) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Type: "string"}
}

func (c mysqlChangeEventCursor) MarshalJSON() ([]byte, error) {
	if c.BinlogFile == "backfill" && c.BinlogOffset == 0 {
		return json.Marshal(fmt.Sprintf("%s:%d", c.BinlogFile, c.RowIndex))
	}
	return json.Marshal(fmt.Sprintf("%s:%d:%d", c.BinlogFile, c.BinlogOffset, c.RowIndex))
}

func (s *mysqlSourceInfo) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

func (mysqlChangeEvent) IsDatabaseEvent() {}
func (mysqlChangeEvent) IsChangeEvent()   {}

func (e *mysqlChangeEvent) String() string {
	switch e.Meta.Operation {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Info.StreamID)
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Info.StreamID)
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Info.StreamID)
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Info.StreamID)
}

func (e *mysqlChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Info.StreamID
}

func (e *mysqlChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// MarshalJSON implements serialization of a ChangeEvent to JSON.
//
// Note that this implementation is destructive, but that's okay because
// emitting the serialized JSON is the last thing we ever do with a change.
func (e *mysqlChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var record = e.Values
	record["_meta"] = &e.Meta
	return json.Append(buf, record, json.EscapeHTML|json.SortMapKeys)
}
