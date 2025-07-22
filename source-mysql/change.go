package main

import (
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
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
	Operation sqlcapture.ChangeOp
	RowKey    []byte
	Source    sqlcapture.SourceMetadata
	Before    map[string]any
	After     map[string]any
}

func (mysqlChangeEvent) IsDatabaseEvent() {}
func (mysqlChangeEvent) IsChangeEvent()   {}

func (e *mysqlChangeEvent) String() string {
	switch e.Operation {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Source.Common().StreamID())
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Source.Common().StreamID())
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Source.Common().StreamID())
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Source.Common().StreamID())
}

func (e *mysqlChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Source.Common().StreamID()
}

func (e *mysqlChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// MarshalJSON implements serialization of a ChangeEvent to JSON.
//
// Note that this implementation is destructive, but that's okay because
// emitting the serialized JSON is the last thing we ever do with a change.
func (e *mysqlChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
	var record map[string]any
	var meta = struct {
		Operation sqlcapture.ChangeOp       `json:"op"`
		Source    sqlcapture.SourceMetadata `json:"source"`
		Before    map[string]any            `json:"before,omitempty"`
	}{
		Operation: e.Operation,
		Source:    e.Source,
		Before:    nil,
	}
	switch e.Operation {
	case sqlcapture.InsertOp:
		record = e.After // Before is never used.
	case sqlcapture.UpdateOp:
		meta.Before, record = e.Before, e.After
	case sqlcapture.DeleteOp:
		record = e.Before // After is never used.
	}
	if record == nil {
		record = make(map[string]any)
	}
	record["_meta"] = &meta

	// Marshal to JSON and append to the provided buffer.
	return json.Append(buf, record, json.EscapeHTML|json.SortMapKeys)
}
