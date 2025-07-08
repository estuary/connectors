package main

import (
	"encoding/base64"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

type sqlserverCommitEvent struct {
	CommitLSN LSN
}

func (sqlserverCommitEvent) IsDatabaseEvent() {}
func (sqlserverCommitEvent) IsCommitEvent()   {}

func (evt *sqlserverCommitEvent) String() string {
	return fmt.Sprintf("Commit(%X)", evt.CommitLSN)
}

func (evt *sqlserverCommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	// Since base64 strings never require escaping we can just splat it between quotes.
	buf = append(buf, '"')
	buf = base64.StdEncoding.AppendEncode(buf, evt.CommitLSN)
	return append(buf, '"'), nil
}

type sqlserverChangeEvent struct {
	Operation sqlcapture.ChangeOp
	RowKey    []byte
	Source    sqlcapture.SourceMetadata
	Before    map[string]interface{}
	After     map[string]interface{}
}

func (sqlserverChangeEvent) IsDatabaseEvent() {}
func (sqlserverChangeEvent) IsChangeEvent()   {}

func (e *sqlserverChangeEvent) String() string {
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

func (e *sqlserverChangeEvent) StreamID() sqlcapture.StreamID {
	return e.Source.Common().StreamID()
}

func (e *sqlserverChangeEvent) GetRowKey() []byte {
	return e.RowKey
}

// MarshalJSON implements serialization of a ChangeEvent to JSON.
//
// Note that this implementation is destructive, but that's okay because
// emitting the serialized JSON is the last thing we ever do with a change.
func (e *sqlserverChangeEvent) AppendJSON(buf []byte) ([]byte, error) {
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
