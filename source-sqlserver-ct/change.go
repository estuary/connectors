package main

import (
	"fmt"
	"strconv"

	"github.com/estuary/connectors/go/capture/sqlserver/change"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

// sqlserverCommitEventCT is a cursor checkpoint event containing per-table version numbers.
type sqlserverCommitEventCT struct {
	Versions  map[sqlcapture.StreamID]int64
	Deletions []sqlcapture.StreamID // Tables to remove from the cursor (emitted as null)
}

func (sqlserverCommitEventCT) IsDatabaseEvent() {}
func (sqlserverCommitEventCT) IsCommitEvent()   {}

func (evt *sqlserverCommitEventCT) String() string {
	if evt.Deletions != nil {
		return fmt.Sprintf("Commit(versions=%v, deletions=%v)", evt.Versions, evt.Deletions)
	}
	return fmt.Sprintf("Commit(versions=%v)", evt.Versions)
}

func (evt *sqlserverCommitEventCT) AppendJSON(buf []byte) ([]byte, error) {
	buf = append(buf, '{')
	var first = true
	for k, v := range evt.Versions {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = json.AppendEscape(buf, k.EncodeKey(), 0)
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, v, 10)
	}
	for _, k := range evt.Deletions {
		if !first {
			buf = append(buf, ',')
		}
		first = false
		buf = json.AppendEscape(buf, k.EncodeKey(), 0)
		buf = append(buf, ':')
		buf = append(buf, "null"...)
	}
	return append(buf, '}'), nil
}

// sqlserverChangeEvent is a type alias for the generic change event parameterized with CT metadata.
type sqlserverChangeEvent = change.Event[sqlserverChangeMetadataCT]

// sqlserverChangeMetadataCT holds metadata (the `_meta` field) for Change Tracking capture events.
type sqlserverChangeMetadataCT struct {
	Operation sqlcapture.ChangeOp   `json:"op"`
	Source    sqlserverSourceInfoCT `json:"source"`
}

// Op implements change.Metadata.
func (meta sqlserverChangeMetadataCT) Op() sqlcapture.ChangeOp {
	return meta.Operation
}

type sqlserverSourceInfoCT struct {
	sqlcapture.SourceCommon

	Version int64  `json:"version,omitempty" jsonschema:"description=The Change Tracking version at which this event occurred. Only set for replication events, not backfills."`
	Tag     string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (meta sqlserverChangeMetadataCT) AppendJSON(buf []byte) ([]byte, error) {
	return json.Append(buf, meta, 0)
}
