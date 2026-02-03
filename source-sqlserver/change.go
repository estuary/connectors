package main

import (
	"encoding/base64"
	"fmt"

	"github.com/estuary/connectors/go/capture/sqlserver/change"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

type sqlserverCommitEventCDC struct {
	CommitLSN LSN
}

func (sqlserverCommitEventCDC) IsDatabaseEvent() {}
func (sqlserverCommitEventCDC) IsCommitEvent()   {}

func (evt *sqlserverCommitEventCDC) String() string {
	return fmt.Sprintf("Commit(%X)", evt.CommitLSN)
}

func (evt *sqlserverCommitEventCDC) AppendJSON(buf []byte) ([]byte, error) {
	// Since base64 strings never require escaping we can just splat it between quotes.
	buf = append(buf, '"')
	buf = base64.StdEncoding.AppendEncode(buf, evt.CommitLSN)
	return append(buf, '"'), nil
}

// sqlserverChangeEvent is a type alias for the generic change event parameterized with CDC metadata.
type sqlserverChangeEvent = change.Event[sqlserverChangeMetadataCDC]

// sqlserverChangeMetadataCDC holds metadata (the `_meta` field) for CDC capture events.
type sqlserverChangeMetadataCDC struct {
	Operation sqlcapture.ChangeOp    `json:"op"`
	Source    sqlserverSourceInfoCDC `json:"source"`
}

// Op implements change.Metadata.
func (meta sqlserverChangeMetadataCDC) Op() sqlcapture.ChangeOp {
	return meta.Operation
}

type sqlserverSourceInfoCDC struct {
	sqlcapture.SourceCommon

	LSN        LSN    `json:"lsn" jsonschema:"description=The LSN at which a CDC event occurred. Only set for CDC events, not backfills."`
	SeqVal     []byte `json:"seqval" jsonschema:"description=Sequence value used to order changes to a row within a transaction. Only set for CDC events, not backfills."`
	UpdateMask any    `json:"updateMask,omitempty" jsonschema:"description=A bit mask with a bit corresponding to each captured column identified for the capture instance. Only set for CDC events, not backfills."`
	Tag        string `json:"tag,omitempty" jsonschema:"description=Optional 'Source Tag' property as defined in the endpoint configuration."`
}

// AppendJSON appends the JSON representation of the change metadata to the provided buffer.
func (meta sqlserverChangeMetadataCDC) AppendJSON(buf []byte) ([]byte, error) {
	return json.Append(buf, meta, 0)
}
