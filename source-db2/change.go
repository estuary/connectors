package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"maps"
	"time"

	"github.com/estuary/connectors/sqlcapture"
)

type db2CommitEvent struct {
	CommitSeq []byte
}

func (db2CommitEvent) IsDatabaseEvent() {}
func (db2CommitEvent) IsCommitEvent()   {}

func (evt *db2CommitEvent) String() string { return fmt.Sprintf("Commit(%X)", evt.CommitSeq) }

func (evt *db2CommitEvent) AppendJSON(buf []byte) ([]byte, error) {
	buf = append(buf, '"')
	buf = hex.AppendEncode(buf, evt.CommitSeq)
	return append(buf, '"'), nil
}

// A CD row carries a transaction sequence (COMMITSEQ) and a row sequence
// (INTENTSEQ), plus optional before-images of source columns.
type db2ChangeRow struct {
	Table     *tableReplicationInfo
	CommitSeq []byte
	IntentSeq []byte
	Operation string
	LogMarker time.Time
	Fields    map[string]any
	Before    map[string]any
}

func (rs *db2ReplicationStream) changeEvent(row *db2ChangeRow, op sqlcapture.ChangeOp, fields map[string]any) (*sqlcapture.OldChangeEvent, error) {
	var key, err = rs.db.encodeRowKey(row.Table.KeyColumns, fields, row.Table.Discovery.Columns)
	if err != nil {
		return nil, err
	}
	fields = maps.Clone(fields)
	if err := rs.db.translateRecord(fields, row.Table.Discovery.Columns); err != nil {
		return nil, err
	}
	var source = db2SourceInfo{
		SourceCommon: sqlcapture.SourceCommon{Schema: row.Table.StreamID.Schema, Table: row.Table.StreamID.Table},
		Cursor:       hex.EncodeToString(row.CommitSeq),
		Intent:       hex.EncodeToString(row.IntentSeq),
		Tag:          rs.db.config.Advanced.SourceTag,
	}
	if !row.LogMarker.IsZero() {
		var marker, err = rs.db.translateDB2Value(row.LogMarker, db2ColumnType{Name: "TIMESTAMP"})
		if err != nil {
			return nil, err
		}
		var timestamp, parseErr = time.Parse(time.RFC3339Nano, marker.(string))
		if parseErr != nil {
			return nil, parseErr
		}
		source.Millis = timestamp.UnixMilli()
	}
	var event = &sqlcapture.OldChangeEvent{Operation: op, RowKey: key, Source: source}
	if op == sqlcapture.DeleteOp {
		event.Before = fields
	} else {
		event.After = fields
	}
	return event, nil
}

type updatePairer struct {
	stream  *db2ReplicationStream
	pending *db2ChangeRow
}

// pairUpdates consumes ordered CD rows. For UPDATE AS DELETE INSERT registrations,
// a D followed by an I in the same table and COMMITSEQ with the next INTENTSEQ is
// one update. A U uses its before-image columns. Either form emits delete + insert
// if the key changed; all other rows retain their operation. Updates use the new
// row's metadata; deletes use the old row's metadata. Nil flushes a pending delete
// at each transaction boundary, including the end of a poll. Inputs are not mutated.
func (p *updatePairer) pairUpdates(row *db2ChangeRow) ([]*sqlcapture.OldChangeEvent, error) {
	var events []*sqlcapture.OldChangeEvent
	var old = p.pending
	p.pending = nil
	var paired = old != nil && row != nil && old.Table == row.Table && row.Operation == "I" &&
		bytes.Equal(old.CommitSeq, row.CommitSeq) && consecutiveIntent(old.IntentSeq, row.IntentSeq)
	if old != nil && !paired {
		var event, err = p.stream.changeEvent(old, sqlcapture.DeleteOp, old.Fields)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if row == nil {
		return events, nil
	}
	var before map[string]any
	var op sqlcapture.ChangeOp
	switch row.Operation {
	case "I":
		op = sqlcapture.InsertOp
		if paired {
			before = old.Fields
		}
	case "D":
		if row.Table.Registration.UpdateAsDeleteInsert {
			p.pending = row
			return events, nil
		}
		op = sqlcapture.DeleteOp
	case "U":
		op = sqlcapture.UpdateOp
		// The connector-owned watermark key never changes and needs no before-image.
		if row.Table.StreamID != p.stream.db.WatermarksTable() && len(row.Table.KeyColumns) != 0 {
			old, before = row, row.Before
		}
	default:
		return nil, fmt.Errorf("unexpected IBMSNAP_OPERATION %q for %s", row.Operation, row.Table.StreamID)
	}
	var event, err = p.stream.changeEvent(row, op, row.Fields)
	if err != nil {
		return nil, err
	}
	if paired || before != nil {
		previous, err := p.stream.changeEvent(old, sqlcapture.DeleteOp, before)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(previous.RowKey, event.RowKey) {
			event.Operation, event.Before = sqlcapture.UpdateOp, previous.Before
		} else {
			events = append(events, previous)
			event.Operation = sqlcapture.InsertOp
		}
	}
	return append(events, event), nil
}

func consecutiveIntent(previous, next []byte) bool {
	if len(previous) == 0 || len(previous) != len(next) {
		return false
	}
	for i := len(previous) - 1; i >= 0; i-- {
		if previous[i] != 0xff {
			return next[i] == previous[i]+1 && bytes.Equal(previous[:i], next[:i])
		}
		if next[i] != 0 {
			return false
		}
	}
	return false
}
