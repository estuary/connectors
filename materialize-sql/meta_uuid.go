package sql

import (
	"encoding/json"
	"fmt"
	"time"

	"go.gazette.dev/core/message"
)

// zeroProducer is the ProducerID used for synthesized document UUIDs. Only the
// clock encoded in the UUID is meaningful to the runtime (it is compared
// against a collection's backfill markers for truncations), so a fixed all-zero
// producer is sufficient and keeps synthesis deterministic.
var zeroProducer message.ProducerID

// sentinelUUID is the synthesized document UUID used when a row's /_meta/uuid
// source column is NULL — for example rows that predate the column being added.
// It encodes the Unix epoch (1970-01-01T00:00:00Z), the earliest possible
// clock.
var sentinelUUID = synthesizeUUID(time.Unix(0, 0).UTC())

// synthesizeUUID builds a Gazette v1 document UUID whose clock corresponds to
// t, so that the runtime can extract an equivalent document clock from it.
func synthesizeUUID(t time.Time) string {
	return message.BuildUUID(zeroProducer, message.NewClock(t), message.Flag_OUTSIDE_TXN).String()
}

// SynthesizeMetaUUID ensures a reconstructed no_flow_document document carries a
// valid Gazette document UUID at /_meta/uuid, from which the runtime extracts
// the document clock.
//
// col is the materialized projection of /_meta/uuid whose value the load query
// nested at _meta.uuid (see Table.MetaUUIDColumn):
//   - When col is a date-time projection (e.g. flow_published_at), the nested
//     value is a timestamp and a UUID is synthesized from its clock.
//   - When col is the raw document UUID, the value is already a UUID and is
//     preserved unchanged.
//   - A NULL or missing value is synthesized from the 1970-01-01 sentinel.
func SynthesizeMetaUUID(doc json.RawMessage, col *Column) (json.RawMessage, error) {
	timestampMode := col.Inference.String_ != nil && col.Inference.String_.Format == "date-time"

	var top map[string]json.RawMessage
	if err := json.Unmarshal(doc, &top); err != nil {
		return nil, fmt.Errorf("unmarshalling document for _meta/uuid synthesis: %w", err)
	}

	var meta map[string]json.RawMessage
	if raw, ok := top["_meta"]; ok && string(raw) != "null" {
		if err := json.Unmarshal(raw, &meta); err != nil {
			return nil, fmt.Errorf("unmarshalling _meta object for uuid synthesis: %w", err)
		}
	} else {
		meta = make(map[string]json.RawMessage, 1)
	}

	if raw, ok := meta["uuid"]; !ok || string(raw) == "null" {
		// Missing/NULL source value in either mode: use the epoch sentinel.
	} else if timestampMode {
		var ts string
		if err := json.Unmarshal(raw, &ts); err != nil {
			return nil, fmt.Errorf("unmarshalling _meta/uuid timestamp: %w", err)
		}
		t, err := parseFlowTimestamp(ts)
		if err != nil {
			return nil, err
		}
		newUUID, err := json.Marshal(synthesizeUUID(t))
		if err != nil {
			return nil, err
		}
		meta["uuid"] = newUUID
		return remarshal(top, meta)
	} else {
		// Raw UUID already present; leave the document untouched.
		return doc, nil
	}

	newUUID, err := json.Marshal(sentinelUUID)
	if err != nil {
		return nil, err
	}
	meta["uuid"] = newUUID
	return remarshal(top, meta)
}

func remarshal(top, meta map[string]json.RawMessage) (json.RawMessage, error) {
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshalling _meta after uuid synthesis: %w", err)
	}
	top["_meta"] = metaJSON

	out, err := json.Marshal(top)
	if err != nil {
		return nil, fmt.Errorf("marshalling document after uuid synthesis: %w", err)
	}
	return out, nil
}

// parseFlowTimestamp parses an RFC3339 date-time string as reconstructed by the
// dialects' load queries for a /_meta/uuid date-time projection. Flow date-time
// values are RFC3339, but the sub-second precision and zone rendering vary by
// dialect, so a few tolerant layouts are attempted.
func parseFlowTimestamp(ts string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z0700",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999-07:00",
	} {
		if t, err := time.Parse(layout, ts); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("parsing document UUID timestamp %q as RFC3339", ts)
}
