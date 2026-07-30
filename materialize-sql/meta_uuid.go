package sql

import (
	"fmt"
	"time"

	"go.gazette.dev/core/message"
)

const (
	// metaUUIDPtr is the JSON pointer of the document UUID, from which the
	// runtime extracts a document's clock.
	metaUUIDPtr = "/_meta/uuid"
	// metaUUIDField is the canonical field name of the raw document UUID
	// projection, which carries the UUID itself rather than a view of it.
	metaUUIDField = "_meta/uuid"
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

// SpliceMeta appends the reconstructed _meta object to a no_flow_document
// document, inserting the document UUID that the runtime extracts a document
// clock from.
//
// It operates on raw bytes and never parses the document, which matters because
// this runs once per loaded document:
//
//   - doc is the reconstructed document without _meta, as built by the load
//     query from the binding's root-level columns.
//   - metaJSON is the JSON object of the binding's materialized /_meta/<name>
//     values (see Table.MetaColumns), or nil when it has none. It already
//     carries "uuid" when the raw _meta/uuid field is materialized.
//   - hasClock reports whether the binding has a date-time projection of
//     /_meta/uuid (see Table.MetaUUIDClockColumn), whose value arrives as
//     clockMicros. A nil clockMicros with hasClock means the row's column is
//     NULL — for example a row written before the column existed — and gets the
//     1970-01-01 sentinel, the earliest possible clock.
//
// When the binding has neither a raw UUID nor a clock, no uuid is inserted: such
// a materialization cannot support truncations, which is reported if a backfill
// is ever applied to it.
func SpliceMeta(doc, metaJSON []byte, clockMicros *int64, hasClock bool) ([]byte, error) {
	if len(doc) < 2 || doc[0] != '{' || doc[len(doc)-1] != '}' {
		return nil, fmt.Errorf("reconstructed document is not a JSON object: %s", doc)
	}

	var uuidVal string
	if hasClock && clockMicros != nil {
		uuidVal = synthesizeUUID(time.UnixMicro(*clockMicros).UTC())
	} else if hasClock {
		uuidVal = sentinelUUID
	}

	// Build the _meta object: the uuid (when synthesized) followed by the
	// materialized /_meta/* values.
	var meta []byte
	switch {
	case uuidVal == "" && len(metaJSON) == 0:
		return doc, nil // Nothing to splice.
	case uuidVal == "":
		meta = metaJSON
	case len(metaJSON) == 0:
		meta = append(meta, `{"uuid":"`...)
		meta = append(meta, uuidVal...)
		meta = append(meta, `"}`...)
	default:
		if len(metaJSON) < 2 || metaJSON[0] != '{' || metaJSON[len(metaJSON)-1] != '}' {
			return nil, fmt.Errorf("reconstructed _meta is not a JSON object: %s", metaJSON)
		}
		meta = append(meta, `{"uuid":"`...)
		meta = append(meta, uuidVal...)
		meta = append(meta, '"')
		if len(metaJSON) > 2 {
			meta = append(meta, ',')
			meta = append(meta, metaJSON[1:len(metaJSON)-1]...)
		}
		meta = append(meta, '}')
	}

	// Grown by append rather than pre-sized from the input lengths, which
	// CodeQL flags as a possible allocation-size overflow.
	var out []byte
	out = append(out, doc[:len(doc)-1]...)
	if len(doc) > 2 {
		out = append(out, ',')
	}
	out = append(out, `"_meta":`...)
	out = append(out, meta...)
	out = append(out, '}')

	return out, nil
}
