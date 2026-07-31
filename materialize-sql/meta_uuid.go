package sql

import (
	"fmt"
	"time"

	"github.com/google/uuid"

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

// SpliceMeta appends a document UUID to a no_flow_document document, which the
// runtime reads by querying /_meta/uuid against the document to recover its
// clock.
//
// It operates on raw bytes and never parses the document, which matters because
// this runs once per loaded document. Exactly one source is reported by the load
// query, per the binding's materialized projections of /_meta/uuid:
//
//   - rawUUID is the document's own UUID, from a projection carrying the value
//     itself (see Table.MetaUUIDColumn). It is used verbatim.
//   - clockMicros is a date-time projection's value as Unix microseconds (see
//     Table.MetaUUIDClockColumn), which a UUID is synthesized from.
//
// A nil value for the binding's source means the row's column is NULL — for
// example a row written before the column existed — and gets the 1970-01-01
// sentinel, the earliest possible clock.
func SpliceMeta(doc []byte, rawUUID *string, clockMicros *int64) ([]byte, error) {
	if len(doc) < 2 || doc[0] != '{' || doc[len(doc)-1] != '}' {
		return nil, fmt.Errorf("reconstructed document is not a JSON object: %s", doc)
	}

	uuidVal := sentinelUUID
	if rawUUID != nil {
		// Verify rather than trust: a load query that reported a different
		// projection than the column describes would otherwise hand the runtime
		// a document clock it cannot parse.
		if _, err := uuid.Parse(*rawUUID); err != nil {
			return nil, fmt.Errorf("materialized document UUID is not a UUID: %q", *rawUUID)
		}
		uuidVal = *rawUUID
	} else if clockMicros != nil {
		uuidVal = synthesizeUUID(time.UnixMicro(*clockMicros).UTC())
	}

	// Grown by append rather than pre-sized from the input lengths, which
	// CodeQL flags as a possible allocation-size overflow.
	var out []byte
	out = append(out, doc[:len(doc)-1]...)
	if len(doc) > 2 {
		out = append(out, ',')
	}
	out = append(out, `"_meta":{"uuid":"`...)
	out = append(out, uuidVal...)
	out = append(out, `"}}`...)

	return out, nil
}
