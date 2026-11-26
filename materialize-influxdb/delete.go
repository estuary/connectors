package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

type deleteRequest struct {
	Start     string `json:"start"`
	Stop      string `json:"stop"`
	Predicate string `json:"predicate"`
}

// errEmptyKeyTag marks a delete whose key tag is empty. Its points were written
// without that tag, so no predicate can match them without also matching others.
var errEmptyKeyTag = errors.New("empty key tag")

func (b *measurementBinding) isDelete(it *m.StoreIterator) bool {
	if it.Delete {
		return true
	}
	// Delta updates may carry a source deletion without the protocol delete flag.
	// Read the selected operation when Flow omits the root document.
	for i, field := range b.fields.Keys {
		if field == b.operationField && i < len(it.Key) && isDeleteOperation(it.Key[i]) {
			return true
		}
	}
	for i, field := range b.fields.Values {
		if field == b.operationField && i < len(it.Values) && isDeleteOperation(it.Values[i]) {
			return true
		}
	}
	// Maps match metadata keys exactly; struct decoding also accepts different casing.
	var doc map[string]json.RawMessage
	if len(it.RawJSON) == 0 || json.Unmarshal(it.RawJSON, &doc) != nil {
		return false
	}
	var meta map[string]json.RawMessage
	if json.Unmarshal(doc["_meta"], &meta) != nil {
		return false
	}
	var op string
	return json.Unmarshal(meta["op"], &op) == nil && op == "d"
}

func isDeleteOperation(value any) bool {
	switch v := value.(type) {
	case string:
		return v == "d"
	case []byte:
		var op string
		return json.Unmarshal(v, &op) == nil && op == "d"
	default:
		return false
	}
}

func (b *measurementBinding) deleteRequest(key, values tuple.Tuple) (deleteRequest, error) {
	request := deleteRequest{
		Start: "1677-09-21T00:12:43.145224194Z",
		Stop:  "2262-04-11T23:47:16.854775806Z",
	}
	selected, err := b.selectedFields(key, values, nil)
	if err != nil {
		return request, err
	}
	if b.timestampInKey {
		timestamp, err := b.timestamp(selected[b.resource.TimestampField])
		if err != nil {
			return request, err
		}
		request.Start = timestamp.Format(time.RFC3339Nano)
		// InfluxDB 2.x includes the stop instant in deletes. Adding a nanosecond
		// would also remove a neighboring point at that timestamp.
		request.Stop = request.Start
	}

	var predicate strings.Builder
	predicate.WriteString("_measurement=")
	predicate.WriteString(quoteString(b.resource.Measurement))
	var tags int
	for _, field := range b.resource.TagFields {
		// Non-key tags may have changed over the document's life, so only key
		// tags identify its points.
		if !b.keyTags[field] {
			continue
		}
		value, err := encodeTag(selected[field])
		if err != nil {
			return request, fmt.Errorf("encoding tag %q: %w", field, err)
		}
		if value == "" {
			return request, fmt.Errorf("tag %q from the collection key is empty: %w", field, errEmptyKeyTag)
		}
		tags++
		predicate.WriteString(" AND ")
		predicate.WriteString(quoteString(field))
		predicate.WriteByte('=')
		predicate.WriteString(quoteString(value))
	}
	if tags == 0 && !b.timestampInKey {
		return request, fmt.Errorf("refusing a delete that would delete every point in measurement %q: it has no tag values and the timestamp field is not in the collection key", b.resource.Measurement)
	}
	request.Predicate = predicate.String()
	if strings.ContainsAny(request.Predicate, "\r\n") {
		return request, fmt.Errorf("delete predicates do not support newlines in measurements, tags, or tag values")
	}
	return request, nil
}
