package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

var (
	measurementEscaper = strings.NewReplacer(",", `\,`, " ", `\ `)
	keyEscaper         = strings.NewReplacer(",", `\,`, " ", `\ `, "=", `\=`)
	stringEscaper      = strings.NewReplacer(`\`, `\\`, `"`, `\"`)
)

func (b *measurementBinding) point(key, values tuple.Tuple, doc json.RawMessage) (string, error) {
	selected, err := b.selectedFields(key, values, doc)
	if err != nil {
		return "", err
	}
	timestamp, err := b.timestamp(selected[b.resource.TimestampField])
	if err != nil {
		return "", err
	}

	if err := checkLineElement(b.resource.Measurement); err != nil {
		return "", fmt.Errorf("invalid measurement: %w", err)
	}
	var line strings.Builder
	line.WriteString(measurementEscaper.Replace(b.resource.Measurement))
	for _, field := range b.resource.TagFields {
		value, err := encodeTag(selected[field])
		if err != nil {
			return "", fmt.Errorf("encoding tag %q: %w", field, err)
		}
		if value == "" {
			continue
		}
		if err := checkLineElement(field); err != nil {
			return "", fmt.Errorf("encoding tag %q: %w", field, err)
		}
		if err := checkLineElement(value); err != nil {
			return "", fmt.Errorf("encoding tag %q: %w", field, err)
		}
		line.WriteByte(',')
		line.WriteString(keyEscaper.Replace(field))
		line.WriteByte('=')
		line.WriteString(keyEscaper.Replace(value))
	}

	var fields int
	for _, field := range b.fields.AllFields() {
		if field == b.resource.TimestampField || slices.Contains(b.resource.TagFields, field) {
			continue
		}
		value, err := encodeField(selected[field], b.numericFields[field])
		if err != nil {
			return "", fmt.Errorf("encoding field %q: %w", field, err)
		}
		if value == "" {
			continue
		}
		if err := checkLineElement(field); err != nil {
			return "", fmt.Errorf("encoding field %q: %w", field, err)
		}
		if fields == 0 {
			line.WriteByte(' ')
		} else {
			line.WriteByte(',')
		}
		line.WriteString(keyEscaper.Replace(field))
		line.WriteByte('=')
		line.WriteString(value)
		fields++
	}
	if fields == 0 {
		return "", nil
	}
	line.WriteByte(' ')
	line.WriteString(strconv.FormatInt(timestamp.UnixNano(), 10))
	return line.String(), nil
}

// checkLineElement rejects a measurement, tag key or value, or field key that
// line protocol cannot represent. Quoted string field values may hold newlines.
func checkLineElement(value string) error {
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("line protocol does not support newlines in measurements, tags, or field names")
	}
	// Line protocol has no escape for a backslash, so a trailing one would
	// escape the separator that follows it.
	if strings.HasSuffix(value, `\`) {
		return fmt.Errorf("line protocol does not support measurements, tags, or field names ending in a backslash")
	}
	return nil
}

func (b *measurementBinding) selectedFields(key, values tuple.Tuple, doc json.RawMessage) (map[string]any, error) {
	if len(key) != len(b.fields.Keys) || len(values) != len(b.fields.Values) {
		return nil, fmt.Errorf("stored keys and values do not match the field selection")
	}
	selected := make(map[string]any, len(key)+len(values)+1)
	for i, field := range b.fields.Keys {
		selected[field] = key[i]
	}
	for i, field := range b.fields.Values {
		selected[field] = values[i]
	}
	if b.fields.Document != "" {
		selected[b.fields.Document] = doc
	}
	return selected, nil
}

func (b *measurementBinding) timestamp(value any) (time.Time, error) {
	stamp, ok := value.(string)
	if !ok {
		return time.Time{}, fmt.Errorf("timestamp field %q must contain a date-time string", b.resource.TimestampField)
	}
	timestamp, err := time.Parse(time.RFC3339Nano, stamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing timestamp field %q: %w", b.resource.TimestampField, err)
	}
	ns := timestamp.UnixNano()
	if !time.Unix(0, ns).Equal(timestamp) || ns < math.MinInt64+2 || ns > math.MaxInt64-1 {
		return time.Time{}, fmt.Errorf("timestamp field %q is outside InfluxDB's nanosecond time range", b.resource.TimestampField)
	}
	return timestamp.UTC(), nil
}

func encodeTag(value any) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return encodeFloat(v)
	case bool:
		return strconv.FormatBool(v), nil
	case []byte:
		decoded, err := decodeJSONScalar(v)
		if err != nil {
			return "", err
		}
		return encodeTag(decoded)
	case json.Number:
		return string(v), nil
	default:
		return "", fmt.Errorf("expected a scalar tag, got %T", value)
	}
}

func encodeField(value any, numericType string) (string, error) {
	switch v := value.(type) {
	case nil:
		return "", nil
	case string:
		return quoteString(v), nil
	case int64:
		if numericType == "number" {
			return strconv.FormatInt(v, 10), nil
		}
		return strconv.FormatInt(v, 10) + "i", nil
	case uint64:
		if numericType == "number" {
			return strconv.FormatUint(v, 10), nil
		}
		if v > math.MaxInt64 {
			return "", fmt.Errorf("integer exceeds InfluxDB's signed 64-bit range")
		}
		return strconv.FormatUint(v, 10) + "i", nil
	case float64:
		if numericType == "integer" {
			if math.IsNaN(v) || math.Trunc(v) != v || v < math.MinInt64 || v >= math.MaxInt64 {
				return "", fmt.Errorf("expected an integer in InfluxDB's signed 64-bit range")
			}
			return strconv.FormatInt(int64(v), 10) + "i", nil
		}
		return encodeFloat(v)
	case bool:
		return strconv.FormatBool(v), nil
	case []byte:
		var compact bytes.Buffer
		if err := json.Compact(&compact, v); err != nil {
			return "", fmt.Errorf("invalid JSON field: %w", err)
		}
		if compact.Bytes()[0] == '{' || compact.Bytes()[0] == '[' {
			return quoteString(compact.String()), nil
		}
		decoded, err := decodeJSONScalar(compact.Bytes())
		if err != nil {
			return "", err
		}
		return encodeField(decoded, numericType)
	case json.RawMessage:
		var compact bytes.Buffer
		if err := json.Compact(&compact, v); err != nil {
			return "", fmt.Errorf("invalid JSON document: %w", err)
		}
		return quoteString(compact.String()), nil
	case json.Number:
		if numericType != "number" && !strings.ContainsAny(string(v), ".eE") {
			n, err := v.Int64()
			if err != nil {
				return "", fmt.Errorf("integer exceeds InfluxDB's signed 64-bit range: %w", err)
			}
			return strconv.FormatInt(n, 10) + "i", nil
		}
		n, err := v.Float64()
		if err != nil {
			return "", fmt.Errorf("invalid number: %w", err)
		}
		return encodeField(n, numericType)
	default:
		return "", fmt.Errorf("unsupported field type %T", value)
	}
}

func decodeJSONScalar(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("invalid JSON value: %w", err)
	}
	return value, nil
}

func encodeFloat(value float64) (string, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", fmt.Errorf("InfluxDB fields and tags must contain finite numbers")
	}
	return strconv.FormatFloat(value, 'g', -1, 64), nil
}

func quoteString(value string) string {
	return `"` + stringEscaper.Replace(value) + `"`
}
