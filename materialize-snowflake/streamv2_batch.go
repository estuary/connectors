package connector

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"
)

// streamV2Batch accumulates encoded rows into the payload of one Snowpipe
// Streaming append call. The payload is kept a JSON array at every step, so a
// batch is shipped by closing it rather than by re-reading its rows.
type streamV2Batch struct {
	enc         *streamV2RowEncoder
	payload     []byte
	rows        int
	firstOffset int64
	// hint is the size of the batch finished before this one, and it sizes the
	// next payload.
	hint int
}

// streamV2MinBatchCapacity is the size a payload starts with.
const streamV2MinBatchCapacity = 8 * 1024

func newStreamV2Batch(enc *streamV2RowEncoder) streamV2Batch {
	return streamV2Batch{enc: enc}
}

// addRow encodes one document into the payload and reports how many bytes the
// payload grew by.
func (b *streamV2Batch) addRow(offset int64, converted []any) (int, error) {
	var before = len(b.payload)
	if b.rows == 0 {
		b.start()
		b.firstOffset = offset
	} else {
		b.payload = append(b.payload, ',')
	}

	var payload, err = b.enc.appendRow(b.payload, converted)
	if err != nil {
		if b.rows == 0 {
			b.payload = nil
		} else {
			b.payload = b.payload[:before]
		}
		return 0, err
	}

	b.payload = payload
	b.rows++
	return len(b.payload) - before, nil
}

// start opens the payload of a new batch. The size is a little over the batch
// before it, so a full batch does not grow and copy a dozen times on its way to
// 8 MiB. The payload is always a new allocation, because the batch before it
// gave its bytes away.
func (b *streamV2Batch) start() {
	b.payload = append(make([]byte, 0, max(b.hint+b.hint/8, streamV2MinBatchCapacity)), '[')
}

func (b *streamV2Batch) empty() bool {
	return b.rows == 0
}

func (b *streamV2Batch) size() int {
	return len(b.payload)
}

// finish closes the payload and gives up ownership of it. It reports the payload,
// the number of rows it holds, and the buffered bytes it releases.
func (b *streamV2Batch) finish() (payload []byte, rows, released int) {
	released = len(b.payload)
	payload = append(b.payload, ']')
	rows = b.rows

	b.hint = len(payload)
	b.payload, b.rows, b.firstOffset = nil, 0, 0
	return payload, rows, released
}

// streamV2RowEncoder encodes rows of column values as a JSON objects keyed by
// column name. It holds the column names already serialized, because they are
// the same for every row it encodes.
type streamV2RowEncoder struct {
	columns []columnName
}

// columnName is one column's name in the two forms the encoder needs.
type columnName struct {
	// serialized is a comma, the name as a quoted and escaped JSON string,
	// and a colon. The first column written in a row skips the comma.
	serialized []byte
	// raw is the name as given, unquoted and unescaped.
	raw string
}

func newStreamV2RowEncoder(names []string) *streamV2RowEncoder {
	var e = &streamV2RowEncoder{columns: make([]columnName, len(names))}
	for i, name := range names {
		var serialized = e.appendString([]byte{','}, name)
		e.columns[i] = columnName{serialized: append(serialized, ':'), raw: name}
	}
	return e
}

func (e *streamV2RowEncoder) appendRow(buf []byte, converted []any) ([]byte, error) {
	var start = len(buf)
	buf = append(buf, '{')

	var first = true
	for i, value := range converted {
		if value == nil {
			continue
		}

		var serialized = e.columns[i].serialized
		if first {
			serialized, first = serialized[1:], false
		}
		buf = append(buf, serialized...)

		var err error
		if buf, err = e.appendValue(buf, value); err != nil {
			return buf[:start], fmt.Errorf("column %s: %w", e.columns[i].raw, err)
		}
	}

	return append(buf, '}'), nil
}

func (e *streamV2RowEncoder) appendValue(buf []byte, value any) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return e.appendString(buf, v), nil
	case json.RawMessage:
		// An empty payload is left to the standard encoder, which writes a nil
		// RawMessage as null and rejects an empty non-nil one.
		if len(v) == 0 {
			return e.appendMarshaled(buf, value)
		}
		return e.appendPreEncoded(buf, v)
	case []byte:
		if v == nil {
			return append(buf, "null"...), nil
		}
		buf = append(buf, '"')
		buf = base64.StdEncoding.AppendEncode(buf, v)
		return append(buf, '"'), nil
	case bool:
		return strconv.AppendBool(buf, v), nil
	case int64:
		return strconv.AppendInt(buf, v, 10), nil
	case uint64:
		return strconv.AppendUint(buf, v, 10), nil
	case float64:
		return e.appendFloat(buf, v)
	case *big.Int:
		// The standard encoder reaches the same digits through this type's
		// json.Marshaler, at three allocations a row for a column type — an
		// integer too wide for INTEGER, carried as a string — which is common
		// enough in collection schemas to be worth its own case.
		if v == nil {
			return append(buf, "null"...), nil
		}
		return v.Append(buf, 10), nil
	default:
		return e.appendMarshaled(buf, value)
	}
}

func (e *streamV2RowEncoder) appendMarshaled(buf []byte, value any) ([]byte, error) {
	var encoded, err = json.Marshal(value)
	if err != nil {
		return buf, err
	}
	return append(buf, encoded...), nil
}

func (e *streamV2RowEncoder) appendPreEncoded(buf []byte, raw json.RawMessage) ([]byte, error) {
	if bytes.IndexByte(raw, '\n') < 0 && bytes.IndexByte(raw, '\r') < 0 {
		return append(buf, raw...), nil
	}

	var compacted bytes.Buffer
	if err := json.Compact(&compacted, raw); err != nil {
		return buf, err
	}
	return append(buf, compacted.Bytes()...), nil
}

func (e *streamV2RowEncoder) appendFloat(buf []byte, f float64) ([]byte, error) {
	if math.IsNaN(f) {
		return buf, fmt.Errorf("cannot encode NaN as JSON")
	} else if math.IsInf(f, 0) {
		return buf, fmt.Errorf("cannot encode %v as JSON", f)
	}

	var format byte = 'f'
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	buf = strconv.AppendFloat(buf, f, format, -1, 64)

	if format == 'e' {
		// Trim the leading zero of a two-digit negative exponent: 1e-09 -> 1e-9.
		if n := len(buf); n >= 4 && buf[n-4] == 'e' && buf[n-3] == '-' && buf[n-2] == '0' {
			buf[n-2] = buf[n-1]
			buf = buf[:n-1]
		}
	}
	return buf, nil
}

const hexDigits = "0123456789abcdef"

func (e *streamV2RowEncoder) appendString(buf []byte, s string) []byte {
	buf = append(buf, '"')

	var start int
	for i := 0; i < len(s); {
		var c = s[i]
		if c >= utf8.RuneSelf {
			var r, size = utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				buf = append(buf, s[start:i]...)
				buf = append(buf, `�`...)
				i += size
				start = i
				continue
			}
			i += size
			continue
		} else if c >= 0x20 && c != '"' && c != '\\' {
			i++
			continue
		}

		buf = append(buf, s[start:i]...)
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			buf = append(buf, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		}
		i++
		start = i
	}

	buf = append(buf, s[start:]...)
	return append(buf, '"')
}
