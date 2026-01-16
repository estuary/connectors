package datatypes

import (
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
)

// JSONTranscoder is something which can transcode a SQL Server client library value into JSON.
type JSONTranscoder interface {
	TranscodeJSON(buf []byte, v any) ([]byte, error)
}

// JSONTranscoderFunc is a function type that implements JSONTranscoder.
type JSONTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f JSONTranscoderFunc) TranscodeJSON(buf []byte, v any) ([]byte, error) {
	return f(buf, v)
}

// FDBTranscoder is something which can transcode a SQL Server client library value into an FDB tuple value.
type FDBTranscoder interface {
	TranscodeFDB(buf []byte, v any) ([]byte, error)
}

// FDBTranscoderFunc is a function type that implements FDBTranscoder.
type FDBTranscoderFunc func(buf []byte, v any) ([]byte, error)

func (f FDBTranscoderFunc) TranscodeFDB(buf []byte, v any) ([]byte, error) { return f(buf, v) }

// NewJSONTranscoder creates a JSONTranscoder for the given column type.
// The cfg parameter provides configuration like datetime timezone handling.
func NewJSONTranscoder(columnType any, cfg *Config) JSONTranscoder {
	return JSONTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := TranslateColumnValue(columnType, v, cfg); err != nil {
			return nil, fmt.Errorf("error translating value %v for JSON serialization: %w", v, err)
		} else {
			return json.Append(buf, translated, json.EscapeHTML|json.SortMapKeys)
		}
	})
}

// NewFDBTranscoder creates an FDBTranscoder for the given column type.
func NewFDBTranscoder(columnType any) FDBTranscoder {
	return FDBTranscoderFunc(func(buf []byte, v any) ([]byte, error) {
		if translated, err := EncodeKeyFDB(v, columnType); err != nil {
			return nil, fmt.Errorf("error translating value %v for FDB serialization: %w", v, err)
		} else {
			return sqlcapture.AppendFDB(buf, translated)
		}
	})
}
