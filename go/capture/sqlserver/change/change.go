package change

import (
	"fmt"

	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
)

// Metadata is implemented by CDC and CT metadata types.
type Metadata interface {
	// Op returns the change operation type (insert/update/delete).
	Op() sqlcapture.ChangeOp
	// AppendJSON appends the JSON representation to the buffer.
	AppendJSON(buf []byte) ([]byte, error)
}

// SharedInfo holds information shared across change events for a table.
type SharedInfo struct {
	StreamID          sqlcapture.StreamID        // StreamID of the table this change event came from.
	Shape             *encrow.Shape              // Shape of the document values, used to serialize them to JSON.
	Transcoders       []datatypes.JSONTranscoder // Transcoders for column values, in DB column order.
	DeleteNullability []bool                     // Whether nil values of a particular column should be included in delete events.
}

// Event represents a change event with metadata of type M.
type Event[M Metadata] struct {
	Shared *SharedInfo
	Meta   M
	RowKey []byte
	Values []any
}

func (Event[M]) IsDatabaseEvent() {}
func (Event[M]) IsChangeEvent()   {}

func (e *Event[M]) String() string {
	switch e.Meta.Op() {
	case sqlcapture.InsertOp:
		return fmt.Sprintf("Insert(%s)", e.Shared.StreamID)
	case sqlcapture.UpdateOp:
		return fmt.Sprintf("Update(%s)", e.Shared.StreamID)
	case sqlcapture.DeleteOp:
		return fmt.Sprintf("Delete(%s)", e.Shared.StreamID)
	}
	return fmt.Sprintf("UnknownChange(%s)", e.Shared.StreamID)
}

func (e *Event[M]) StreamID() sqlcapture.StreamID {
	return e.Shared.StreamID
}

func (e *Event[M]) GetRowKey() []byte {
	return e.RowKey
}

// AppendJSON appends the JSON representation of the change event to the provided buffer.
func (e *Event[M]) AppendJSON(buf []byte) ([]byte, error) {
	var shape = e.Shared.Shape

	// The number of byte values should be one less than the shape's arity, because the last field of
	// the shape is the metadata. Note that this also means there can never be an empty shape here.
	if len(e.Values) != shape.Arity-1 {
		return nil, fmt.Errorf("incorrect row arity: expected %d but got %d", shape.Arity, len(e.Values))
	}

	var err error
	var subsequentField bool
	buf = append(buf, '{')
	for idx, vidx := range shape.Swizzle {
		var val any
		if vidx < len(e.Values) {
			val = e.Values[vidx]
			if e.Meta.Op() == sqlcapture.DeleteOp && val == nil && !e.Shared.DeleteNullability[vidx] {
				// For delete events, non-key column values may be missing or null depending on
				// the change tracking mechanism. If a null value appears in a column that doesn't
				// allow nulls in this context, we skip the field entirely rather than emitting
				// an invalid null value.
				continue
			}
		}
		if subsequentField {
			buf = append(buf, ',')
		}
		subsequentField = true
		buf = append(buf, shape.Prefixes[idx]...)
		if vidx < len(e.Values) {
			buf, err = e.Shared.Transcoders[vidx].TranscodeJSON(buf, val)
		} else {
			buf, err = e.Meta.AppendJSON(buf) // The last value index is the metadata
		}
		if err != nil {
			return nil, fmt.Errorf("error encoding field %q: %w", shape.Names[idx], err)
		}
	}
	return append(buf, '}'), nil
}
