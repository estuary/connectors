package main

import (
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
)

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format.
func translateRecordField(typeName string, val interface{}) (interface{}, error) {
	if val, ok := val.([]byte); ok && typeName == "JSONB" {
		return json.RawMessage(val), nil
	}

	switch x := val.(type) {
	case *net.IPNet:
		return x.String(), nil
	case net.HardwareAddr:
		return x.String(), nil
	case [16]uint8: // UUIDs
		var s = new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	case pgtype.Bytea:
		return x.Bytes, nil
	case pgtype.Float4:
		return x.Float, nil
	case pgtype.Float8:
		return x.Float, nil
	case pgtype.BPCharArray, pgtype.BoolArray, pgtype.ByteaArray, pgtype.CIDRArray,
		pgtype.DateArray, pgtype.EnumArray, pgtype.Float4Array, pgtype.Float8Array,
		pgtype.HstoreArray, pgtype.InetArray, pgtype.Int2Array, pgtype.Int4Array,
		pgtype.Int8Array, pgtype.JSONBArray, pgtype.MacaddrArray, pgtype.NumericArray,
		pgtype.TextArray, pgtype.TimestampArray, pgtype.TimestamptzArray, pgtype.TsrangeArray,
		pgtype.TstzrangeArray, pgtype.UUIDArray, pgtype.UntypedTextArray, pgtype.VarcharArray:
		return translateArray(typeName, x)
	case pgtype.InfinityModifier:
		if x == pgtype.Infinity {
			return "9999-12-31T23:59:59Z", nil
		} else if x == pgtype.NegativeInfinity {
			return "0000-01-01T00:00:00Z", nil
		}
	}
	if _, ok := val.(json.Marshaler); ok {
		return val, nil
	}
	if enc, ok := val.(pgtype.TextEncoder); ok {
		var bs, err = enc.EncodeText(nil, nil)
		return string(bs), err
	}
	return val, nil
}

func translateArray(typeName string, x interface{}) (interface{}, error) {
	// Use reflection to extract the 'elements' field
	var array = reflect.ValueOf(x)
	if array.Kind() != reflect.Struct {
		return nil, fmt.Errorf("array translation expected struct, got %v", array.Kind())
	}

	var elements = array.FieldByName("Elements")
	if elements.Kind() != reflect.Slice {
		return nil, fmt.Errorf("array translation expected Elements slice, got %v", elements.Kind())
	}
	var vals = make([]interface{}, elements.Len())
	for idx := 0; idx < len(vals); idx++ {
		var element = elements.Index(idx)
		var translated, err = translateRecordField(typeName, element.Interface())
		if err != nil {
			return nil, fmt.Errorf("error translating array element %d: %w", idx, err)
		}
		vals[idx] = translated
	}

	var dimensions, ok = array.FieldByName("Dimensions").Interface().([]pgtype.ArrayDimension)
	if !ok {
		return nil, fmt.Errorf("array translation error: expected Dimensions to have type []ArrayDimension")
	}
	var dims = make([]int, len(dimensions))
	for idx := 0; idx < len(dims); idx++ {
		dims[idx] = int(dimensions[idx].Length)
	}

	return map[string]interface{}{
		"dimensions": dims,
		"elements":   vals,
	}, nil
}
