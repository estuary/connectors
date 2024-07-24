package main

import (
	"database/sql/driver"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

func registerDatatypeTweaks(m *pgtype.Map) error {
	// Prefer text format for 'timestamptz' column results. This is important because the
	// text format is reported in the configured time zone of the server (since we haven't
	// set it on our session) while the binary format is always a Unix microsecond timestamp
	// in UTC and then Go's time.Unix() turns that into a local-time value. Thus without this
	// customization backfills and replication will report the same instant in time in different
	// time zones. The easiest way to make them consistent is to just ask for text here.
	var tstz = &pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &preferTextCodec{inner: &pgtype.TimestamptzCodec{}}}
	m.RegisterType(tstz)

	// Also prefer text format for the endpoints of 'tstzrange' values for the same reason.
	m.RegisterType(&pgtype.Type{Name: "tstzrange", OID: pgtype.TstzrangeOID, Codec: &preferTextCodec{&pgtype.RangeCodec{ElementType: tstz}}})

	// Decode array column types into the dimensioned `pgtype.Array[any]` type rather than
	// the default behavior of a flattened `[]any` list.
	//
	// List of array type OIDs taken from initDefaultMap() at pgtype_default.go:110
	//
	// TODO(wgd): How does this apply to arrays of user-defined data types like enums?
	var arrayTypeOIDs = []uint32{
		pgtype.ACLItemArrayOID, pgtype.BitArrayOID, pgtype.BoolArrayOID, pgtype.BoxArrayOID, pgtype.BPCharArrayOID, pgtype.ByteaArrayOID, pgtype.QCharArrayOID, pgtype.CIDArrayOID,
		pgtype.CIDRArrayOID, pgtype.CircleArrayOID, pgtype.DateArrayOID, pgtype.DaterangeArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.InetArrayOID, pgtype.Int2ArrayOID,
		pgtype.Int4ArrayOID, pgtype.Int4rangeArrayOID, pgtype.Int8ArrayOID, pgtype.Int8rangeArrayOID, pgtype.IntervalArrayOID, pgtype.JSONArrayOID, pgtype.JSONBArrayOID, pgtype.JSONPathArrayOID,
		pgtype.LineArrayOID, pgtype.LsegArrayOID, pgtype.MacaddrArrayOID, pgtype.NameArrayOID, pgtype.NumericArrayOID, pgtype.NumrangeArrayOID, pgtype.OIDArrayOID, pgtype.PathArrayOID,
		pgtype.PointArrayOID, pgtype.PolygonArrayOID, pgtype.RecordArrayOID, pgtype.TextArrayOID, pgtype.TIDArrayOID, pgtype.TimeArrayOID, pgtype.TimestampArrayOID, pgtype.TimestamptzArrayOID,
		pgtype.TsrangeArrayOID, pgtype.TstzrangeArrayOID, pgtype.UUIDArrayOID, pgtype.VarbitArrayOID, pgtype.VarcharArrayOID, pgtype.XIDArrayOID,
	}
	for _, oid := range arrayTypeOIDs {
		var typ, ok = m.TypeForOID(oid)
		if !ok {
			return fmt.Errorf("error adjusting array codec: OID %d not found", oid)
		}
		codec, ok := typ.Codec.(*pgtype.ArrayCodec)
		if !ok {
			return fmt.Errorf("error adjusting array codec: OID %d does not have array codec", oid)
		}
		m.RegisterType(&pgtype.Type{
			Name:  typ.Name,
			OID:   typ.OID,
			Codec: &dimensionedArrayCodec{inner: &pgtype.ArrayCodec{ElementType: codec.ElementType}},
		})
	}
	return nil
}

// preferTextCodec wraps a PGX datatype codec to make it always report text as its preferred format if supported.
type preferTextCodec struct{ inner pgtype.Codec }

func (c *preferTextCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *preferTextCodec) PreferredFormat() int16 {
	if c.FormatSupported(pgtype.TextFormatCode) {
		return pgtype.TextFormatCode
	}
	return c.inner.PreferredFormat()
}
func (c *preferTextCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *preferTextCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *preferTextCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *preferTextCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	return c.inner.DecodeValue(m, oid, format, src)
}

// dimensionedArrayCodec wraps a PGX array codec to make it always decode values into a pgtype.Array[any]
type dimensionedArrayCodec struct{ inner pgtype.Codec }

func (c *dimensionedArrayCodec) FormatSupported(f int16) bool { return c.inner.FormatSupported(f) }
func (c *dimensionedArrayCodec) PreferredFormat() int16       { return c.inner.PreferredFormat() }
func (c *dimensionedArrayCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	return c.inner.PlanEncode(m, oid, format, value)
}
func (c *dimensionedArrayCodec) PlanScan(m *pgtype.Map, oid uint32, format int16, target any) pgtype.ScanPlan {
	return c.inner.PlanScan(m, oid, format, target)
}
func (c *dimensionedArrayCodec) DecodeDatabaseSQLValue(m *pgtype.Map, oid uint32, format int16, src []byte) (driver.Value, error) {
	return c.inner.DecodeDatabaseSQLValue(m, oid, format, src)
}
func (c *dimensionedArrayCodec) DecodeValue(m *pgtype.Map, oid uint32, format int16, src []byte) (any, error) {
	if src == nil {
		return nil, nil
	}
	var dst pgtype.Array[any]
	err := m.PlanScan(oid, format, &dst).Scan(src, &dst)
	return dst, err
}
