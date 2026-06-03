package main

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/estuary/connectors/go/writer"
	"github.com/hamba/avro/v2"
)

// partitionColumn binds a resource partition field to a position in the
// materialized row, carrying the resolved iceberg source type and transform so
// partition values can be computed at write time.
type partitionColumn struct {
	rowIndex   int // index into the row []any (= iceberg schema field ID - 1)
	fieldID    int // partition field ID, assigned from iceberg.PartitionDataIDStart
	name       string
	sourceType iceberg.Type
	transform  iceberg.Transform
}

// buildPartitionSpec resolves the resource partition config against the ordered
// field list (AllFields) and parquet schema, returning the iceberg
// PartitionSpec and the per-column metadata needed to compute partition values
// at write time. It returns (nil, nil, nil) when there are no partition fields.
//
// This is the single source of truth for the partition spec, used both when
// creating the table (CreateResource) and when writing rows (NewTransactor), so
// the runtime spec is guaranteed to match the created spec.
func buildPartitionSpec(allFields []string, pqSchema writer.ParquetSchema, partFields []partitionField) (*iceberg.PartitionSpec, []partitionColumn, error) {
	if len(partFields) == 0 {
		return nil, nil, nil
	}

	fieldIndex := make(map[string]int, len(allFields))
	for i, f := range allFields {
		fieldIndex[f] = i
	}

	specFields := make([]iceberg.PartitionField, 0, len(partFields))
	cols := make([]partitionColumn, 0, len(partFields))
	for ordinal, pf := range partFields {
		idx, ok := fieldIndex[pf.Field]
		if !ok {
			return nil, nil, fmt.Errorf("partition field %q is not a selected field", pf.Field)
		}
		sourceType := parquetTypeToIcebergType(pqSchema[idx].DataType)

		transform, err := iceberg.ParseTransform(pf.Transform)
		if err != nil {
			return nil, nil, fmt.Errorf("partition field %q: %w", pf.Field, err)
		}
		if err := validatePartitionTransform(transform); err != nil {
			return nil, nil, fmt.Errorf("partition field %q: %w", pf.Field, err)
		}
		if !transform.CanTransform(sourceType) {
			return nil, nil, fmt.Errorf("transform %s cannot be applied to partition field %q of type %s", transform, pf.Field, sourceType)
		}

		name := partitionFieldName(pf.Field, transform)
		fieldID := iceberg.PartitionDataIDStart + ordinal
		specFields = append(specFields, iceberg.PartitionField{
			SourceID:  idx + 1,
			FieldID:   fieldID,
			Name:      name,
			Transform: transform,
		})
		cols = append(cols, partitionColumn{
			rowIndex:   idx,
			fieldID:    fieldID,
			name:       name,
			sourceType: sourceType,
			transform:  transform,
		})
	}

	spec := iceberg.NewPartitionSpecID(iceberg.InitialPartitionSpecID, specFields...)
	return &spec, cols, nil
}

// validatePartitionTransform rejects transform parameters that iceberg.ParseTransform
// accepts but which would fail at runtime — notably bucket/truncate with a
// non-positive parameter (a zero modulus/width panics in Transform.Apply).
func validatePartitionTransform(t iceberg.Transform) error {
	switch tt := t.(type) {
	case iceberg.BucketTransform:
		if tt.NumBuckets <= 0 {
			return fmt.Errorf("bucket transform requires a positive number of buckets, got %d", tt.NumBuckets)
		}
	case iceberg.TruncateTransform:
		if tt.Width <= 0 {
			return fmt.Errorf("truncate transform requires a positive width, got %d", tt.Width)
		}
	}
	return nil
}

// partitionFieldName derives the partition column name using Iceberg's
// conventional suffixes so the generated spec is stable and human-readable.
func partitionFieldName(field string, t iceberg.Transform) string {
	switch t.(type) {
	case iceberg.BucketTransform:
		return field + "_bucket"
	case iceberg.TruncateTransform:
		return field + "_trunc"
	case iceberg.YearTransform:
		return field + "_year"
	case iceberg.MonthTransform:
		return field + "_month"
	case iceberg.DayTransform:
		return field + "_day"
	case iceberg.HourTransform:
		return field + "_hour"
	case iceberg.VoidTransform:
		return field + "_null"
	default: // IdentityTransform
		return field
	}
}

// partitionSpecMismatch returns a descriptive error when an existing table's
// partition spec is not compatible with the desired spec. A nil spec means
// unpartitioned. The partition spec is immutable after table creation, so any
// difference is a hard error. Fields are compared structurally (ignoring the
// library's cached escaped-name field that PartitionSpec.Equals would include).
func partitionSpecMismatch(existing, desired *iceberg.PartitionSpec) error {
	existingUnpart := existing == nil || existing.IsUnpartitioned()
	desiredUnpart := desired == nil || desired.IsUnpartitioned()

	mismatch := func() error {
		var have, want string = "(none)", "(none)"
		if !existingUnpart {
			have = existing.String()
		}
		if !desiredUnpart {
			want = desired.String()
		}
		return fmt.Errorf("has partition spec %s but config requests %s; changing partition spec requires recreating the table", have, want)
	}

	switch {
	case existingUnpart && desiredUnpart:
		return nil
	case existingUnpart != desiredUnpart:
		return mismatch()
	}

	if existing.NumFields() != desired.NumFields() {
		return mismatch()
	}
	existingFields := slices.Collect(existing.Fields())
	desiredFields := slices.Collect(desired.Fields())
	for i := range existingFields {
		if !existingFields[i].Equals(desiredFields[i]) {
			return mismatch()
		}
	}
	return nil
}

// partitionLogicalTypes returns the avro logical types needed to encode
// date/timestamp partition values in the manifest. Other result types (string,
// integers, bucket's int32) need no logical type. UUID and time fields never
// reach here because schemaOptions force them to iceberg String.
func partitionLogicalTypes(cols []partitionColumn) map[int]avro.LogicalType {
	var out map[int]avro.LogicalType
	for _, col := range cols {
		var lt avro.LogicalType
		switch col.transform.ResultType(col.sourceType).(type) {
		case iceberg.DateType:
			lt = avro.Date
		case iceberg.TimestampType, iceberg.TimestampTzType:
			lt = avro.TimestampMicros
		default:
			continue
		}
		if out == nil {
			out = make(map[int]avro.LogicalType)
		}
		out[col.fieldID] = lt
	}
	return out
}

var (
	minInt64Float = big.NewFloat(float64(math.MinInt64))
	maxInt64Float = big.NewFloat(float64(math.MaxInt64))
)

// rowToLiteral converts a materialized row value (as produced by
// mapped.ConvertAll, typed per the parquet schema) into an iceberg.Literal of
// the column's source type, so a transform's Apply produces the canonical
// partition value. The second return is false for a SQL NULL (nil value).
//
// Only the iceberg source types that the connector actually materializes are
// handled: UUID, time, arrays, objects, and durations are forced to strings by
// schemaOptions, so they arrive here as iceberg String.
func rowToLiteral(v any, sourceType iceberg.Type) (iceberg.Literal, bool, error) {
	if v == nil {
		return nil, false, nil
	}

	switch sourceType {
	case iceberg.PrimitiveTypes.Int64:
		i, err := coerceInt64(v)
		if err != nil {
			return nil, false, err
		}
		return iceberg.NewLiteral(i), true, nil
	case iceberg.PrimitiveTypes.Float64:
		f, err := coerceFloat64(v)
		if err != nil {
			return nil, false, err
		}
		return iceberg.NewLiteral(f), true, nil
	case iceberg.PrimitiveTypes.Bool:
		b, ok := v.(bool)
		if !ok {
			return nil, false, fmt.Errorf("partition value %v (%T) is not a boolean", v, v)
		}
		return iceberg.NewLiteral(b), true, nil
	case iceberg.PrimitiveTypes.String:
		s, err := coerceString(v)
		if err != nil {
			return nil, false, err
		}
		return iceberg.NewLiteral(s), true, nil
	case iceberg.PrimitiveTypes.Binary:
		s, ok := v.(string)
		if !ok {
			return nil, false, fmt.Errorf("partition value %v (%T) is not a base64 string", v, v)
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, false, fmt.Errorf("decoding partition value as binary: %w", err)
		}
		return iceberg.NewLiteral(b), true, nil
	case iceberg.PrimitiveTypes.Date:
		days, err := coerceDateDays(v)
		if err != nil {
			return nil, false, err
		}
		return iceberg.NewLiteral(iceberg.Date(days)), true, nil
	case iceberg.PrimitiveTypes.TimestampTz:
		micros, err := coerceTimestampMicros(v)
		if err != nil {
			return nil, false, err
		}
		return iceberg.NewLiteral(iceberg.Timestamp(micros)), true, nil
	default:
		return nil, false, fmt.Errorf("partitioning on iceberg type %s is not supported", sourceType)
	}
}

// partitionValues computes, for a converted row, a boundary key for equality
// comparison between consecutive rows and the fieldID->value map used to
// populate a DataFile's partition data.
func partitionValues(row []any, cols []partitionColumn) (string, map[int]any, error) {
	var key strings.Builder
	fieldData := make(map[int]any, len(cols))

	for _, col := range cols {
		lit, ok, err := rowToLiteral(row[col.rowIndex], col.sourceType)
		if err != nil {
			return "", nil, fmt.Errorf("computing partition value for field %q: %w", col.name, err)
		}

		var pv any
		if ok {
			out := col.transform.Apply(iceberg.Optional[iceberg.Literal]{Val: lit, Valid: true})
			if out.Valid {
				pv = out.Val.Any()
			}
		}

		fieldData[col.fieldID] = pv
		fmt.Fprintf(&key, "%v\x1f", pv)
	}

	return key.String(), fieldData, nil
}

func coerceInt64(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	case float64:
		f := big.NewFloat(t)
		if f.Cmp(minInt64Float) < 0 || f.Cmp(maxInt64Float) > 0 {
			return 0, fmt.Errorf("float64 value %f is out of range for int64", t)
		}
		i, _ := f.Int64()
		return i, nil
	case string:
		if idx := strings.Index(t, "."); idx != -1 {
			t = t[:idx]
		}
		i, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing %q as integer: %w", t, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("partition value %v (%T) is not an integer", v, v)
	}
}

func coerceFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case float32:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing %q as number: %w", t, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("partition value %v (%T) is not a number", v, v)
	}
}

func coerceString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case bool:
		return strconv.FormatBool(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("partition value %v (%T) is not a string", v, v)
	}
}

func coerceDateDays(v any) (int32, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("partition value %v (%T) is not a date string", v, v)
	}
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return 0, fmt.Errorf("parsing %q as date: %w", s, err)
	}
	days := d.Unix() / 60 / 60 / 24
	if days > math.MaxInt32 {
		days = math.MaxInt32
	} else if days < math.MinInt32 {
		days = math.MinInt32
	}
	return int32(days), nil
}

func coerceTimestampMicros(v any) (int64, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("partition value %v (%T) is not a timestamp string", v, v)
	}
	s = strings.Replace(s, "z", "Z", 1)
	d, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return 0, fmt.Errorf("parsing %q as timestamp: %w", s, err)
	}
	return d.UnixMicro(), nil
}
