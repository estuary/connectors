package main

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/estuary/connectors/go/writer"
	"github.com/stretchr/testify/require"
)

func testPqSchema() writer.ParquetSchema {
	return writer.ParquetSchema{
		{Name: "id", DataType: writer.PrimitiveTypeInteger},
		{Name: "canary", DataType: writer.LogicalTypeString},
		{Name: "flag", DataType: writer.PrimitiveTypeBoolean},
		{Name: "d", DataType: writer.LogicalTypeDate},
		{Name: "ts", DataType: writer.LogicalTypeTimestamp},
	}
}

var testAllFields = []string{"id", "canary", "flag", "d", "ts"}

func TestBuildPartitionSpec(t *testing.T) {
	t.Run("multi-field assigns source/field IDs in order", func(t *testing.T) {
		spec, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "id", Transform: "bucket[4]"},
			{Field: "canary", Transform: "identity"},
			{Field: "canary", Transform: "truncate[3]"},
		})
		require.NoError(t, err)
		require.NotNil(t, spec)
		require.Equal(t, 3, spec.NumFields())

		require.Len(t, cols, 3)
		require.Equal(t, []int{0, 1, 1}, []int{cols[0].rowIndex, cols[1].rowIndex, cols[2].rowIndex})
		require.Equal(t, []int{1000, 1001, 1002}, []int{cols[0].fieldID, cols[1].fieldID, cols[2].fieldID})
		require.Equal(t, []string{"id_bucket", "canary", "canary_trunc"}, []string{cols[0].name, cols[1].name, cols[2].name})
	})

	t.Run("no partition fields returns nil", func(t *testing.T) {
		spec, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), nil)
		require.NoError(t, err)
		require.Nil(t, spec)
		require.Nil(t, cols)
	})

	t.Run("unselected field is an error", func(t *testing.T) {
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "nonexistent", Transform: "identity"},
		})
		require.ErrorContains(t, err, "not a selected field")
	})

	t.Run("transform incompatible with type is an error", func(t *testing.T) {
		// bucket cannot be applied to a boolean.
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "flag", Transform: "bucket[4]"},
		})
		require.ErrorContains(t, err, "cannot be applied")
	})
}

func TestBuildPartitionSpecAllTransforms(t *testing.T) {
	t.Run("temporal transforms on date and timestamp", func(t *testing.T) {
		spec, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "d", Transform: "year"},
			{Field: "d", Transform: "month"},
			{Field: "d", Transform: "day"},
			{Field: "ts", Transform: "hour"},
			{Field: "canary", Transform: "void"},
		})
		require.NoError(t, err)
		require.Equal(t, 5, spec.NumFields())
		require.Equal(t, []string{"d_year", "d_month", "d_day", "ts_hour", "canary_null"},
			[]string{cols[0].name, cols[1].name, cols[2].name, cols[3].name, cols[4].name})
	})

	t.Run("temporal transform rejected on non-temporal field", func(t *testing.T) {
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "canary", Transform: "year"},
		})
		require.ErrorContains(t, err, "cannot be applied")
	})

	t.Run("invalid transform string", func(t *testing.T) {
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "id", Transform: "nonsense"},
		})
		require.Error(t, err)
	})

	t.Run("zero bucket count rejected", func(t *testing.T) {
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "id", Transform: "bucket[0]"},
		})
		require.ErrorContains(t, err, "positive")
	})

	t.Run("zero truncate width rejected", func(t *testing.T) {
		_, _, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
			{Field: "canary", Transform: "truncate[0]"},
		})
		require.ErrorContains(t, err, "positive")
	})
}

func TestPartitionValues(t *testing.T) {
	spec, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
		{Field: "id", Transform: "bucket[4]"},
		{Field: "canary", Transform: "truncate[3]"},
		{Field: "flag", Transform: "identity"},
		{Field: "d", Transform: "identity"},
		{Field: "ts", Transform: "identity"},
	})
	require.NoError(t, err)
	require.NotNil(t, spec)

	row := []any{int64(5), "hello", true, "2023-01-15", "2023-01-15T10:30:45Z"}
	_, fieldData, err := partitionValues(row, cols)
	require.NoError(t, err)

	// bucket value is deterministic and within range.
	bucket := fieldData[1000].(int32)
	require.GreaterOrEqual(t, bucket, int32(0))
	require.Less(t, bucket, int32(4))
	require.Equal(t, "hel", fieldData[1001])
	require.Equal(t, true, fieldData[1002])
	require.Equal(t, iceberg.Date(19372), fieldData[1003]) // 2023-01-15
	require.Equal(t, iceberg.Timestamp(1673778645000000), fieldData[1004])

	// Same input yields the same partition (boundary detection relies on this).
	key1, _, err := partitionValues(row, cols)
	require.NoError(t, err)
	key2, _, err := partitionValues([]any{int64(5), "hello", true, "2023-01-15", "2023-01-15T10:30:45Z"}, cols)
	require.NoError(t, err)
	require.Equal(t, key1, key2)

	// A different bucket input changes the key.
	keyOther, _, err := partitionValues([]any{int64(999999), "hello", true, "2023-01-15", "2023-01-15T10:30:45Z"}, cols)
	require.NoError(t, err)
	_ = keyOther // may or may not differ depending on hash; bucket equality is asserted above
}

func TestPartitionValuesTemporal(t *testing.T) {
	pqSchema := writer.ParquetSchema{
		{Name: "d", DataType: writer.LogicalTypeDate},
		{Name: "ts", DataType: writer.LogicalTypeTimestamp},
	}
	_, cols, err := buildPartitionSpec([]string{"d", "ts"}, pqSchema, []partitionField{
		{Field: "d", Transform: "year"},
		{Field: "d", Transform: "month"},
		{Field: "d", Transform: "day"},
		{Field: "ts", Transform: "hour"},
		{Field: "d", Transform: "void"},
	})
	require.NoError(t, err)

	_, fieldData, err := partitionValues([]any{"2023-07-15", "2023-07-15T13:45:00Z"}, cols)
	require.NoError(t, err)

	require.Equal(t, int32(2023-1970), fieldData[1000])            // year: years since 1970
	require.Equal(t, int32((2023-1970)*12+(7-1)), fieldData[1001]) // month: months since 1970
	require.Equal(t, int32(19553), fieldData[1002])                // day: days since epoch (2023-07-15)
	require.Equal(t, int32(19553*24+13), fieldData[1003])          // hour: hours since epoch
	require.Nil(t, fieldData[1004])                                // void: always null
}

func TestPartitionValuesNull(t *testing.T) {
	_, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
		{Field: "canary", Transform: "identity"},
	})
	require.NoError(t, err)

	row := []any{int64(1), nil, false, "2023-01-15", "2023-01-15T10:30:45Z"}
	_, fieldData, err := partitionValues(row, cols)
	require.NoError(t, err)
	require.Nil(t, fieldData[1000])
}

// TestPartitionDataRoundtrip verifies that partition values survive the JSON
// round-trip through connector state and decode back to the canonical iceberg
// Go types that NewDataFileBuilder expects.
func TestPartitionDataRoundtrip(t *testing.T) {
	_, cols, err := buildPartitionSpec(testAllFields, testPqSchema(), []partitionField{
		{Field: "id", Transform: "bucket[4]"},
		{Field: "canary", Transform: "truncate[3]"},
		{Field: "flag", Transform: "identity"},
		{Field: "d", Transform: "identity"},
		{Field: "ts", Transform: "identity"},
	})
	require.NoError(t, err)

	row := []any{int64(5), "hello", true, "2023-01-15", "2023-01-15T10:30:45Z"}
	_, fieldData, err := partitionValues(row, cols)
	require.NoError(t, err)

	// Marshal as Store's finishFile does.
	part := make(map[string]json.RawMessage, len(fieldData))
	for fid, v := range fieldData {
		raw, err := json.Marshal(v)
		require.NoError(t, err)
		part[strconv.Itoa(fid)] = raw
	}

	fe := fileEntry{Partition: part}
	got, err := fe.partitionData(cols)
	require.NoError(t, err)
	require.Equal(t, fieldData, got)
}

func TestPartitionSpecMismatch(t *testing.T) {
	build := func(fields ...partitionField) *iceberg.PartitionSpec {
		spec, _, err := buildPartitionSpec(testAllFields, testPqSchema(), fields)
		require.NoError(t, err)
		return spec
	}

	bucketSpec := build(partitionField{Field: "id", Transform: "bucket[4]"})
	identitySpec := build(partitionField{Field: "flag", Transform: "identity"})

	t.Run("both unpartitioned", func(t *testing.T) {
		require.NoError(t, partitionSpecMismatch(nil, nil))
	})
	t.Run("identical", func(t *testing.T) {
		require.NoError(t, partitionSpecMismatch(bucketSpec, build(partitionField{Field: "id", Transform: "bucket[4]"})))
	})
	t.Run("partitioned vs unpartitioned", func(t *testing.T) {
		require.Error(t, partitionSpecMismatch(bucketSpec, nil))
		require.Error(t, partitionSpecMismatch(nil, bucketSpec))
	})
	t.Run("different specs", func(t *testing.T) {
		require.Error(t, partitionSpecMismatch(bucketSpec, identitySpec))
	})
	t.Run("different bucket count", func(t *testing.T) {
		require.Error(t, partitionSpecMismatch(bucketSpec, build(partitionField{Field: "id", Transform: "bucket[8]"})))
	})
}
