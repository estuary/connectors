package connector

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestGetHighestFieldID(t *testing.T) {
	originalFields := []iceberg.NestedField{
		{ID: 1, Name: "firstKey", Required: true, Type: iceberg.Int64Type{}},
		{ID: 2, Name: "secondKey", Required: true, Type: iceberg.StringType{}},
		{ID: 3, Name: "reqVal", Required: true, Type: iceberg.StringType{}},
		{ID: 4, Name: "optVal", Required: false, Type: iceberg.BooleanType{}},
		{ID: 5, Name: "thirdVal", Required: true, Type: iceberg.DecimalTypeOf(38, 0)},
		{ID: 6, Name: "fourthVal", Required: true, Type: &iceberg.ListType{
			ElementID: 7,
			Element:   iceberg.StringType{},
		}},
	}

	originalSchema := iceberg.NewSchemaWithIdentifiers(1, []int{1, 2}, originalFields...)
	require.NotEqual(t, originalSchema.HighestFieldID(), getHighestFieldID(originalSchema))
	require.Equal(t, 7, getHighestFieldID(originalSchema))
}
