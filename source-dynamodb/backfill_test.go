package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestResumeKeyRoundTrip(t *testing.T) {
	var av = map[string]types.AttributeValue{
		"string": &types.AttributeValueMemberS{
			Value: "stringValue",
		},
		"decimal": &types.AttributeValueMemberN{
			Value: "123.45",
		},
		"integer": &types.AttributeValueMemberN{
			Value: "123",
		},
		"binary": &types.AttributeValueMemberB{
			Value: []byte{1, 2, 3},
		},
	}

	var keyFields = []string{"string", "decimal", "integer", "binary"}

	var enc, err = encodeKey(keyFields, av)
	require.NoError(t, err)

	got, err := decodeKey(keyFields, enc)
	require.NoError(t, err)

	require.Equal(t, av, got)
}
