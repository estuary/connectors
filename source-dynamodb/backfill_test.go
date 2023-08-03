package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

func TestKeyAttributeWrapperRoundTrip(t *testing.T) {
	av := map[string]types.AttributeValue{
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

	wrapper := keyAttributeWrapper{
		inner: av,
	}

	bytes, err := json.MarshalIndent(&wrapper, "", "\t")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(bytes))

	got := keyAttributeWrapper{}
	require.NoError(t, json.Unmarshal(bytes, &got))

	require.Equal(t, av, got.inner)
}
