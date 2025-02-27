package main

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/stretchr/testify/require"
)

func TestProcessLoadObjBody(t *testing.T) {
	data := `1,|{"id":1,"value":"one"}|
2,|{"id":2,"value":"two"}|
3,|{"id":3,"value":"three"}|
`

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	_, err := gzw.Write([]byte(data))
	require.NoError(t, err)
	require.NoError(t, gzw.Close())
	rc := io.NopCloser(&buf)

	var gotBindings []int
	var gotDocs []json.RawMessage

	loaded := func(binding int, doc json.RawMessage) error {
		gotBindings = append(gotBindings, binding)
		gotDocs = append(gotDocs, doc)
		return nil
	}

	require.NoError(t, processLoadObjBody(rc, loaded))
	require.Equal(t, []int{1, 2, 3}, gotBindings)
	require.Equal(t, []json.RawMessage{
		[]byte(`{"id":1,"value":"one"}`),
		[]byte(`{"id":2,"value":"two"}`),
		[]byte(`{"id":3,"value":"three"}`),
	}, gotDocs)
}
