package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSheetsIDParsing(t *testing.T) {
	id, err := parseSheetsID("https://docs.google.com/spreadsheets/d/1s7S1Abp8kAJEkReV10omef_ETZXKB2vHKPook49HpFk/edit#gid=1649530432")
	require.Nil(t, err)
	require.Equal(t, "1s7S1Abp8kAJEkReV10omef_ETZXKB2vHKPook49HpFk", id)

	_, err = parseSheetsID("https://github.com/estuary/connectors/issues/245")
	require.Regexp(t, "invalid Google Sheets URL: .*", err)
}
