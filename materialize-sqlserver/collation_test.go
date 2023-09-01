package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortCollations(t *testing.T) {
	var collations = []string{
		"Latin1_General_BIN",
		"Latin1_General_BIN2",
		"Latin1_General_100_BIN",
		"Latin1_General_100_BIN2",
		"Latin1_General_100_BIN2_UTF8",
	}
	var expected = []string{
		"Latin1_General_100_BIN2_UTF8",
		"Latin1_General_100_BIN2",
		"Latin1_General_BIN2",
		"Latin1_General_100_BIN",
		"Latin1_General_BIN",
	}
	sortCollations(collations)
	require.Equal(t, expected, collations)
}

func TestSortCollations2(t *testing.T) {
	var collations = []string{
		"Latin1_General_100_BIN",
		"Latin1_General_100_BIN2",
		"Latin1_General_BIN2",
		"Latin1_General_BIN",
	}
	var expected = []string{
		"Latin1_General_100_BIN2",
		"Latin1_General_100_BIN",
		"Latin1_General_BIN2",
		"Latin1_General_BIN",
	}
	sortCollations(collations)
	require.Equal(t, expected, collations)
}
