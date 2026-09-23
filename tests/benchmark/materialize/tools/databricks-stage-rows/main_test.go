package main

import "testing"

// Keys must match tests/benchmark/materialize/generate.py's uuid_ordered keys
// so staged rows hit the rows of a table the fixture generator loaded.
func TestOrderedUUIDMatchesGenerator(t *testing.T) {
	for key, want := range map[int64]string{
		0:         "019b76da-a800-7c20-940e-25b3f8659a8a",
		1:         "019b76da-a801-7033-83ae-df98a85ff0d8",
		1_000_000: "019b76e9-ea40-7730-a841-41846a9bbad5",
	} {
		if got := orderedUUID(key); got != want {
			t.Errorf("key %d: got %s, want %s", key, got, want)
		}
	}
}
