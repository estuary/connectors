package tests

import (
	"testing"

	"github.com/estuary/connectors/go/capture/blackbox"
)

type TestDatabase interface {
	Expand(s string) string
	Exec(t testing.TB, query string)
	// QuietExec(t testing.TB, query string)
	// QueryRow(t testing.TB, query string, dest ...any)
	CreateTable(t testing.TB, name, defs string)
}

type testSetupFunc func(t testing.TB) (TestDatabase, *blackbox.TranscriptCapture)
