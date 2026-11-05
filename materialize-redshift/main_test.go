package connector

import (
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	time.Local = time.UTC
	os.Exit(m.Run())
}
