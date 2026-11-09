package connector

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// failingLoadCatalog fails every LoadTable.
type failingLoadCatalog struct {
	icebergcatalog.Catalog
	err   error
	calls int
}

func (c *failingLoadCatalog) LoadTable(context.Context, icebergtable.Identifier) (*icebergtable.Table, error) {
	c.calls++
	return nil, c.err
}

func TestAppendFilesRetries(t *testing.T) {
	var loadErr = errors.New("connection reset")

	t.Run("gives up after three attempts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fake = &failingLoadCatalog{err: loadErr}
			var c = &catalog{cat: fake}
			var start = time.Now()
			var err = c.appendFiles(t.Context(), "acmeCo/m", []string{"ns", "t"}, []string{"s3://b/f"}, "", "next")
			require.ErrorIs(t, err, loadErr)
			require.ErrorContains(t, err, "after 3 attempts")
			require.Equal(t, 3, fake.calls)
			require.GreaterOrEqual(t, time.Since(start), 3*time.Second)
		})
	})

	t.Run("stops waiting when the context is done", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var fake = &failingLoadCatalog{err: loadErr}
			var c = &catalog{cat: fake}
			var ctx, cancel = context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			var err = c.appendFiles(ctx, "acmeCo/m", []string{"ns", "t"}, []string{"s3://b/f"}, "", "next")
			require.ErrorIs(t, err, loadErr)
			require.Equal(t, 1, fake.calls)
		})
	})
}
