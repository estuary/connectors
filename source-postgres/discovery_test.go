package main

import (
	"context"
	"fmt"
	"regexp"
	"testing"
)

func TestDiscoveryComplex(t *testing.T) {
	var tb, ctx = postgresTestBackend(t), context.Background()

	const uniqueString = "cheap_oxygenation"
	var tableName = tb.CreateTable(ctx, t, uniqueString, `(
		k1             INTEGER NOT NULL,
		foo            TEXT,
		real_          REAL NOT NULL,
		"Bounded Text" VARCHAR(255),
		k2             TEXT,
		doc            JSON,
		"doc/bin"      JSONB NOT NULL,
		PRIMARY KEY(k2, k1)
	)`)
	tb.Query(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.foo IS 'This is a text field!';", tableName))
	tb.Query(ctx, t, fmt.Sprintf("COMMENT ON COLUMN %s.k1 IS 'I think this is a key ?';", tableName))

	tb.CaptureSpec(t).VerifyDiscover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueString)))
}
