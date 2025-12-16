package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

// TestOversizedFields verifies that fields exceeding truncateColumnThreshold are truncated.
func TestOversizedFields(t *testing.T) {
	var ctx, cs, control = context.Background(), testCaptureSpec(t), testControlClient(t)
	var tableName, uniqueID = testTableName(t, uniqueTableID(t))
	createTestTable(t, control, tableName, `(id INTEGER PRIMARY KEY, v_text TEXT, v_bytea BYTEA, v_json JSON, v_jsonb JSONB)`)
	cs.Bindings = discoverBindings(ctx, t, cs, regexp.MustCompile(uniqueID))

	setShutdownAfterQuery(t, true)

	var largeText = strings.Repeat("data", (truncateColumnThreshold/4)+1) // One more repeat than we'll accept
	var largeJSON = fmt.Sprintf(`{"text":"%s"}`, largeText)

	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_text ) VALUES (101, '%s')", tableName, largeText))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_bytea) VALUES (102, '%s')", tableName, largeText))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_json ) VALUES (103, '%s')", tableName, largeJSON))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_jsonb) VALUES (104, '%s')", tableName, largeJSON))
	cs.Capture(ctx, t, nil)

	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_text ) VALUES (201, '%s')", tableName, largeText))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_bytea) VALUES (202, '%s')", tableName, largeText))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_json ) VALUES (203, '%s')", tableName, largeJSON))
	executeControlQuery(t, control, fmt.Sprintf("INSERT INTO %s (id, v_jsonb) VALUES (204, '%s')", tableName, largeJSON))
	cs.Capture(ctx, t, nil)

	cupaloy.SnapshotT(t, cs.Summary())
}
