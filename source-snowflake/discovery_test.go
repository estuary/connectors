package main

import (
	"context"
	"regexp"
	"testing"
)

func TestDiscoveryWithPrimaryKey(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "51816324"
	tb.CreateTable(ctx, t, uniqueID, "(a INTEGER PRIMARY KEY, b TEXT, c REAL NOT NULL, d VARCHAR(255))")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

func TestDiscoveryWithoutPrimaryKey(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "24824096"
	tb.CreateTable(ctx, t, uniqueID, "(a INTEGER, b TEXT, c REAL NOT NULL, d VARCHAR(255))")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}

func TestDiscoveryMultiplePrimaryKeys(t *testing.T) {
	var ctx, tb = context.Background(), snowflakeTestBackend(t)
	var uniqueID = "83519234"
	tb.CreateTable(ctx, t, uniqueID, "(a TEXT, b TEXT, c TEXT, d TEXT, PRIMARY KEY (c, a, b))")
	tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueID))
}
