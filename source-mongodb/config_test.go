package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

func TestShouldBackfill(t *testing.T) {
	type expect struct {
		database   string
		collection string
		want       bool
	}

	for _, tt := range []struct {
		name          string
		skipBackfills string
		expect        []expect
	}{
		{
			name:          "unset backfills everything",
			skipBackfills: "",
			expect: []expect{
				{"db", "one", true},
				{"db", "two", true},
			},
		},
		{
			name:          "wildcard skips everything",
			skipBackfills: "*:*",
			expect: []expect{
				{"db", "one", false},
				{"otherDb", "two", false},
			},
		},
		{
			name:          "single collection",
			skipBackfills: "db:one",
			expect: []expect{
				{"db", "one", false},
				{"db", "two", true},
				{"otherDb", "one", true},
			},
		},
		{
			name:          "comma-separated list",
			skipBackfills: "db:one,otherDb:two",
			expect: []expect{
				{"db", "one", false},
				{"otherDb", "two", false},
				{"db", "two", true},
			},
		},
		{
			// Dots are ordinary characters here, unlike in MongoDB's own
			// 'database.collection' namespace notation.
			name:          "collection names containing dots",
			skipBackfills: "db:system.views",
			expect: []expect{
				{"db", "system.views", false},
				{"db", "system", true},
			},
		},
		{
			// The namespace is compared as one string and never parsed, so a
			// name containing the ':' separator still matches.
			name:          "names containing the separator",
			skipBackfills: "db:logs:2024",
			expect: []expect{
				{"db", "logs:2024", false},
				{"db", "logs", true},
			},
		},
		{
			// Known limitation of the ':' separator: because a database name may
			// contain a ':' too, two distinct collections can format to the same
			// configuration string and are therefore indistinguishable here. Both
			// are skipped by the single entry below. This requires a ':' in a
			// database name, which is legal but pathological.
			name:          "ambiguous names collide",
			skipBackfills: "pro:beDb:orders",
			expect: []expect{
				{"pro:beDb", "orders", false},
				{"pro", "beDb:orders", false},
			},
		},
		{
			name:          "matching is case-sensitive",
			skipBackfills: "db:Users",
			expect: []expect{
				{"db", "Users", false},
				{"db", "users", true},
				{"DB", "Users", true},
			},
		},
		{
			// No character within an element is special, so a wildcard matches
			// only a collection genuinely named that way.
			name:          "wildcards are literal",
			skipBackfills: "db:*,other:logs*",
			expect: []expect{
				{"db", "*", false},
				{"other", "logs*", false},
				{"db", "one", true},
				{"other", "logs", true},
				{"other", "logs2024", true},
			},
		},
		{
			name:          "wildcard within a list matches nothing",
			skipBackfills: "db:one,*:*",
			expect: []expect{
				{"db", "one", false},
				{"db", "two", true},
			},
		},
		{
			name:          "whitespace around list elements is ignored",
			skipBackfills: "db:one, otherDb:two ,\tdb:three",
			expect: []expect{
				{"db", "one", false},
				{"otherDb", "two", false},
				{"db", "three", false},
				{"db", "two", true},
			},
		},
		{
			name:          "whitespace around the wildcard is ignored",
			skipBackfills: " *:*\n",
			expect: []expect{
				{"db", "one", false},
				{"otherDb", "two", false},
			},
		},
		{
			name:          "whitespace within an element is preserved",
			skipBackfills: "db: one",
			expect: []expect{
				{"db", " one", false},
				{"db", "one", true},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config{Advanced: advancedConfig{SkipBackfills: tt.skipBackfills}}
			for _, e := range tt.expect {
				require.Equalf(t, e.want, cfg.shouldBackfill(e.database, e.collection),
					"database %q collection %q", e.database, e.collection)
			}
		})
	}
}

func TestValidateSkipBackfills(t *testing.T) {
	for _, tt := range []struct {
		name          string
		skipBackfills string
		wantErr       string
	}{
		{name: "unset", skipBackfills: ""},
		{name: "wildcard", skipBackfills: "*:*"},
		{name: "single collection", skipBackfills: "db:one"},
		{name: "list", skipBackfills: "db:one,otherDb:two"},
		{
			// Accepted, though it matches only a collection genuinely named '*'.
			name:          "per-database wildcard is not special",
			skipBackfills: "db:*",
		},
		{
			// A ':' in a name is accepted: the value is never parsed, only
			// compared, so there is no ambiguity to reject.
			name:          "names containing the separator",
			skipBackfills: "pro:beDb:orders",
		},
		{
			name:          "missing separator",
			skipBackfills: "one",
			wantErr:       `collection "one" must be formatted as "database_name:collection"`,
		},
		{
			name:          "missing separator within a list",
			skipBackfills: "db:one,two",
			wantErr:       `collection "two" must be formatted as "database_name:collection"`,
		},
		{
			// The wildcard is only meaningful as the entire value, so within a
			// list it is just another element which needs a separator.
			name:          "bare wildcard",
			skipBackfills: "*",
			wantErr:       `collection "*" must be formatted as "database_name:collection"`,
		},
		{
			name:          "whitespace around list elements",
			skipBackfills: "db:one, otherDb:two",
		},
		{
			name:          "whitespace around the wildcard",
			skipBackfills: " *:*\n",
		},
		{
			// A value of only whitespace carries no namespaces and is treated
			// as unset rather than as one malformed entry.
			name:          "whitespace-only value",
			skipBackfills: "   ",
		},
		{
			// The offending element is reported trimmed, so the message names
			// the collection the user typed rather than " two".
			name:          "missing separator after whitespace",
			skipBackfills: "db:one, two",
			wantErr:       `collection "two" must be formatted as "database_name:collection"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config{
				Address:  "mongodb://localhost:27017",
				User:     "user",
				Password: "password",
				Advanced: advancedConfig{SkipBackfills: tt.skipBackfills},
			}

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pc.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}
