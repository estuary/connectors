package main

import (
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
)

func TestBackfillQueryGeneration(t *testing.T) {
	var testCases = []struct {
		name           string
		isPrecise      bool
		keyColumns     []string
		columnTypes    map[string]interface{}
		schemaName     string
		tableName      string
		minBackfillXID string
	}{
		{
			name:        "integer_key_precise",
			isPrecise:   true,
			keyColumns:  []string{"id"},
			columnTypes: map[string]interface{}{"id": "integer"},
			schemaName:  "public",
			tableName:   "users",
		},
		{
			name:        "string_key_imprecise",
			isPrecise:   false,
			keyColumns:  []string{"name"},
			columnTypes: map[string]interface{}{"name": "text"},
			schemaName:  "public",
			tableName:   "users",
		},
		{
			name:        "composite_key",
			isPrecise:   false,
			keyColumns:  []string{"first_name", "last_name"},
			columnTypes: map[string]interface{}{"first_name": "varchar", "last_name": "varchar"},
			schemaName:  "public",
			tableName:   "users",
		},
		{
			name:           "string_key_with_min_xid",
			isPrecise:      false,
			keyColumns:     []string{"name"},
			columnTypes:    map[string]interface{}{"name": "text"},
			schemaName:     "public",
			tableName:      "users",
			minBackfillXID: "12345",
		},
		{
			name:        "quoted_column_name",
			isPrecise:   true,
			keyColumns:  []string{"user-id", "group.name"},
			columnTypes: map[string]interface{}{"user-id": "integer", "group.name": "text"},
			schemaName:  "public",
			tableName:   "special_users",
		},
	}

	var result = new(strings.Builder)
	for _, tc := range testCases {
		var db = &postgresDatabase{
			config: &Config{
				Advanced: advancedConfig{
					MinimumBackfillXID: tc.minBackfillXID,
					BackfillChunkSize:  100,
				},
			},
		}
		var startQuery = db.buildScanQuery(true, tc.isPrecise, tc.keyColumns, tc.columnTypes, tc.schemaName, tc.tableName)
		var subsequentQuery = db.buildScanQuery(false, tc.isPrecise, tc.keyColumns, tc.columnTypes, tc.schemaName, tc.tableName)

		// Add to cumulative result string
		result.WriteString("--- " + tc.name + " ---\n")
		result.WriteString(startQuery + "\n\n")
		result.WriteString(subsequentQuery + "\n\n")
	}
	cupaloy.SnapshotT(t, result.String())
}

func TestKeylessBackfillQueryGeneration(t *testing.T) {
	var testCases = []struct {
		name           string
		minBackfillXID string
	}{
		{
			name: "no_xid_filtering",
		},
		{
			name:           "with_xid_filtering",
			minBackfillXID: "12345",
		},
	}

	var result = new(strings.Builder)
	for _, tc := range testCases {
		var db = &postgresDatabase{
			config: &Config{
				Advanced: advancedConfig{
					MinimumBackfillXID: tc.minBackfillXID,
					BackfillChunkSize:  100,
				},
			},
		}
		var query = db.keylessScanQuery(nil, "public", "users")
		result.WriteString("--- " + tc.name + " ---\n")
		result.WriteString(query + "\n\n")
	}
	cupaloy.SnapshotT(t, result.String())
}
