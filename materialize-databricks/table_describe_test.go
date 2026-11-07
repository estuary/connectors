package connector

import (
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestExtendedFields(t *testing.T) {
	require.Equal(t, log.Fields{
		"predictiveOptimization": "ENABLE (inherited from METASTORE metastore_aws_us_east_2)",
		"statistics":             "9606279675960 bytes, 1500000000 rows",
	}, extendedFields([][2]string{
		// Columns of the same names as detailed rows are ignored.
		{"Statistics", "string"},
		{"id", "string"},
		{"# Clustering Information", ""},
		{"# col_name", "data_type"},
		{"id", "string"},
		{"", ""},
		{"# Detailed Table Information", ""},
		{"Catalog", "main"},
		{"Statistics", "9606279675960 bytes, 1500000000 rows"},
		{"Type", "MANAGED"},
		{"Predictive Optimization", "ENABLE (inherited from METASTORE metastore_aws_us_east_2)"},
		{"Table Properties", "[delta.enableDeletionVectors=true]"},
	}))

	require.Empty(t, extendedFields([][2]string{{"Statistics", "string"}}))
}

func TestDetailFields(t *testing.T) {
	fields, err := detailFields(map[string]any{
		"format":            "delta",
		"partitionColumns":  "[]",
		"clusteringColumns": "[\"`my id`\",\"created_at\"]",
		"numFiles":          int64(2),
		"sizeInBytes":       int64(40651),
		"properties": `{"delta.parquet.compression.codec":"zstd","delta.enableChangeDataFeed":"true","delta.enableDeletionVectors":"true",` +
			`"delta.enableRowTracking":"true","delta.checkpointPolicy":"v2","delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"2",` +
			`"delta.autoOptimize.optimizeWrite":"true","delta.targetFileSize":"128mb","delta.rowTracking.materializedRowIdColumnName":"_row-id-col-x"}`,
		"tableFeatures": `["changeDataFeed","clustering","deletionVectors"]`,
		"statistics":    `{"numRowsDeletedByDeletionVectors":910,"numDeletionVectors":1}`,
		"clusterByAuto": false,
	})
	require.NoError(t, err)
	require.Equal(t, log.Fields{
		"numFiles":          int64(2),
		"sizeInBytes":       int64(40651),
		"clusteringColumns": []string{"`my id`", "created_at"},
		"features":          []string{"changeDataFeed", "clustering", "deletionVectors"},
		"properties": map[string]string{
			"delta.enableChangeDataFeed":       "true",
			"delta.enableDeletionVectors":      "true",
			"delta.enableRowTracking":          "true",
			"delta.checkpointPolicy":           "v2",
			"delta.columnMapping.mode":         "name",
			"delta.autoOptimize.optimizeWrite": "true",
			"delta.targetFileSize":             "128mb",
		},
		"deletionVectors":              int64(1),
		"rowsDeletedByDeletionVectors": int64(910),
	}, fields)

	t.Run("partitioned, auto clustering, missing columns", func(t *testing.T) {
		fields, err := detailFields(map[string]any{
			"partitionColumns":  `["day"]`,
			"clusteringColumns": nil,
			"clusterByAuto":     true,
		})
		require.NoError(t, err)
		require.Equal(t, log.Fields{"partitionColumns": []string{"day"}, "clusterByAuto": true}, fields)
	})

	t.Run("keeps the columns that parse", func(t *testing.T) {
		fields, err := detailFields(map[string]any{"numFiles": int64(3), "statistics": "{"})
		require.ErrorContains(t, err, "statistics")
		require.Equal(t, log.Fields{"numFiles": int64(3)}, fields)
	})
}

func TestTableHistoryFields(t *testing.T) {
	var at = func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return ts
	}
	const sp = "f27142bf-843f-448f-b5d7-81e152850b03"

	require.Equal(t, log.Fields{
		"historySince": "2026-10-08T00:00:00Z",
		"history": map[string]map[string]any{
			"MERGE by connector":           {"count": int64(501), "last": "2026-11-06T12:00:00Z", "dvsAdded": int64(1833), "dvsUpdated": int64(1824)},
			"OPTIMIZE by servicePrincipal": {"count": int64(40), "last": "2026-11-06T11:30:00Z", "dvsRemoved": int64(10)},
			"OPTIMIZE by user":             {"count": int64(3), "last": "2026-11-01T00:00:00Z", "dvsRemoved": int64(7)},
			"REORG by user":                {"count": int64(1), "last": "2026-10-20T00:00:00Z"},
			"COPY INTO by connector":       {"count": int64(2), "last": "2026-11-02T00:00:00Z", "dvsRemoved": int64(1)},
		},
	}, tableHistoryFields([]historyGroup{
		{operation: "WRITE", userName: "me@example.com", self: true, count: 9, first: at("2026-10-08T00:00:00Z"), last: at("2026-11-06T00:00:00Z")},
		{operation: "MERGE", userName: "me@example.com", self: true, count: 500, first: at("2026-10-09T00:00:00Z"), last: at("2026-11-06T12:00:00Z"), dvsAdded: 1833, dvsUpdated: 1824},
		{operation: "OPTIMIZE", userName: sp, count: 40, first: at("2026-10-09T00:00:00Z"), last: at("2026-11-06T11:30:00Z"), dvsRemoved: 10},
		// Users are summed together.
		{operation: "OPTIMIZE", userName: "a@example.com", count: 1, first: at("2026-10-10T00:00:00Z"), last: at("2026-11-01T00:00:00Z"), dvsRemoved: 7},
		{operation: "OPTIMIZE", userName: "b@example.com", count: 2, first: at("2026-10-10T00:00:00Z"), last: at("2026-10-15T00:00:00Z")},
		{operation: "REORG", userName: "a@example.com", count: 1, first: at("2026-10-20T00:00:00Z"), last: at("2026-10-20T00:00:00Z")},
		// Another operation is summarized only if it changed deletion vectors.
		{operation: "COPY INTO", userName: "me@example.com", self: true, count: 2, first: at("2026-10-10T00:00:00Z"), last: at("2026-11-02T00:00:00Z"), dvsRemoved: 1},
		{operation: "VACUUM END", userName: sp, count: 4, first: at("2026-10-10T00:00:00Z"), last: at("2026-11-02T00:00:00Z")},
		// The connector may run as a service principal.
		{operation: "MERGE", userName: "0a1b2c3d-0000-4000-8000-000000000000", self: true, count: 1, first: at("2026-10-10T00:00:00Z"), last: at("2026-10-10T00:00:00Z")},
	}))

	require.Empty(t, tableHistoryFields(nil))
}
