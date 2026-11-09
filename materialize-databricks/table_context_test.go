package connector

import (
	"testing"
	"time"

	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
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
		"numFiles":                     int64(2),
		"sizeInBytes":                  int64(40651),
		"clusteringColumns":            []string{"`my id`", "created_at"},
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

	require.Equal(t, log.Fields{
		"historySince": "2026-10-08T00:00:00Z",
		"history": map[string]map[string]map[string]any{
			"connector": {
				"MERGE":     {"count": int64(500), "last": "2026-11-06T12:00:00Z", "dvsAdded": int64(1833), "dvsUpdated": int64(1824)},
				"COPY INTO": {"count": int64(2), "last": "2026-11-02T00:00:00Z", "dvsRemoved": int64(1)},
			},
			"predictiveOptimization": {"OPTIMIZE": {"count": int64(40), "last": "2026-11-06T11:30:00Z", "dvsRemoved": int64(10)}},
			"job":                    {"OPTIMIZE": {"count": int64(4), "last": "2026-11-05T00:00:00Z", "dvsRemoved": int64(3)}},
			"user":                   {"REORG": {"count": int64(1), "last": "2026-10-20T00:00:00Z"}},
		},
	}, tableHistoryFields([]historyGroup{
		{operation: "WRITE", actor: "connector", count: 9, first: at("2026-10-08T00:00:00Z"), last: at("2026-11-06T00:00:00Z")},
		{operation: "MERGE", actor: "connector", count: 500, first: at("2026-10-09T00:00:00Z"), last: at("2026-11-06T12:00:00Z"), dvsAdded: 1833, dvsUpdated: 1824},
		{operation: "OPTIMIZE", actor: "predictiveOptimization", count: 40, first: at("2026-10-09T00:00:00Z"), last: at("2026-11-06T11:30:00Z"), dvsRemoved: 10},
		{operation: "OPTIMIZE", actor: "job", count: 4, first: at("2026-10-12T00:00:00Z"), last: at("2026-11-05T00:00:00Z"), dvsRemoved: 3},
		{operation: "REORG", actor: "user", count: 1, first: at("2026-10-20T00:00:00Z"), last: at("2026-10-20T00:00:00Z")},
		// Another operation is summarized only if it changed deletion vectors.
		{operation: "COPY INTO", actor: "connector", count: 2, first: at("2026-10-10T00:00:00Z"), last: at("2026-11-02T00:00:00Z"), dvsRemoved: 1},
		{operation: "VACUUM END", actor: "predictiveOptimization", count: 4, first: at("2026-10-09T00:00:00Z"), last: at("2026-11-02T00:00:00Z")},
	}))

	require.Empty(t, tableHistoryFields(nil))
}

func TestLoggedProperties(t *testing.T) {
	require.Equal(t, map[string]string{
		"clusteringColumns":                                `[["my id"]]`,
		"delta.enableDeletionVectors":                      "true",
		"delta.minReaderVersion":                           "3",
		"delta.minWriterVersion":                           "7",
		"delta.feature.deletionVectors":                    "supported",
		"delta.parquet.format.version":                     "2.12.0",
		"delta.parquet.compression.codec":                  "zstd",
		"delta.rowTracking.materializedRowIdColumnName":    "_row-id-col-625c78f0-ec11-4f0c-9312-6f57e6000fc1",
		"databricks.delta.expressionStats.selectedColumns": "upper(`v-al`)",
	}, loggedProperties(map[string]string{
		"clusteringColumns":                                `[["my id"]]`,
		"delta.enableDeletionVectors":                      "true",
		"delta.minReaderVersion":                           "3",
		"delta.minWriterVersion":                           "7",
		"delta.feature.deletionVectors":                    "supported",
		"delta.parquet.format.version":                     "2.12.0",
		"delta.parquet.compression.codec":                  "zstd",
		"delta.rowTracking.materializedRowIdColumnName":    "_row-id-col-625c78f0-ec11-4f0c-9312-6f57e6000fc1",
		"databricks.delta.expressionStats.selectedColumns": "upper(`v-al`)",
		// The user's own properties are dropped.
		"owner_team": "growth",
		"comment":    "orders",
	}))
}

func TestVersionFields(t *testing.T) {
	fields, err := versionFields(`{"dbr_version":null,"dbsql_version":"2026.43","u_build_hash":"aa28dd26","r_build_hash":"c7377b86"}`)
	require.NoError(t, err)
	require.Equal(t, log.Fields{"dbsqlVersion": "2026.43"}, fields)

	fields, err = versionFields(`{"dbr_version":"17.3.x-photon-scala2.13","dbsql_version":"2026.43"}`)
	require.NoError(t, err)
	require.Equal(t, log.Fields{"dbsqlVersion": "2026.43", "dbrVersion": "17.3.x-photon-scala2.13"}, fields)

	_, err = versionFields("2026.43")
	require.Error(t, err)
}

func TestWarehouseFields(t *testing.T) {
	require.Equal(t, log.Fields{
		"type": dbsql.GetWarehouseResponseWarehouseType("PRO"), "serverless": true, "clusterSize": "2X-Small", "photon": true,
		"minClusters": 1, "maxClusters": 2, "channel": dbsql.ChannelName("CHANNEL_NAME_CURRENT"), "channelDbsqlVersion": "2026.43",
	}, warehouseFields(&dbsql.GetWarehouseResponse{
		Name:                    "Serverless Starter Warehouse",
		WarehouseType:           "PRO",
		EnableServerlessCompute: true,
		ClusterSize:             "2X-Small",
		EnablePhoton:            true,
		MinNumClusters:          1,
		MaxNumClusters:          2,
		Channel:                 &dbsql.Channel{Name: "CHANNEL_NAME_CURRENT", DbsqlVersion: "2026.43"},
	}))

	require.Equal(t, log.Fields{
		"type": dbsql.GetWarehouseResponseWarehouseType("CLASSIC"), "serverless": false, "clusterSize": "Small", "photon": false,
		"minClusters": 1, "maxClusters": 1,
	}, warehouseFields(&dbsql.GetWarehouseResponse{WarehouseType: "CLASSIC", ClusterSize: "Small", MinNumClusters: 1, MaxNumClusters: 1}))
}

// fakeQueries returns its fields, failing the named parts, and counts calls.
