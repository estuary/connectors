package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
)

// newTableContext reads a table's size and clustering key, how well its
// micro-partitions are clustered on its key columns, which the load and merge
// queries prune by, and the warehouse's configuration.
func newTableContext(db *stdsql.DB, bindings []*binding) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		var b = bindings[binding]
		var errs []error
		var read = func(into map[string]any, fields log.Fields, err error) {
			for k, v := range fields {
				into[k] = v
			}
			errs = append(errs, err)
		}

		var table = make(map[string]any)
		fields, err := describeTable(ctx, db, b)
		read(table, fields, err)
		fields, err = describeKeyClustering(ctx, db, b)
		read(table, fields, err)

		var warehouse = make(map[string]any)
		fields, err = describeWarehouse(ctx, db)
		read(warehouse, fields, err)

		var out = make(log.Fields)
		if len(table) > 0 {
			out["table"] = table
		}
		if len(warehouse) > 0 {
			out["warehouse"] = warehouse
		}
		return out, errors.Join(errs...)
	}
}

func describeTable(ctx context.Context, db *stdsql.DB, b *binding) (log.Fields, error) {
	var rows, bytes stdsql.NullInt64
	var clusteringKey, autoClustering, transient stdsql.NullString
	if err := db.QueryRowContext(ctx, `
SELECT ROW_COUNT, BYTES, CLUSTERING_KEY, AUTO_CLUSTERING_ON, IS_TRANSIENT
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		b.target.InfoLocation.TableSchema, b.target.InfoLocation.TableName,
	).Scan(&rows, &bytes, &clusteringKey, &autoClustering, &transient); err != nil {
		return nil, fmt.Errorf("information_schema.tables: %w", err)
	}

	var out = log.Fields{"rows": rows.Int64, "bytes": bytes.Int64}
	if clusteringKey.String != "" {
		out["clusteringKey"] = clusteringKey.String
		out["autoClustering"] = autoClustering.String == "YES"
	}
	if transient.String == "YES" {
		out["transient"] = true
	}
	return out, nil
}

// describeKeyClustering reads how the table's micro-partitions overlap on its
// key columns, whether or not it's clustered by them.
func describeKeyClustering(ctx context.Context, db *stdsql.DB, b *binding) (log.Fields, error) {
	var keys = make([]string, len(b.target.Keys))
	for i, key := range b.target.Keys {
		keys[i] = key.Identifier
	}

	var result stdsql.NullString
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT SYSTEM$CLUSTERING_INFORMATION(%s, %s)",
		quoteLiteral(b.target.Identifier), quoteLiteral("("+strings.Join(keys, ", ")+")"))).Scan(&result); err != nil {
		return nil, fmt.Errorf("clustering information: %w", err)
	}
	return keyClusteringFields(result.String)
}

func keyClusteringFields(info string) (log.Fields, error) {
	var v struct {
		Partitions      int64   `json:"total_partition_count"`
		AverageOverlaps float64 `json:"average_overlaps"`
		AverageDepth    float64 `json:"average_depth"`
	}
	if err := json.Unmarshal([]byte(info), &v); err != nil {
		return nil, fmt.Errorf("clustering information: %w", err)
	}
	return log.Fields{
		"partitions":         v.Partitions,
		"keyAverageOverlaps": v.AverageOverlaps,
		"keyAverageDepth":    v.AverageDepth,
	}, nil
}

// warehouseColumns are the columns of SHOW WAREHOUSES that are logged, by the
// names they're logged under.
var warehouseColumns = map[string]string{
	"type":                                "type",
	"size":                                "size",
	"min_cluster_count":                   "minClusters",
	"max_cluster_count":                   "maxClusters",
	"scaling_policy":                      "scalingPolicy",
	"auto_suspend":                        "autoSuspend",
	"enable_query_acceleration":           "queryAcceleration",
	"query_acceleration_max_scale_factor": "queryAccelerationMaxScaleFactor",
	"resource_constraint":                 "resourceConstraint",
}

// describeWarehouse reads the session's warehouse as SHOW WAREHOUSES lists
// it, which the role may not be allowed to see, and the Snowflake version.
func describeWarehouse(ctx context.Context, db *stdsql.DB) (log.Fields, error) {
	var name, version stdsql.NullString
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE(), CURRENT_VERSION()").Scan(&name, &version); err != nil {
		return nil, fmt.Errorf("current warehouse: %w", err)
	}
	var out = log.Fields{"version": version.String}

	rows, err := db.QueryContext(ctx, "SHOW WAREHOUSES LIKE "+quoteLiteral(name.String))
	if err != nil {
		return out, fmt.Errorf("show warehouses: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return out, fmt.Errorf("show warehouses: %w", err)
	}
	for rows.Next() {
		var values = make([]stdsql.NullString, len(columns))
		var ptrs = make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return out, fmt.Errorf("show warehouses: %w", err)
		}

		var shown = make(map[string]string, len(columns))
		for i, c := range columns {
			shown[c] = values[i].String
		}
		// LIKE matches case-insensitively and with wildcards.
		if shown["name"] != name.String {
			continue
		}
		for column, field := range warehouseColumns {
			if v := shown[column]; v != "" {
				out[field] = v
			}
		}
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("show warehouses: %w", err)
	}
	return out, nil
}
