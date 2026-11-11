package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	dbsql "github.com/databricks/databricks-sdk-go/service/sql"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	log "github.com/sirupsen/logrus"
)

type warehousesAPI interface {
	GetById(ctx context.Context, id string) (*dbsql.GetWarehouseResponse, error)
}

// newTableContext reads additional context and debugging information that
// may be useful in troubleshooting slow queries. This is only run for the first
// slow query of each table.
func newTableContext(openDB func() (*stdsql.DB, error), warehouses warehousesAPI, warehouseID string, identifiers []string) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		db, err := openDB()
		if err != nil {
			return nil, err
		}
		defer db.Close()

		var errs []error
		var read = func(into map[string]any, fields log.Fields, err error) {
			maps.Copy(into, fields)
			errs = append(errs, err)
		}

		var table = make(map[string]any)
		for _, describe := range []func(context.Context, *stdsql.DB, string) (log.Fields, error){
			describeExtended, describeDetail, describeProperties, describeHistory,
		} {
			fields, err := describe(ctx, db, identifiers[binding])
			read(table, fields, err)
		}

		var warehouse = make(map[string]any)
		fields, err := describeVersion(ctx, db)
		read(warehouse, fields, err)
		fields, err = describeWarehouse(ctx, warehouses, warehouseID)
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

func describeVersion(ctx context.Context, db *stdsql.DB) (log.Fields, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT to_json(current_version())").Scan(&version); err != nil {
		return nil, fmt.Errorf("current_version: %w", err)
	}
	return versionFields(version)
}

// versionFields picks the DBSQL and runtime versions from current_version().
func versionFields(version string) (log.Fields, error) {
	var v struct {
		DBSQL string `json:"dbsql_version"`
		DBR   string `json:"dbr_version"`
	}
	if err := json.Unmarshal([]byte(version), &v); err != nil {
		return nil, fmt.Errorf("current_version: %w", err)
	}
	var out = make(log.Fields)
	if v.DBSQL != "" {
		out["dbsqlVersion"] = v.DBSQL
	}
	if v.DBR != "" {
		out["dbrVersion"] = v.DBR
	}
	return out, nil
}

// describeWarehouse reads the warehouse's configuration, which credentials may
// not be allowed to.
func describeWarehouse(ctx context.Context, warehouses warehousesAPI, id string) (log.Fields, error) {
	res, err := warehouses.GetById(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("getting warehouse %s: %w", id, err)
	}
	return warehouseFields(res), nil
}

func warehouseFields(w *dbsql.GetWarehouseResponse) log.Fields {
	var out = log.Fields{
		"type":        w.WarehouseType,
		"serverless":  w.EnableServerlessCompute,
		"clusterSize": w.ClusterSize,
		"photon":      w.EnablePhoton,
		"minClusters": w.MinNumClusters,
		"maxClusters": w.MaxNumClusters,
	}
	if c := w.Channel; c != nil {
		out["channel"] = c.Name
		if c.DbsqlVersion != "" {
			out["channelDbsqlVersion"] = c.DbsqlVersion
		}
	}
	return out
}

// describeExtended reads what only DESCRIBE TABLE EXTENDED shows, which can't
// be queried as a subquery.
func describeExtended(ctx context.Context, db *stdsql.DB, identifier string) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, "DESCRIBE TABLE EXTENDED "+identifier)
	if err != nil {
		return nil, fmt.Errorf("describe table extended: %w", err)
	}
	defer rows.Close()

	var described [][2]string
	for rows.Next() {
		var name, value, comment stdsql.NullString
		if err := rows.Scan(&name, &value, &comment); err != nil {
			return nil, fmt.Errorf("describe table extended: %w", err)
		}
		described = append(described, [2]string{name.String, value.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("describe table extended: %w", err)
	}
	return extendedFields(described), nil
}

// extendedFields picks the table's predictive optimization setting and
// statistics from the rows of DESCRIBE TABLE EXTENDED. They're only read from
// the detailed section, which follows any columns of the same names.
func extendedFields(described [][2]string) log.Fields {
	var out = make(log.Fields)
	var detailed bool
	for _, row := range described {
		if row[0] == "# Detailed Table Information" {
			detailed = true
		} else if !detailed {
			continue
		}
		switch row[0] {
		case "Predictive Optimization":
			out["predictiveOptimization"] = row[1]
		case "Statistics":
			out["statistics"] = row[1]
		}
	}
	return out
}

func describeDetail(ctx context.Context, db *stdsql.DB, identifier string) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, "DESCRIBE DETAIL "+identifier)
	if err != nil {
		return nil, fmt.Errorf("describe detail: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("describe detail: %w", err)
	}
	var detail = make(map[string]any)
	if rows.Next() {
		var values = make([]any, len(columns))
		var ptrs = make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("describe detail: %w", err)
		}
		for i, c := range columns {
			detail[c] = values[i]
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("describe detail: %w", err)
	}
	return detailFields(detail)
}

// detailFields picks the table's files, clustering and deletion vectors from
// the row of DESCRIBE DETAIL, whose array and map columns arrive as JSON.
func detailFields(detail map[string]any) (log.Fields, error) {
	var out = make(log.Fields)
	for _, c := range []string{"numFiles", "sizeInBytes"} {
		if v, ok := detail[c]; ok {
			out[c] = v
		}
	}
	if auto, _ := detail["clusterByAuto"].(bool); auto {
		out["clusterByAuto"] = true
	}

	var errs []error
	var decode = func(column string, into any) bool {
		var raw, ok = detail[column].(string)
		if !ok {
			return false
		} else if err := json.Unmarshal([]byte(raw), into); err != nil {
			errs = append(errs, fmt.Errorf("describe detail %s: %w", column, err))
			return false
		}
		return true
	}

	var clustering, partitioning []string
	if decode("clusteringColumns", &clustering) && len(clustering) > 0 {
		out["clusteringColumns"] = clustering
	}
	if decode("partitionColumns", &partitioning) && len(partitioning) > 0 {
		out["partitionColumns"] = partitioning
	}

	var stats struct {
		NumDeletionVectors              *int64 `json:"numDeletionVectors"`
		NumRowsDeletedByDeletionVectors *int64 `json:"numRowsDeletedByDeletionVectors"`
	}
	if decode("statistics", &stats) {
		if stats.NumDeletionVectors != nil {
			out["deletionVectors"] = *stats.NumDeletionVectors
		}
		if stats.NumRowsDeletedByDeletionVectors != nil {
			out["rowsDeletedByDeletionVectors"] = *stats.NumRowsDeletedByDeletionVectors
		}
	}

	return out, errors.Join(errs...)
}

// describeProperties reads the table's properties. Unlike DESCRIBE DETAIL's,
// SHOW TBLPROPERTIES includes the protocol versions and enabled features.
func describeProperties(ctx context.Context, db *stdsql.DB, identifier string) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, "SHOW TBLPROPERTIES "+identifier)
	if err != nil {
		return nil, fmt.Errorf("show tblproperties: %w", err)
	}
	defer rows.Close()

	var properties = make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, fmt.Errorf("show tblproperties: %w", err)
		}
		properties[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("show tblproperties: %w", err)
	}
	return log.Fields{"properties": loggedProperties(properties)}, nil
}

// loggedPropertyPrefixes are the system properties' namespaces. Other
// properties are the user's own, and may say anything.
var loggedPropertyPrefixes = []string{"delta.", "databricks."}

// loggedProperties are the system properties of a table, plus its clustering
// columns.
func loggedProperties(properties map[string]string) map[string]string {
	var out = make(map[string]string)
	for k, v := range properties {
		if k == "clusteringColumns" || slices.ContainsFunc(loggedPropertyPrefixes, func(p string) bool { return strings.HasPrefix(k, p) }) {
			out[k] = v
		}
	}
	return out
}

func describeHistory(ctx context.Context, db *stdsql.DB, identifier string) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(historyQuery, identifier))
	if err != nil {
		return nil, fmt.Errorf("describe history: %w", err)
	}
	defer rows.Close()

	var groups []historyGroup
	for rows.Next() {
		var g historyGroup
		if err := rows.Scan(&g.operation, &g.actor, &g.count, &g.first, &g.last, &g.dvsAdded, &g.dvsUpdated, &g.dvsRemoved); err != nil {
			return nil, fmt.Errorf("describe history: %w", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("describe history: %w", err)
	}
	return tableHistoryFields(groups), nil
}

// historyQuery aggregates 30 days of retained history by operation
// and by who ran it, without reading out any user's name: the connector,
// predictive optimization, another job, or a user. MERGE records its
// deletion vectors as numTargetDeletionVectors*, other operations
// as numDeletionVectors*.
const historyQuery = `SELECT operation,
  CASE WHEN userName = current_user() THEN 'connector'
    WHEN startswith(job.jobName, 'Predictive Optimization Job-') THEN 'predictiveOptimization'
    WHEN job IS NOT NULL THEN 'job'
    ELSE 'user' END,
  count(*), min(timestamp), max(timestamp),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsAdded'], operationMetrics['numDeletionVectorsAdded']) AS BIGINT)), 0),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsUpdated'], operationMetrics['numDeletionVectorsUpdated']) AS BIGINT)), 0),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsRemoved'], operationMetrics['numDeletionVectorsRemoved']) AS BIGINT)), 0)
FROM (DESCRIBE HISTORY %s) GROUP BY ALL`

// historyGroup aggregates the history entries of one operation by one actor.
type historyGroup struct {
	operation   string
	actor       string
	count       int64
	first, last time.Time
	dvsAdded    int64
	dvsUpdated  int64
	dvsRemoved  int64
}

// historyOperations are the operations that add or remove deletion vectors.
var historyOperations = []string{"MERGE", "UPDATE", "DELETE", "OPTIMIZE", "REORG"}

// tableHistoryFields summarizes the history groups of the operations that bear
// on deletion vectors, by actor and then by operation.
func tableHistoryFields(groups []historyGroup) log.Fields {
	var since time.Time
	var summary = make(map[string]map[string]map[string]any)
	for _, g := range groups {
		if since.IsZero() || g.first.Before(since) {
			since = g.first
		}
		if !slices.Contains(historyOperations, g.operation) && g.dvsAdded+g.dvsUpdated+g.dvsRemoved == 0 {
			continue
		}

		var s = map[string]any{"count": g.count, "last": g.last.UTC().Format(time.RFC3339)}
		for name, n := range map[string]int64{"dvsAdded": g.dvsAdded, "dvsUpdated": g.dvsUpdated, "dvsRemoved": g.dvsRemoved} {
			if n != 0 {
				s[name] = n
			}
		}
		if summary[g.actor] == nil {
			summary[g.actor] = make(map[string]map[string]any)
		}
		summary[g.actor][g.operation] = s
	}

	var out = make(log.Fields)
	if !since.IsZero() {
		out["historySince"] = since.UTC().Format(time.RFC3339)
	}
	if len(summary) > 0 {
		out["history"] = summary
	}
	return out
}
