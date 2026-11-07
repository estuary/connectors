package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// tableDescriber describes a binding's table for the profiles of its slow
// queries: the layout, deletion vectors and maintenance that decide how much
// of the table a query reads.
type tableDescriber struct {
	openDB      func() (*stdsql.DB, error)
	identifiers []string
}

func (d *tableDescriber) DescribeTable(ctx context.Context, binding int) (log.Fields, error) {
	db, err := d.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var identifier = d.identifiers[binding]
	var out = make(log.Fields)
	var errs []error
	for _, describe := range []func(context.Context, *stdsql.DB, string) (log.Fields, error){
		describeExtended, describeDetail, describeHistory,
	} {
		fields, err := describe(ctx, db, identifier)
		maps.Copy(out, fields)
		errs = append(errs, err)
	}
	return out, errors.Join(errs...)
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

// loggedProperties are the table properties that bear on how a query reads
// the table. Properties with loggedPropertyPrefix are logged too.
var loggedProperties = []string{
	"delta.enableDeletionVectors",
	"delta.enableChangeDataFeed",
	"delta.enableRowTracking",
	"delta.columnMapping.mode",
	"delta.targetFileSize",
	"delta.tuneFileSizesForRewrites",
	"delta.checkpointPolicy",
	"delta.dataSkippingNumIndexedCols",
	"delta.dataSkippingStatsColumns",
}

const loggedPropertyPrefix = "delta.autoOptimize."

// detailFields picks the table's files, clustering, features, deletion
// vectors and a selection of its properties from the row of DESCRIBE DETAIL,
// whose array and map columns arrive as JSON.
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

	var clustering, partitioning, features []string
	if decode("clusteringColumns", &clustering) && len(clustering) > 0 {
		out["clusteringColumns"] = clustering
	}
	if decode("partitionColumns", &partitioning) && len(partitioning) > 0 {
		out["partitionColumns"] = partitioning
	}
	if decode("tableFeatures", &features) {
		out["features"] = features
	}

	var properties map[string]string
	if decode("properties", &properties) {
		var logged = make(map[string]string)
		for k, v := range properties {
			if slices.Contains(loggedProperties, k) || strings.HasPrefix(k, loggedPropertyPrefix) {
				logged[k] = v
			}
		}
		out["properties"] = logged
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

// historyGroup aggregates the table's history entries of one operation by one
// user.
type historyGroup struct {
	operation   string
	userName    string
	self        bool
	count       int64
	first, last time.Time
	dvsAdded    int64
	dvsUpdated  int64
	dvsRemoved  int64
}

// historyQuery aggregates the retained history, about 30 days, by operation
// and user. MERGE records its deletion vectors as numTargetDeletionVectors*,
// other operations as numDeletionVectors*.
const historyQuery = `SELECT operation, userName, coalesce(userName = current_user(), false), count(*), min(timestamp), max(timestamp),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsAdded'], operationMetrics['numDeletionVectorsAdded']) AS BIGINT)), 0),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsUpdated'], operationMetrics['numDeletionVectorsUpdated']) AS BIGINT)), 0),
  coalesce(sum(CAST(coalesce(operationMetrics['numTargetDeletionVectorsRemoved'], operationMetrics['numDeletionVectorsRemoved']) AS BIGINT)), 0)
FROM (DESCRIBE HISTORY %s) GROUP BY ALL`

func describeHistory(ctx context.Context, db *stdsql.DB, identifier string) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(historyQuery, identifier))
	if err != nil {
		return nil, fmt.Errorf("describe history: %w", err)
	}
	defer rows.Close()

	var groups []historyGroup
	for rows.Next() {
		var g historyGroup
		var userName stdsql.NullString
		if err := rows.Scan(&g.operation, &userName, &g.self, &g.count, &g.first, &g.last, &g.dvsAdded, &g.dvsUpdated, &g.dvsRemoved); err != nil {
			return nil, fmt.Errorf("describe history: %w", err)
		}
		g.userName = userName.String
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("describe history: %w", err)
	}
	return tableHistoryFields(groups), nil
}

// historyOperations are the operations that add or remove deletion vectors.
var historyOperations = []string{"MERGE", "UPDATE", "DELETE", "OPTIMIZE", "REORG"}

// servicePrincipalName matches the application ID a service principal, such as
// predictive optimization's, records as its userName.
var servicePrincipalName = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// tableHistoryFields summarizes the history groups of the operations that bear on
// deletion vectors, by operation and by who ran it: the connector, a service
// principal or a user.
func tableHistoryFields(groups []historyGroup) log.Fields {
	var since time.Time
	var summary = make(map[string]map[string]any)
	for _, g := range groups {
		if since.IsZero() || g.first.Before(since) {
			since = g.first
		}
		if !slices.Contains(historyOperations, g.operation) && g.dvsAdded+g.dvsUpdated+g.dvsRemoved == 0 {
			continue
		}

		var by = "user"
		if g.self {
			by = "connector"
		} else if servicePrincipalName.MatchString(g.userName) {
			by = "servicePrincipal"
		}
		var key = g.operation + " by " + by
		var s = summary[key]
		if s == nil {
			s = map[string]any{"count": int64(0), "last": g.last.UTC().Format(time.RFC3339)}
			summary[key] = s
		}
		s["count"] = s["count"].(int64) + g.count
		if last := g.last.UTC().Format(time.RFC3339); last > s["last"].(string) {
			s["last"] = last
		}
		for name, n := range map[string]int64{"dvsAdded": g.dvsAdded, "dvsUpdated": g.dvsUpdated, "dvsRemoved": g.dvsRemoved} {
			if n != 0 {
				var prev, _ = s[name].(int64)
				s[name] = prev + n
			}
		}
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
