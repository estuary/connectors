package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
)

type binding struct {
	target            sql.Table
	nullFieldsToStrip []string
	storeInsertSQL    string
	truncateSQL       string

	loadSchema       bigquery.Schema
	storeSchema      bigquery.Schema
	tempTableName    string
	mustMerge        bool
	loadMergeBounds  *sql.MergeBoundsBuilder
	storeMergeBounds *sql.MergeBoundsBuilder
	// timestampKeys are the indices of the TIMESTAMP key columns.
	timestampKeys []int
}

func isTimestampColumn(col sql.Column) bool {
	return col.BareDDL == "TIMESTAMP"
}

func timestampKeys(target sql.Table) []int {
	var out []int
	for idx, key := range target.Keys {
		if isTimestampColumn(key) {
			out = append(out, idx)
		}
	}
	return out
}

// newMergeBoundsBuilder bounds TIMESTAMP keys exactly, and splits them and
// string keys into ranges at gaps. TIMESTAMP keys must pass through boundsKey.
func newMergeBoundsBuilder(target sql.Table, literal func(any) string) *sql.MergeBoundsBuilder {
	return sql.NewMergeBoundsBuilder(target.Keys, literal,
		sql.WithExactDatetimeBounds(isTimestampColumn),
		sql.WithDatetimeRanges(maxBoundRanges),
		sql.WithStringRanges(maxBoundRanges),
	)
}

// boundsKey returns key with its TIMESTAMP values floored to microseconds, for
// tracking merge bounds. BigQuery stores a timestamp floored to microseconds
// and rejects a literal with finer precision.
func (b *binding) boundsKey(key []any) []any {
	if len(b.timestampKeys) == 0 {
		return key
	}
	key = append([]any(nil), key...)
	for _, idx := range b.timestampKeys {
		if ts, ok := sql.ParseDatetime(key[idx]); ok {
			key[idx] = ts.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z")
		}
	}
	return key
}

// errNullFlowDocument is returned when a load query reads a row whose
// flow_document column is NULL. The column is added as nullable when a binding
// adopts a table that already held rows, and nothing backfills it for them.
//
// A sentinel so this stays distinguishable from a type mismatch without matching
// on message text. Two caveats not worth putting in front of operators: enabling
// no_flow_document takes effect on the next transaction of an existing binding
// (transactor.go selects the load template from the endpoint config alone), and
// its reconstruction yields an explicit null for any root-level column that is
// NULL, which only matters for collections that reduce the root document rather
// than replacing it.
var errNullFlowDocument = errors.New("row has no flow_document value, " +
	"typically because the binding adopted a table that already held data: see https://go.estuary.dev/matff. " +
	"The 'Exclude Flow Document' advanced option makes loads reconstruct the document " +
	"from the table's own columns instead")

// bindingDocument is used by the load operation to fetch binding flow_document values
type bindingDocument struct {
	Binding  int
	Document json.RawMessage
}

// Load implements the bigquery ValueLoader interface for mapping data from the database to this struct
// It requires 2 values, the first must be the binding and the second must be the flow_document
func (bd *bindingDocument) Load(v []bigquery.Value, s bigquery.Schema) error {
	if len(v) != 2 {
		return fmt.Errorf("invalid value count: %d", len(v))
	}
	if binding, ok := v[0].(int64); !ok {
		return fmt.Errorf("value[0] wrong type %T expecting int", v[0])
	} else {
		bd.Binding = int(binding)
	}
	if v[1] == nil {
		return errNullFlowDocument
	} else if document, ok := v[1].(string); !ok {
		return fmt.Errorf("value[1] wrong type %T expecting string", v[1])
	} else {
		bd.Document = json.RawMessage(document)
	}

	return nil
}
