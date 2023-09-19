package connector

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
)

type binding struct {
	target         sql.Table
	loadQuerySQL   string
	storeInsertSQL string
	storeUpdateSQL string

	loadFile      *stagedFile
	storeFile     *stagedFile
	tempTableName string
}

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
	if document, ok := v[1].(string); !ok {
		return fmt.Errorf("value[1] wrong type %T expecting string", v[0])
	} else {
		bd.Document = json.RawMessage(document)
	}

	return nil
}
