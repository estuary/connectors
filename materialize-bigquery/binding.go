package main

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
)

type binding struct {
	target            sql.Table
	nullFieldsToStrip []string
	hasMetaClock      bool
	storeInsertSQL    string

	loadSchema       bigquery.Schema
	storeSchema      bigquery.Schema
	tempTableName    string
	mustMerge        bool
	loadMergeBounds  *sql.MergeBoundsBuilder
	storeMergeBounds *sql.MergeBoundsBuilder
}

// bindingDocument is used by the load operation to fetch binding flow_document values
type bindingDocument struct {
	Binding  int
	Document json.RawMessage
	// The no_flow_document load query reports the reconstructed _meta object and
	// the document's publication clock separately, see sql.SpliceMeta.
	RawUUID     *string
	ClockMicros *int64
}

// Load implements the bigquery ValueLoader interface for mapping data from the database to this struct
// It requires 2 values, the first must be the binding and the second must be the flow_document
func (bd *bindingDocument) Load(v []bigquery.Value, s bigquery.Schema) error {
	// The no_flow_document load query adds the reconstructed _meta object and the
	// document's publication clock.
	if len(v) != 2 && len(v) != 4 {
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

	// Reset per-row, since the iterator reuses this struct across rows.
	bd.RawUUID, bd.ClockMicros = nil, nil

	if len(v) == 4 {
		if v[2] != nil {
			raw, ok := v[2].(string)
			if !ok {
				return fmt.Errorf("value[2] wrong type %T expecting string", v[2])
			}
			bd.RawUUID = &raw
		}
		if v[3] != nil {
			clock, ok := v[3].(int64)
			if !ok {
				return fmt.Errorf("value[3] wrong type %T expecting int64", v[3])
			}
			bd.ClockMicros = &clock
		}
	}

	return nil
}
