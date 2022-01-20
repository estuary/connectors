package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	pf "github.com/estuary/protocols/flow"
	sqlDriver "github.com/estuary/protocols/materialize/sql"
)

const tempTableNamePrefix = "flow_temp"

type binding struct {
	name string

	// Variables exclusively used by Load.
	load struct {
		paramsConverter sqlDriver.ParametersConverter
		extDataConfig   *bigquery.ExternalDataConfig
		keysFile        *ExternalDataConnectionFile
		sql             string
		tempTableName   string // The name the external table we be referenced by
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		paramsConverter sqlDriver.ParametersConverter
		extDataConfig   *bigquery.ExternalDataConfig
		mergeFile       *ExternalDataConnectionFile
		sql             string
		tempTableName   string // The name the external table we be referenced by
		hasRootDocument bool   // Whether the root document is included in this binding
	}
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

// newBinding generates the bindings for the spec to the BigQuery table.
func newBinding(generator sqlDriver.Generator, bindingPos int, targetName string, spec *pf.MaterializationSpec_Binding) (*binding, error) {

	var err error
	var b = &binding{
		name: targetName,
	}

	// Generate the table definition for this materialization.
	var tableDef = sqlDriver.TableForMaterialization(targetName, "", generator.IdentifierRenderer, spec)

	// BINDING LOADS: Load is done via query against an external table with primary keys.

	// QueryOnPrimaryKey will return the paramsConverter for the primary keys which is all we need here
	_, b.load.paramsConverter, err = generator.QueryOnPrimaryKey(tableDef)
	if err != nil {
		return nil, fmt.Errorf("building load params converter: %w", err)
	}

	// External Data Config used by this binding. This will be a Google Cloud Storage bucket where we will
	// create a file and then directly reference it as a table in a transaction.
	b.load.extDataConfig = &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		Schema:       make([]*bigquery.FieldSchema, 0),
	}

	var pkJoins []string // How to join the main table to the external table based on keys.

	// We use the sqlDriver.NullableTypeMapping to handle null fields for defining schemas
	// but when using bigquery it does not allow the NOT NULL suffix for external table
	// columns. We need to get a reference to the base type mapper to leverage it's types.
	baseTypeMapper := generator.TypeMappings.(sqlDriver.NullableTypeMapping).Inner

	// Build the query for loading values from bigquery by joining a google cloud storage file
	// based table of primary keys against the database.
	for _, key := range spec.FieldSelection.Keys {
		var col = tableDef.GetColumn(key)

		colType, err := baseTypeMapper.GetColumnType(col)
		if err != nil {
			return nil, err
		}

		b.load.extDataConfig.Schema = append(b.load.extDataConfig.Schema, &bigquery.FieldSchema{
			Name:     identifierSanitizer(col.Name), // Sanitized name without quoting required
			Repeated: false,
			Required: col.NotNull,
			Type:     bigquery.FieldType(colType.SQLType),
		})

		pkJoins = append(pkJoins, fmt.Sprintf("l.%s = r.%s", col.Identifier, col.Identifier))
	}

	b.load.tempTableName = fmt.Sprintf("%s_load_%d", tempTableNamePrefix, bindingPos)

	if spec.DeltaUpdates {
		// We should never issue a load query in delta updates mode, so just in case
		// that happens we'll hopefully produce a reasonable error message with this.
		b.load.sql = `ASSERT false AS 'Load queries should never be executed in Delta Updates mode.'`
	} else {
		// SELECT documents joined with the external table of keys to load
		b.load.sql = fmt.Sprintf(`
		SELECT %d, l.%s
			FROM %s AS l
			JOIN %s AS r
			ON %s
		`,
			bindingPos,
			tableDef.GetColumn(spec.FieldSelection.Document).Identifier,
			tableDef.Identifier,
			b.load.tempTableName,
			strings.Join(pkJoins, " AND "),
		)
	}

	// BINDING STORES: Store is done via upserts using a merge query against an external table.

	// InsertStatement will return the paramsConverter for ALL the keys of the table.
	_, b.store.paramsConverter, err = generator.InsertStatement(tableDef)
	if err != nil {
		return nil, fmt.Errorf("building load params converter: %w", err)
	}

	// External Data Config used by this binding. This will be a Google Cloud Storage file based table
	// that can be used to merge data into BigQuery.
	b.store.extDataConfig = &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		Schema:       make([]*bigquery.FieldSchema, 0),
	}

	// This builds the columns and strings required to upsert via a merge query.
	var colIdentifiers, rColIdentifiers []string
	for _, colName := range spec.FieldSelection.AllFields() {
		var col = tableDef.GetColumn(colName)

		colType, err := baseTypeMapper.GetColumnType(col)
		if err != nil {
			return nil, err
		}

		b.store.extDataConfig.Schema = append(b.store.extDataConfig.Schema, &bigquery.FieldSchema{
			Name:     identifierSanitizer(col.Name), // Sanitized name without quoting required
			Repeated: false,
			Required: col.PrimaryKey,
			Type:     bigquery.FieldType(colType.SQLType),
		})

		colIdentifiers = append(colIdentifiers, col.Identifier)
		rColIdentifiers = append(rColIdentifiers, fmt.Sprintf("r.%s", col.Identifier))
	}

	// If a field is selected whose document pointer is the root document, then Store()
	// will need to include the root document in the set of values being written.
	b.store.hasRootDocument = spec.FieldSelection.Document != ""

	b.store.tempTableName = fmt.Sprintf("%s_store_%d", tempTableNamePrefix, bindingPos)

	if spec.DeltaUpdates {
		// Store everything (do not merge/upsert).
		b.store.sql = fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		;`,
			tableDef.Identifier,
			strings.Join(colIdentifiers, ", "),
			strings.Join(colIdentifiers, ", "),
			b.store.tempTableName,
		)
	} else {
		// Updates for all columns minus the primary keys plus the document field.
		var lrUpdates []string
		for _, colName := range append(spec.FieldSelection.Values, spec.FieldSelection.Document) {
			var col = tableDef.GetColumn(colName)
			lrUpdates = append(lrUpdates, fmt.Sprintf("l.%s = r.%s", col.Identifier, col.Identifier))
		}

		// Perform merge query to update existing values.
		b.store.sql = fmt.Sprintf(`
		MERGE INTO %s AS l
		USING %s AS r
		ON %s
		WHEN MATCHED AND r.%s IS NULL THEN
			DELETE
		WHEN MATCHED THEN
			UPDATE SET %s
		WHEN NOT MATCHED THEN
			INSERT (%s)
			VALUES (%s)
		;`,
			tableDef.Identifier,
			b.store.tempTableName,
			strings.Join(pkJoins, " AND "),
			tableDef.GetColumn(spec.FieldSelection.Document).Identifier,
			strings.Join(lrUpdates, ", "),
			strings.Join(colIdentifiers, ", "),
			strings.Join(rColIdentifiers, ", "),
		)
	}

	return b, nil
}
