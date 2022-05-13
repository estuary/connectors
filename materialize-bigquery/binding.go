package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"

	log "github.com/sirupsen/logrus"
)

type Binding struct {
	// LoadSQL is a template SQL query that is generated
	// based on the values for this binding. The value can be compiled when the
	// Binding is initialized as there's no dynamic value in the
	// query as it's moving data that is stored in cloud storage.
	LoadSQL string

	// InsertOrMergeSQL is a template SQL query that is generated
	// based on the values for this binding. The value can be compiled when the
	// Binding is initialized as there's no dynamic value in the
	// query as it's moving data that is stored in cloud storage.
	InsertOrMergeSQL string

	// The google cloud storage bucket that is configured
	// to work with this binding. It is expected to be
	// configured and working when being assigned here.
	bucket *storage.BucketHandle

	// ExternalTableAlias is a string generated so that it can be used to reference
	// the external cloud storage data in a SQL Query. At the core of this, there is data
	// stored in Google Cloud Storage in a JSON format, and the connector needs to access that data to
	// merge it into BigQuery's dataset. By defining this table alias here, the Queries stored above can
	// use this alias when compiling the SQL queries, and job will be configured so that the query runs
	// and has access to that external table.
	externalTableAlias string

	// Read only
	Version string

	// Table is an internal represation of the Schema for this binding. The schema
	// contains references to the underlying BindingSpec as well as the BigQuery schema
	// generated and validated against the Binding Spec and the Binding Resource spec.
	*Table

	// Stateful and mutable part of the Binding struct. The ExternalStorage is a
	// Google Cloud Storage object representation that is used to stored the values
	// before it gets acknowledged and merged into BigQuery.
	*ExternalStorage
}

var externalTableNameTemplate = "external_table_binding_%s"

// Returns a new compiled Binding ready to be used. The Binding is generated from a materialization spec and merges the config
// info available to generate data structure that only requires to be compiled once when the materialization starts up. When
// this struct is called, all the information to generate the proper SQL queries is already available.
func NewBinding(
	ctx context.Context,
	bucket *storage.BucketHandle,
	bindingSpec *pf.MaterializationSpec_Binding,
	version string,
) (*Binding, error) {

	log.Infof("Generating Binding for Version: %s", version)

	var br bindingResource
	if err := pf.UnmarshalStrict(bindingSpec.ResourceSpecJson, &br); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w", err)
	}

	table, err := NewTable(bindingSpec, &br)
	if err != nil {
		return nil, err
	}

	externalTableAlias := fmt.Sprintf(externalTableNameTemplate, br.Table)

	b := &Binding{
		bucket:             bucket,
		Table:              table,
		externalTableAlias: externalTableAlias,
		Version:            version,
	}

	if !b.DeltaUpdates() {
		b.LoadSQL = generateLoadSQLQuery(b)
	}

	b.InsertOrMergeSQL = generateInsertOrMergeSQL(b)

	return b, nil
}

func (b *Binding) GenerateDocument(key, values tuple.Tuple, flowDocument json.RawMessage) map[string]interface{} {
	document := make(map[string]interface{})
	allValues := append(key, values...)

	for i, value := range allValues {
		field := b.Table.Fields[i]
		// If a value is nil, we need to
		// skip it, because otherwise it will overwrite
		// the value as "".
		if value == nil {
			continue
		}

		renderedValue, err := field.Render(value)
		if err != nil {
			panic(err)
		}

		document[field.Name()] = renderedValue
	}

	if b.Table.RootDocument != nil {
		document[b.Table.RootDocument.Name()] = string(flowDocument)
	}

	return document
}

// SetExternalStorage will replace whatever storage was attached to this binding
// If an existing storage is already attached, that external storage gets destroyed
// by calling DestroyExternalStorage() to make sure no data persists on Cloud Storage
func (b *Binding) SetExternalStorage(ctx context.Context, es *ExternalStorage) {
	var err error

	if b.ExternalStorage != nil {
		if err = b.DestroyExternalStorage(ctx); err != nil {
			log.Errorf("tried to delete the external storage: %v", err)
		}
	}

	b.ExternalStorage = es
}

// DestroyWriter clears the writer and delete the underlying
// asset in Cloud Storage.
// When this method returns, no writer is associated to this binding
// and it's required to assign a new one using the SetWriter() method.
func (b *Binding) DestroyExternalStorage(ctx context.Context) error {
	defer func() {
		b.ExternalStorage = nil
	}()

	if b.ExternalStorage != nil {
		err := b.ExternalStorage.Destroy(ctx)
		// It's possible that the previous writer never persisted
		// and as a result, the destroy operation won't have anything
		// to cleanup. If the object doesn't exist, it can safely move
		// to the next step
		if err != nil && err != storage.ErrObjectNotExist {
			return err
		}
	}

	return nil
}

// Helper function to generate a unique version for a given index and materialization spec
// It is created because the version needs to be passed to a Binding, even though the logic is
// simple, using a function to generate it means that the version generated is always consistent
func BindingVersion(materializationVersion string, bindingIndex int) string {
	return fmt.Sprintf("%s.%d", materializationVersion, bindingIndex)
}

// Generate the query for the provided binding to load
// rows from BigQuery. Essentially, this function returns
// a SQL Query that will look like this:
//
// "
// 		SELECT bq.`flow_document`
// 		FROM `materialized_table` AS bq
//  	JOIN `external_table_binding_materialized_table` AS external
//  	ON CAST(bq.`bike_id` AS INTEGER) = CAST(external.`bike_id` AS INTEGER)
//  	AND CAST(bq.`station_id` AS INTEGER) = CAST(external.`station_id` AS INTEGER);
// "
//
// All joined fields are casted mostly because the query joins
// between two different table format and I've seen some odd thing with timestamps
// and felt like if an error between type mismatch can happen, I rather
// have the error raised as a casting issue from BigQuery as that might help
// debugging in the future (P-O)
//
func generateLoadSQLQuery(binding *Binding) string {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(fmt.Sprintf(`
				SELECT bq.%s
				FROM %s AS bq
				JOIN %s AS external
				ON 
			`, backtickWrapper(binding.Table.RootDocument.Name()), backtickWrapper(binding.Name()), backtickWrapper(binding.externalTableAlias)))

	for i, field := range binding.Table.Keys {
		if i > 0 {
			queryBuilder.WriteString(" AND ")
		}

		// We're casting everything, because bigquery supports casting all
		// the currently supported values and it simplifies the quoting.
		queryBuilder.WriteString(fmt.Sprintf(
			"CAST(bq.%s AS %s) = CAST(external.%s AS %s)",
			backtickWrapper(field.Name()),
			field.fieldType,
			backtickWrapper(field.Name()),
			field.fieldType,
		))
	}

	queryBuilder.WriteString(";")

	return queryBuilder.String()
}

// Generate the query for the provided binding to create
// rows to BigQuery. The SQL statements aren't dynamic because the data
// is structured in a way that we're effectively merging data from an external
// table (Cloud storage) into BigQuery. ExternalDataConfig allows the query
// to use the same table name over and over but point to a different file
// in Cloud Storage. This allows the create statement to be immutable and reused
// on every occasion and still point at a dynamic table.
//
// Depending on the binding being configured
// to run in DeltaUpdate or not, the SQL returned here will be
// different. When a binding operates in non-delta mode, the SQL will look
// something like this:
//
// "
//  /* Non-Delta Mode SQL */
//		MERGE INTO `materialized_table` AS l
//		USING `external_table_binding_materialized_table` as r
//  	ON l.`bike_id` = r.`bike_id`
//		WHEN MATCHED AND r.`bike_id` IS NULL THEN
//			DELETE
//		WHEN MATCHED THEN
//			UPDATE SET (
//				l.`bike_id` = r.`bike_id`,
//				l.`station_id` = r.`station_id`,
//				l.`other_field` = r.`other_field`,
//				l.`flow_document` = r.`flow_document`
//			)
//		WHEN NOT MATCHED THEN
//			INSERT (`bike_id`, `station_id`, `other_field`, `flow_document`)
//			VALUES (r.`bike_id`, r.`station_id`, r.`other_field`, r.`flow_document`)
//		;`,
// "
//
// In case of a Delta update, the SQL generated is much simpler:
// "
//  /* Delta Mode SQL */
//		INSERT INTO `materialized_table` (`bike_id`, `station_id`, `other_field`, `flow_document`)
//		SELECT `bike_id`, `station_id`, `other_field`, `flow_document`
//		FROM `external_table_binding_materialized_table`;
// "

func generateInsertOrMergeSQL(binding *Binding) string {
	var columns []string
	var rColumns []string

	for _, field := range binding.Fields {
		columns = append(columns, backtickWrapper(field.Name()))
		rColumns = append(rColumns, fmt.Sprintf("r.%s", backtickWrapper(field.Name())))
	}

	// Early return if the binding spec is a delta
	// update
	if binding.Spec.DeltaUpdates {
		return fmt.Sprintf(`
			INSERT INTO %s (%s)
			SELECT %s FROM %s
			;`,
			backtickWrapper(binding.Table.Name()),
			strings.Join(columns, ", "),
			strings.Join(columns, ", "),
			backtickWrapper(binding.externalTableAlias),
		)
	}

	var joinPredicates []string
	var updates []string

	for _, field := range binding.Keys {
		joinPredicates = append(joinPredicates, fmt.Sprintf(
			"l.%s = r.%s",
			backtickWrapper(field.Name()),
			backtickWrapper(field.Name())),
		)
	}

	for _, field := range binding.Values {
		updates = append(updates, fmt.Sprintf(
			"l.%s = r.%s",
			backtickWrapper(field.Name()),
			backtickWrapper(field.Name())),
		)
	}

	return fmt.Sprintf(`
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
		backtickWrapper(binding.Table.Name()),
		backtickWrapper(binding.externalTableAlias),
		strings.Join(joinPredicates, " AND "),
		backtickWrapper(binding.RootDocument.Name()),
		strings.Join(updates, ", "),
		strings.Join(columns, ", "),
		strings.Join(rColumns, ", "),
	)
}
