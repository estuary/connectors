package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
)

type Binding struct {
	// Queries is a collection of the queries that this binding
	// is configured to run over its lifetime. The queries do not change between
	// pipelines and as a result, gets compiled and stored here for use everytime
	// a Binding needs to run.
	Queries *sqlQueries

	// Reference to the definition of this binding as it was generated from
	// MaterializatioSpec. It is stored here so the individual values inside this
	// spec doesn't need to be explicitely referenced in this Binding struct and can
	// just be accessed when needed.
	spec *pf.MaterializationSpec_Binding

	schema bigquery.Schema
	bucket *storage.BucketHandle

	// Region is a configuration flag set by the user to determine
	// in which Region, in GCP, should the queries run.
	region string

	// Dataset is a configuration flag set by the user that defines the
	// location of the dataset, in GCP.
	dataset string

	// ObjectPath is a configuration flag set by the user to allow them to namespace
	// where this connector stores data in Google Cloud Storage
	objectPath string

	// ExternalTableAlias is a string generated so that it can be used to reference
	// the external cloud storage data in a SQL Query. At the core of this, there is data
	// stored in Google Cloud Storage in a JSON format, and the connector needs to access that data to
	// merge it into BigQuery's dataset. By defining this table alias here, the Queries stored above can
	// use this alias when compiling the SQL queries, and job will be configured so that the query runs
	// and has access to that external table.
	externalTableAlias string

	// Stateful and mutable part of the Binding struct. This writer gets
	// replaced everytime the pipeline needs to write new data
	*writer
}

type writer struct {
	edc     bigquery.ExternalDataConfig
	object  *storage.ObjectHandle
	writer  *storage.Writer
	buffer  *bufio.Writer
	encoder *json.Encoder
}

type sqlQueries struct {
	LoadSQL  string
	WriteSQL string
}

var externalTableNameTemplate = "external_table_binding_%s"

// Returns a new compiled Binding ready to be used. The Binding is generated from a materialization spec and merges the config
// info available to generate data structure that only requires to be compiled once when the materialization starts up. When
// this struct is called, all the information to generate the proper SQL queries is already available.
// The Binding object also includes a mutable part that is encapsulated inside the `writer` struct.
func NewBinding(ctx context.Context, cfg *config, bucket *storage.BucketHandle, bindingSpec *pf.MaterializationSpec_Binding) (*Binding, error) {
	schema, err := schemaForBinding(bindingSpec)
	if err != nil {
		return nil, err
	}

	var br bindingResource

	if err := pf.UnmarshalStrict(bindingSpec.ResourceSpecJson, &br); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w", err)
	}

	externalTableAlias := fmt.Sprintf(externalTableNameTemplate, br.Table)

	return &Binding{
		Queries:            generateSQLQueries(externalTableAlias, br, bindingSpec),
		bucket:             bucket,
		spec:               bindingSpec,
		schema:             schema,
		region:             cfg.Region,
		dataset:            cfg.Dataset,
		externalTableAlias: externalTableAlias,
	}, nil
}

// FilePath for the CloudStorage object that the underlying `writer` is connected to.
func (b *Binding) FilePath() string {
	obj := b.writer.object

	return fmt.Sprintf("%s/%s", obj.BucketName(), obj.ObjectName())
}

func (b *Binding) GenerateDocument(key, values tuple.Tuple, flowDocument json.RawMessage) map[string]interface{} {
	document := make(map[string]interface{})
	for i, value := range key {
		document[b.spec.FieldSelection.Keys[i]] = value
	}

	for i, value := range values {
		document[b.spec.FieldSelection.Values[i]] = value
	}

	if b.spec.FieldSelection.Document != "" {
		document[b.spec.FieldSelection.Document] = string(flowDocument)
	}

	return document
}

// Job is a small wrapper around `bigquery.Job` and doesn't provide much in terms of logic
// but ultimately removes some of the boilerplate code for the user's convenience.
// Since all the data to generate a job lives inside a Binding, it is possible for a Binding
// to build the job on its own and return a running Job.
//
// The returned Job is going to be configured so it's possible to run a query that joins over
// the bigquery dataset & the cloud storage external table. The name of the table is already computed
// in the initialization of a Binding and is stored in `externalTableAlias`
func (b *Binding) Job(ctx context.Context, client *bigquery.Client, q string) (*bigquery.Job, error) {
	query := client.Query(q)
	query.TableDefinitions = map[string]bigquery.ExternalData{b.externalTableAlias: &b.writer.edc}
	query.DefaultDatasetID = b.dataset
	query.Location = b.region

	return query.Run(ctx)
}

// Once Commit returns, the binding cannot be written to until Reset() is called.
func (b *Binding) Commit(ctx context.Context) error {
	return b.writer.commit(ctx)
}

// InitializeNewWriter is called when a new Writer need to be instantiated.
// This is done so every pipeline operation in the bigquery materialization
// can work with it's own writer to avoid data integrity issues.
// Call Reset when the lifecycle of a bigquery materialization is completed and
// the data is committed and acknowledged.
func (b *Binding) InitializeNewWriter(ctx context.Context, name string) error {
	var err error

	if b.writer != nil {
		err = b.writer.Destroy(ctx)
		if err != nil {
			return err
		}
	}

	obj := b.bucket.Object(name)
	edc := bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		Schema:       b.schema,
		SourceURIs:   []string{fmt.Sprintf("gs://%s/%s", obj.BucketName(), obj.ObjectName())},
	}

	b.writer = newWriter(ctx, edc, obj)

	return err
}

func (b *Binding) Store(doc map[string]interface{}) error {
	err := b.writer.encoder.Encode(doc)

	return err
}

func (b *Binding) DeltaUpdates() bool {
	return b.spec.DeltaUpdates
}

func newWriter(ctx context.Context, edc bigquery.ExternalDataConfig, objHandle *storage.ObjectHandle) *writer {
	w := objHandle.NewWriter(ctx)
	b := bufio.NewWriter(w)

	e := json.NewEncoder(b)
	e.SetEscapeHTML(false)
	e.SetIndent("", "")

	writer := &writer{
		edc:     edc,
		object:  objHandle,
		writer:  w,
		buffer:  b,
		encoder: e,
	}
	return writer
}

// After commit has returend, the writer cannot be written to
// as the underlying objects are closed.
// This method will make the writer as committed, even if an
// error occurred while comitting the data. This is so it tells the binding
// that it's not safe to write data to this writer anymore.
func (w *writer) commit(ctx context.Context) error {

	var err error
	if err = w.buffer.Flush(); err != nil {
		return fmt.Errorf("flushing the buffer: %w", err)
	}

	if err = w.writer.Close(); err != nil {
		return fmt.Errorf("closing the writer to cloud storage: %w", err)
	}

	return nil
}

// Delete the object associated to this Writer. Once deleted, the file
// cannot be recovered. This is a destructive operation that should only
// happen once the underlying data is written to bigquery. Otherwise, it could result
// in data loss.
func (w *writer) Destroy(ctx context.Context) error {
	return w.object.Delete(ctx)
}

func randomString() (string, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("generating UUID: %w", err)
	}

	return id.String(), nil
}

// Generates the sqlQueries that reflects the specification for the provided materialization
// binding.
//
func generateSQLQueries(externalTableAlias string, br bindingResource, bindingSpec *pf.MaterializationSpec_Binding) *sqlQueries {
	var loadQuery, writeQuery string
	var columns []string
	var rColumns []string

	var loadPredicates []string
	for i, value := range bindingSpec.FieldSelection.Keys {
		loadPredicates = append(loadPredicates, fmt.Sprintf("%s = '%s'", bindingSpec.FieldSelection.Keys[i], value))
	}

	for _, key := range bindingSpec.FieldSelection.AllFields() {
		columns = append(columns, key)
		rColumns = append(rColumns, fmt.Sprintf("r.%s", key))
	}

	if bindingSpec.DeltaUpdates {
		loadQuery = "ASSERT false AS 'Load queries should never be executed in Delta Updates mode.';"
		writeQuery = fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		;`,
			br.Table,
			strings.Join(columns, ", "),
			strings.Join(rColumns, ", "),
			br.Table,
		)

	} else {
		loadQuery = fmt.Sprintf(`
			SELECT %s
			FROM %s
			WHERE %s
			;`,
			bindingSpec.FieldSelection.Document,
			br.Table,
			strings.Join(loadPredicates, " AND "),
		)

		var joinPredicates []string
		for _, key := range bindingSpec.FieldSelection.Keys {
			joinPredicates = append(joinPredicates, fmt.Sprintf("l.%s = r.%s", key, key))
		}

		var updates []string
		for _, k := range append(bindingSpec.FieldSelection.Values, bindingSpec.FieldSelection.Document) {
			updates = append(updates, fmt.Sprintf("l.%s = r.%s", k, k))
		}

		writeQuery = fmt.Sprintf(`
			MERGE INTO %s AS l
			USING %s as r
			ON %s
			WHEN MATCHED AND r.%s IS NULL THEN
				DELETE
			WHEN MATCHED THEN
				UPDATE SET %s
			WHEN NOT MATCHED THEN
				INSERT (%s)
				VALUES (%s)
			;`,
			br.Table,
			externalTableAlias,
			strings.Join(joinPredicates, " AND "),
			bindingSpec.FieldSelection.Document,
			strings.Join(updates, ", "),
			strings.Join(columns, ", "),
			strings.Join(rColumns, ", "),
		)
	}
	return &sqlQueries{
		LoadSQL:  loadQuery,
		WriteSQL: writeQuery,
	}
}

// Bigquery only allows underscore, letters, numbers, and sometimes hyphens for identifiers. Convert everything else to underscore.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-\._0-9a-zA-Z]`)

func identifierSanitizer(text string) string {
	return identifierSanitizerRegexp.ReplaceAllString(text, "_")
}
