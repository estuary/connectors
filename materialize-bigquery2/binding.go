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
	Queries *sqlQueries

	spec               *pf.MaterializationSpec_Binding
	schema             bigquery.Schema
	bucket             *storage.BucketHandle
	region             string
	dataset            string
	objectPath         string
	externalTableAlias string

	*writer
}

type writer struct {
	dataCommitted bool
	empty         bool
	edc           bigquery.ExternalDataConfig
	object        *storage.ObjectHandle
	writer        *storage.Writer
	buffer        *bufio.Writer
	encoder       *json.Encoder
}

type sqlQueries struct {
	CreateSQL string
	LoadSQL   string
	WriteSQL  string
}

var externalTableNameTemplate = "external_table_binding_%s"

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

	b := &Binding{
		Queries:            generateSQLQueries(externalTableAlias, br, bindingSpec),
		bucket:             bucket,
		spec:               bindingSpec,
		schema:             schema,
		region:             cfg.Region,
		dataset:            cfg.Dataset,
		externalTableAlias: externalTableAlias,
	}

	name, err := randomString()
	if err != nil {
		return nil, err
	}

	b.Reset(ctx, fmt.Sprintf("%s/%s", cfg.BucketPath, name))
	return b, nil
}

func (b *Binding) EDC() *bigquery.ExternalDataConfig {
	return &b.writer.edc
}

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

func (b *Binding) Reset(ctx context.Context, name string) {
	obj := b.bucket.Object(name)
	edc := bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		Schema:       b.schema,
		SourceURIs:   []string{fmt.Sprintf("gs://%s/%s", obj.BucketName(), obj.ObjectName())},
	}

	b.writer = newWriter(ctx, edc, obj)
}

func (b *Binding) Store(doc map[string]interface{}) error {
	err := b.writer.encoder.Encode(doc)

	if err == nil {
		b.empty = false
	}

	return err
}

func (b *Binding) DeltaUpdates() bool {
	return b.spec.DeltaUpdates
}

func (b *Binding) DataCommitted() bool {
	return b.writer.dataCommitted
}

func newWriter(ctx context.Context, edc bigquery.ExternalDataConfig, objHandle *storage.ObjectHandle) *writer {
	w := objHandle.NewWriter(ctx)
	b := bufio.NewWriter(w)

	e := json.NewEncoder(b)
	e.SetEscapeHTML(false)
	e.SetIndent("", "")

	return &writer{
		empty:         true,
		dataCommitted: false,
		edc:           edc,
		object:        objHandle,
		writer:        w,
		buffer:        b,
		encoder:       e,
	}
}

// After commit has returend, the writer cannot be written to
// as the underlying objects are closed.
// This method will make the writer as committed, even if an
// error occurred while comitting the data. This is so it tells the binding
// that it's not safe to write data to this writer anymore.
func (w *writer) commit(ctx context.Context) error {
	w.dataCommitted = true

	var err error
	if err = w.buffer.Flush(); err != nil {
		return fmt.Errorf("flushing the buffer: %w", err)
	}

	if err = w.writer.Close(); err != nil {
		return fmt.Errorf("closing the writer to cloud storage: %w", err)
	}

	return nil
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
		CreateSQL: "",
		LoadSQL:   loadQuery,
		WriteSQL:  writeQuery,
	}
}

// Bigquery only allows underscore, letters, numbers, and sometimes hyphens for identifiers. Convert everything else to underscore.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-\._0-9a-zA-Z]`)

func identifierSanitizer(text string) string {
	return identifierSanitizerRegexp.ReplaceAllString(text, "_")
}
