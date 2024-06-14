package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type glueCatalog struct {
	cfg *config
	// TODO(whb): Including the lastSpec from the validate or apply request is a temporary hack
	// until we get around to removing the "load/persist a spec in the destination" concept more
	// thoroughly. As of this writing, the iceberg materialization is the first one to actually use
	// the lastSpec from the validate or apply request.
	lastSpec      *pf.MaterializationSpec
	tableLocation string
}

func newGlueCatalog(cfg config, lastSpec *pf.MaterializationSpec) *glueCatalog {
	return &glueCatalog{
		cfg:           &cfg,
		lastSpec:      lastSpec,
		tableLocation: fmt.Sprintf("s3://%s/%s/", cfg.Bucket, cfg.Prefix),
	}
}

func (c *glueCatalog) infoSchema() (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		func(rp []string) []string { return rp },
		func(f string) string { return f },
	)

	b, err := runIcebergctl(c.cfg, "info-schema")
	if err != nil {
		return nil, err
	}

	var got map[string][]existingIcebergColumn
	if err := json.Unmarshal(b, &got); err != nil {
		return nil, err
	}

	for res, fields := range got {
		parts := strings.Split(res, ".")
		namespace := parts[0]
		table := parts[1]
		is.PushResource(namespace, table)

		for _, f := range fields {
			is.PushField(boilerplate.EndpointField{
				Name:     f.Name,
				Nullable: f.Nullable,
				Type:     string(f.Type),
			}, namespace, table)
		}
	}

	return is, nil
}

func (c *glueCatalog) listNamespaces() ([]string, error) {
	var got []string

	if b, err := runIcebergctl(c.cfg, "list-namespaces"); err != nil {
		return nil, err
	} else if err := json.Unmarshal(b, &got); err != nil {
		return nil, err
	}

	return got, nil
}

func (c *glueCatalog) createNamespace(namespace string) error {
	_, err := runIcebergctl(c.cfg, "create-namespace", namespace)
	return err
}

func (c *glueCatalog) CreateResource(_ context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	b := spec.Bindings[bindingIndex]

	tc := tableCreate{Location: c.tableLocation}

	parquetSchema := schemaWithOptions(b.FieldSelection.AllFields(), b.Collection)
	for _, f := range parquetSchema {
		tc.Fields = append(tc.Fields, existingIcebergColumn{
			Name:     f.Name,
			Nullable: !f.Required,
			Type:     parquetTypeToIcebergType(f.DataType),
		})
	}

	input, err := json.Marshal(tc)
	if err != nil {
		return "", nil, err
	}

	fqn := pathToFQN(b.ResourcePath)

	return fmt.Sprintf("create table %q", fqn), func(_ context.Context) error {
		if _, err := runIcebergctl(c.cfg, "create-table", fqn, string(input)); err != nil {
			return fmt.Errorf("creating table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

func (c *glueCatalog) DeleteResource(_ context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	fqn := pathToFQN(path)

	return fmt.Sprintf("drop table %q", fqn), func(_ context.Context) error {
		if _, err := runIcebergctl(c.cfg, "drop-table", fqn); err != nil {
			return fmt.Errorf("dropping table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

func (c *glueCatalog) UpdateResource(_ context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	b := spec.Bindings[bindingIndex]

	ta := tableAlter{}

	for _, f := range bindingUpdate.NewlyNullableFields {
		ta.NewlyNullableColumns = append(ta.NewlyNullableColumns, f.Name)
	}

	for _, p := range bindingUpdate.NewProjections {
		s := enc.ProjectionToParquetSchemaElement(p, schemaOptions...)
		ta.NewColumns = append(ta.NewColumns, existingIcebergColumn{
			Name:     s.Name,
			Nullable: true, // always true for added columns
			Type:     parquetTypeToIcebergType(s.DataType),
		})
	}

	input, err := json.Marshal(ta)
	if err != nil {
		return "", nil, err
	}

	fqn := pathToFQN(b.ResourcePath)

	return fmt.Sprintf("alter table %q", fqn), func(_ context.Context) error {
		if _, err := runIcebergctl(c.cfg, "alter-table", fqn, string(input)); err != nil {
			return fmt.Errorf("altering table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

func (c *glueCatalog) appendFiles(
	tablePath []string,
	filePaths []string,
	prevCheckpoint string,
	nextCheckpoint string,
) error {
	fqn := pathToFQN(tablePath)

	b, err := runIcebergctl(
		c.cfg,
		"append-files",
		fqn,
		prevCheckpoint,
		nextCheckpoint,
		strings.Join(filePaths, ","),
	)
	if err != nil {
		return err
	}

	if len(b) > 0 {
		log.WithFields(log.Fields{
			"table":  fqn,
			"output": string(b),
		}).Info("append files")
	}

	return nil
}

// These functions are vestigial from the age of persisting specs in the destination.

func (c *glueCatalog) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (c *glueCatalog) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	return c.lastSpec, nil
}

func (c *glueCatalog) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, exists bool) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}
