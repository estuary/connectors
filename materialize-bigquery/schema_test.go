package main

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type projectionTestExample struct {
	projection  *pf.Projection
	testFn      func(*Field, *pf.Projection) error
	shouldError bool
}

func TestNewField(t *testing.T) {
	projections := []projectionTestExample{
		{
			projection: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"string", "integer"},
				},
			},
			shouldError: true,
		},
		{
			projection: &pf.Projection{
				Inference: pf.Inference{
					Types: []string{"null"},
				},
			},
			shouldError: true,
		},
		{
			projection: &pf.Projection{
				Field: "column_a",
				Inference: pf.Inference{
					Types: []string{"string", "null"},
				},
			},
			shouldError: false,
			testFn: func(f *Field, pf *pf.Projection) error {
				if f.fieldType != bigquery.StringFieldType {
					return fmt.Errorf("expected string field, got %s", f.fieldType)
				}

				schema, err := f.FieldSchema(pf)
				if err != nil {
					return err
				}

				if schema.Required == true {
					return fmt.Errorf("expected schema field to be nullable")
				}

				return nil
			},
		},
	}

	for _, te := range projections {
		var err error
		field, err := NewField(te.projection)

		if te.shouldError && err == nil {
			t.Error("expected error for a projection that includes multiple inference types")
		}

		if te.testFn == nil {
			continue
		}

		if err = te.testFn(field, te.projection); err != nil {
			t.Error(err)
		}
	}
}
