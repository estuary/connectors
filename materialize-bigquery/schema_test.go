package main

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
)

type projectionTestExample struct {
	projection  *pf.Projection
	testFn      func(f *Field) error
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
				Inference: pf.Inference{
					Types: []string{"string", "null"},
				},
			},
			shouldError: false,
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

		if err = te.testFn(field); err != nil {
			t.Error(err)
		}
	}
}
