package sql

import (
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestMapOnStringMaxLength(t *testing.T) {
	defDDL := "BIGINT"
	numericDDL := "NUMERIC(38,0)"
	textDDL := "TEXT"

	defMapper := MapStatic(defDDL)
	numericMapper := MapStatic(numericDDL)
	stringMapper := MapStatic(textDDL)

	require.Panics(t, func() { MapOnStringMaxLength(defMapper) })
	require.Panics(t, func() {
		MapOnStringMaxLength(
			defMapper,
			stringStep{startAt: 50, mapper: stringMapper},
			stringStep{startAt: 16, mapper: numericMapper},
		)
	})

	makeProjection := func(length int) boilerplate.Projection {
		return boilerplate.Projection{Projection: pf.Projection{
			Inference: pf.Inference{
				String_: &pf.Inference_String{MaxLength: uint32(length)},
			},
		}}
	}

	mapper := MapOnStringMaxLength(
		defMapper,
		stringStep{startAt: 16, mapper: numericMapper},
		stringStep{startAt: 50, mapper: stringMapper},
	)

	for _, tt := range []struct {
		in  boilerplate.Projection
		ddl string
	}{
		{makeProjection(0), defDDL},
		{makeProjection(15), defDDL},
		{makeProjection(16), numericDDL},
		{makeProjection(17), numericDDL},
		{makeProjection(49), numericDDL},
		{makeProjection(50), textDDL},
		{makeProjection(51), textDDL},
	} {
		ddl, _ := mapper(tt.in)
		require.Equal(t, tt.ddl, ddl)
	}

	// nil string inference uses the default mapper.
	p := boilerplate.Projection{Projection: pf.Projection{Inference: pf.Inference{}}}
	ddl, _ := mapper(p)
	require.Equal(t, defDDL, ddl)

	// String inference with no max length uses the default mapper.
	p = boilerplate.Projection{Projection: pf.Projection{Inference: pf.Inference{
		String_: &pf.Inference_String{MaxLength: 0},
	}}}
	ddl, _ = mapper(p)
	require.Equal(t, defDDL, ddl)
}
