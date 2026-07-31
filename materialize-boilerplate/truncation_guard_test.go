package boilerplate

import (
	"context"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

type stubTransactor struct {
	m.Transactor
	flushed bool
}

func (s *stubTransactor) Flush(context.Context, *pm.Request_Flush) error {
	s.flushed = true
	return nil
}

type guardBinding = MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper]

// binding builds a MappedBinding with the given value-field pointers, and
// optionally a materialized root document.
func guardBindingFor(collection string, delta bool, withDocument bool, valuePtrs ...string) guardBinding {
	var b guardBinding
	b.Collection = pf.CollectionSpec{Name: pf.Collection(collection)}
	b.DeltaUpdates = delta
	for _, ptr := range valuePtrs {
		inf := pf.Inference{}
		if ptr == metaUUIDPtr {
			// Only a date-time projection of the location is a clock.
			inf.String_ = &pf.Inference_String{Format: "date-time"}
		}
		b.Values = append(b.Values, MappedProjection[testMappedTyper]{
			Projection: Projection{Projection: pf.Projection{Field: ptr, Ptr: ptr, Inference: inf}},
		})
	}
	if withDocument {
		b.Document = &MappedProjection[testMappedTyper]{
			Projection: Projection{Projection: pf.Projection{Field: "flow_document", Ptr: ""}},
		}
	}
	return b
}

func flushWithBackfill(binding uint32, complete bool) *pm.Request_Flush {
	if complete {
		return &pm.Request_Flush{
			BackfillCompletes: []*pm.Request_Flush_BackfillComplete{{Binding: binding}},
		}
	}
	return &pm.Request_Flush{
		BackfillBegins: []*pm.Request_Flush_BackfillBegin{{Binding: binding}},
	}
}

func TestTruncationGuard(t *testing.T) {
	t.Run("not wrapped when every binding can report a clock", func(t *testing.T) {
		inner := &stubTransactor{}
		got := newTruncationGuard[testEndpointConfiger, testResourcer, testMappedTyper](inner, []guardBinding{
			guardBindingFor("acmeCo/a", false, true),                 // root document
			guardBindingFor("acmeCo/b", false, false, "/_meta/uuid"), // flow_published_at
			guardBindingFor("acmeCo/c", false, false, "/id", "/_meta/uuid"),
			guardBindingFor("acmeCo/d", true, false, "/id"), // delta: exempt
		})
		require.Same(t, m.Transactor(inner), got, "should not add indirection")
	})

	t.Run("backfill marker fails a binding with no clock source", func(t *testing.T) {
		guard := newTruncationGuard[testEndpointConfiger, testResourcer, testMappedTyper](
			&stubTransactor{},
			[]guardBinding{guardBindingFor("acmeCo/no-uuid", false, false, "/id")},
		)
		ft, ok := guard.(m.FlushTransactor)
		require.True(t, ok, "guard must implement FlushTransactor")

		for _, complete := range []bool{false, true} {
			err := ft.Flush(context.Background(), flushWithBackfill(0, complete))
			require.ErrorContains(t, err, "acmeCo/no-uuid")
			require.ErrorContains(t, err, "flow_published_at")
		}
	})

	t.Run("marker for a clock-capable binding passes and delegates", func(t *testing.T) {
		inner := &stubTransactor{}
		guard := newTruncationGuard[testEndpointConfiger, testResourcer, testMappedTyper](inner, []guardBinding{
			guardBindingFor("acmeCo/no-uuid", false, false, "/id"),
			guardBindingFor("acmeCo/has-uuid", false, false, "/flow_published_at", "/_meta/uuid"),
		})
		ft := guard.(m.FlushTransactor)

		require.NoError(t, ft.Flush(context.Background(), flushWithBackfill(1, false)))
		require.True(t, inner.flushed, "inner Flush should be called")
	})

	t.Run("no markers passes even for a binding with no clock source", func(t *testing.T) {
		inner := &stubTransactor{}
		guard := newTruncationGuard[testEndpointConfiger, testResourcer, testMappedTyper](inner, []guardBinding{
			guardBindingFor("acmeCo/no-uuid", false, false, "/id"),
		})
		ft := guard.(m.FlushTransactor)

		require.NoError(t, ft.Flush(context.Background(), &pm.Request_Flush{}))
		require.NoError(t, ft.Flush(context.Background(), nil))
		require.True(t, inner.flushed)
	})

	t.Run("marker for an unknown binding errors", func(t *testing.T) {
		guard := newTruncationGuard[testEndpointConfiger, testResourcer, testMappedTyper](
			&stubTransactor{},
			[]guardBinding{guardBindingFor("acmeCo/no-uuid", false, false, "/id")},
		)
		err := guard.(m.FlushTransactor).Flush(context.Background(), flushWithBackfill(7, false))
		require.ErrorContains(t, err, "unknown binding 7")
	})
}
