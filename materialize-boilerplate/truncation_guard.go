package boilerplate

import (
	"context"
	"fmt"

	m "github.com/estuary/connectors/go/materialize"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// metaUUIDPtr is the JSON pointer of the document UUID, from which the runtime
// extracts a document's clock.
const metaUUIDPtr = "/_meta/uuid"

// truncationGuard wraps a Transactor to fail a transaction that delivers a
// backfill marker for a binding which cannot report a document clock.
//
// Acting on a truncation requires comparing each loaded document's clock against
// the backfill's truncation boundary, and that clock is carried by /_meta/uuid.
// A binding materializing the root document always has it. Otherwise it needs a
// materialized projection of /_meta/uuid — either the raw field or a view of it
// such as flow_published_at. Selecting one is optional, so a task may be
// configured without it; such a task simply cannot support truncations, and this
// guard reports that as a clear error at the moment a truncation is actually
// triggered rather than letting the runtime fail on a missing clock.
type truncationGuard struct {
	m.Transactor
	// canReportClock is indexed by binding, and false only for bindings whose
	// Loaded documents would carry no document UUID.
	canReportClock []bool
	// collections is indexed by binding, for error messages.
	collections []string
}

func (g *truncationGuard) Flush(ctx context.Context, flush *pm.Request_Flush) error {
	if flush != nil {
		for _, marker := range flush.BackfillBegins {
			if err := g.check(marker.Binding); err != nil {
				return err
			}
		}
		for _, marker := range flush.BackfillCompletes {
			if err := g.check(marker.Binding); err != nil {
				return err
			}
		}
	}

	// Preserve any Flush handling of the wrapped Transactor.
	if inner, ok := g.Transactor.(m.FlushTransactor); ok {
		return inner.Flush(ctx, flush)
	}
	return nil
}

func (g *truncationGuard) check(binding uint32) error {
	if int(binding) >= len(g.canReportClock) {
		return fmt.Errorf("backfill marker for unknown binding %d", binding)
	} else if g.canReportClock[binding] {
		return nil
	}

	return fmt.Errorf(
		"cannot apply the backfill of collection %s: this materialization does not report a document clock for that binding, "+
			"which is required to determine which destination rows the backfill superseded. "+
			"Materialize the root document, or add the %q field to the binding's selected fields",
		g.collections[binding], "flow_published_at",
	)
}

// newTruncationGuard wraps transactor so that backfill markers are validated
// against what each binding is able to report. It returns transactor unchanged
// when every binding can report a document clock, so that the common case adds
// no indirection.
func newTruncationGuard[EC EndpointConfiger, RC Resourcer[RC, EC], MT MappedTyper](
	transactor m.Transactor,
	bindings []MappedBinding[EC, RC, MT],
) m.Transactor {
	var (
		canReportClock = make([]bool, len(bindings))
		collections    = make([]string, len(bindings))
		allCan         = true
	)

	for idx, b := range bindings {
		collections[idx] = b.Collection.Name.String()
		canReportClock[idx] = bindingReportsClock(b)
		allCan = allCan && canReportClock[idx]
	}

	if allCan {
		return transactor
	}

	return &truncationGuard{
		Transactor:     transactor,
		canReportClock: canReportClock,
		collections:    collections,
	}
}

func bindingReportsClock[EC EndpointConfiger, RC Resourcer[RC, EC], MT MappedTyper](b MappedBinding[EC, RC, MT]) bool {
	if b.DeltaUpdates {
		// Delta-updates bindings do not issue Loads, so there are no loaded
		// document clocks to compare in the first place.
		return true
	} else if b.Document != nil {
		// The materialized root document carries its own /_meta/uuid.
		return true
	}

	for _, p := range b.SelectedProjections() {
		// Only a date-time projection of the location (canonically
		// flow_published_at) is reported as a clock.
		if p.Ptr == metaUUIDPtr && p.Inference.String_ != nil && p.Inference.String_.Format == "date-time" {
			return true
		}
	}

	return false
}
