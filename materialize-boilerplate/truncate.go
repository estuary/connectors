package boilerplate

import (
	"errors"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
)

// ErrPublishedAtExcluded is the reason given when no selected field projects
// /_meta/uuid.
var ErrPublishedAtExcluded = errors.New("the binding excludes the flow_published_at field, which must be included to enable deletion after a backfill")

// PublishedAtMismatch returns why a projection of /_meta/uuid cannot be
// compared as each document's publication time, or nil if it can. The error
// is a clause suited for an operator to act on.
func PublishedAtMismatch(p *pf.Projection) error {
	var s = p.Inference.String_
	if s == nil {
		return fmt.Errorf("field %s projects /_meta/uuid without a string type, where a date-time with uuid encoding is required", p.Field)
	}
	if s.Format != "date-time" || s.ContentEncoding != "uuid" {
		return fmt.Errorf("field %s projects /_meta/uuid with the unexpected format %q and content encoding %q, where a date-time with uuid encoding is required", p.Field, s.Format, s.ContentEncoding)
	}
	return nil
}
