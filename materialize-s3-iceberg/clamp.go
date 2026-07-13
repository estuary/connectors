package main

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// Date and timestamp values are clamped to years [1, 9999]. This bound was
// originally forced by the removed pyiceberg/pyarrow tool, which materialized
// parquet min/max stats as Python datetime/date objects and crashed the append
// with `OverflowError: date value out of range` for anything outside the band.
// iceberg-go reads those stats as raw INT32 days (date) / INT64 micros
// (timestamptz) and has no such limit, so the clamp is retained only to keep
// newly-written values consistent with data already materialized by the old
// tool; it could be relaxed if that consistency is not required.
const (
	minYear = 1
	maxYear = 9999
)

// dateLikeRe matches YYYY-MM-DD with any digit-count year (Postgres can emit
// e.g. "5874897-12-31"; Go's time.Parse(time.DateOnly, ...) won't accept it).
var dateLikeRe = regexp.MustCompile(`^(\d+)-(\d{2})-(\d{2})$`)

// clampTimestamp returns a boundary RFC3339Nano value when v parses cleanly
// but its UTC-normalized year is outside [minYear, maxYear].
func clampTimestamp(v tuple.TupleElement) (any, error) {
	s, ok := v.(string)
	if !ok {
		return v, nil
	}

	t, canonical, err := sql.ParseRFC3339Nano(s)
	if err != nil {
		return nil, err
	}
	// The UTC-normalized year is what determines the bound:
	// "9999-12-31T23:59:59-14:00" looks like 9999 locally but is 10000 in UTC.
	switch year := t.UTC().Year(); {
	case year > maxYear:
		return time.Date(maxYear, 12, 31, 23, 59, 59, 999999000, time.UTC).Format(time.RFC3339Nano), nil
	case year < minYear:
		return time.Date(minYear, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), nil
	}
	return canonical, nil
}

// clampDate also rescues > 4-digit years, which time.Parse(time.DateOnly, ...)
// refuses to parse at all.
func clampDate(v tuple.TupleElement) (any, error) {
	s, ok := v.(string)
	if !ok {
		return v, nil
	}

	m := dateLikeRe.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("could not parse %q as simple date", s)
	}
	year, _ := strconv.Atoi(m[1])
	switch {
	case year > maxYear:
		return fmt.Sprintf("%04d-12-31", maxYear), nil
	case year < minYear:
		return fmt.Sprintf("%04d-01-01", minYear), nil
	}
	return s, nil
}
