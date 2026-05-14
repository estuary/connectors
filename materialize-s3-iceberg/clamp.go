package main

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// Bounds of Python `datetime`. pyiceberg.add_files reads parquet column
// min/max stats through pyarrow, which materializes them as Python
// datetime/date - so any value outside this band crashes the append with
// `OverflowError: date value out of range`, even though Iceberg's INT64
// micros (timestamptz) and INT32 days (date) encodings would accept it.
const (
	pyMinYear = 1
	pyMaxYear = 9999
)

// Bounds of an int64 nanoseconds-since-epoch Iceberg `timestamptz_ns` column.
// Outside this range, time.Time.UnixNano() (called from
// go/writer.getTimestampNanosVal) silently wraps, producing nonsense values.
// The bounds correspond exactly to math.MinInt64 / math.MaxInt64 nanoseconds
// since 1970-01-01T00:00:00Z.
var (
	nsMinTime = time.Unix(0, math.MinInt64).UTC()
	nsMaxTime = time.Unix(0, math.MaxInt64).UTC()
)

// dateLikeRe matches YYYY-MM-DD with any digit-count year (Postgres can emit
// e.g. "5874897-12-31"; Go's time.Parse(time.DateOnly, ...) won't accept it).
var dateLikeRe = regexp.MustCompile(`^(\d+)-(\d{2})-(\d{2})$`)

// clampTimestamp returns a boundary RFC3339Nano value when v parses cleanly
// but its UTC-normalized year is outside [pyMinYear, pyMaxYear].
func clampTimestamp(v tuple.TupleElement) (any, error) {
	s, ok := v.(string)
	if !ok {
		return v, nil
	}

	t, err := time.Parse(time.RFC3339Nano, strings.Replace(s, "z", "Z", 1))
	if err != nil {
		return nil, fmt.Errorf("could not parse %q as RFC3339 timestamp: %w", s, err)
	}
	// UTC year is what reaches pyarrow: "9999-12-31T23:59:59-14:00" looks
	// like 9999 locally but materializes as 10000 in UTC.
	switch year := t.UTC().Year(); {
	case year > pyMaxYear:
		return time.Date(pyMaxYear, 12, 31, 23, 59, 59, 999999000, time.UTC).Format(time.RFC3339Nano), nil
	case year < pyMinYear:
		return time.Date(pyMinYear, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano), nil
	}
	return s, nil
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
	case year > pyMaxYear:
		return fmt.Sprintf("%04d-12-31", pyMaxYear), nil
	case year < pyMinYear:
		return fmt.Sprintf("%04d-01-01", pyMinYear), nil
	}
	return s, nil
}

// clampTimestampNanos parses v as an RFC3339Nano timestamp and clamps it to
// the int64 nanoseconds-since-epoch range supported by Iceberg
// `timestamptz_ns` columns. Boundary times are returned in RFC3339Nano form so
// downstream parsing in go/writer.getTimestampNanosVal succeeds without
// overflow.
func clampTimestampNanos(v tuple.TupleElement) (any, error) {
	s, ok := v.(string)
	if !ok {
		return v, nil
	}

	t, err := time.Parse(time.RFC3339Nano, strings.Replace(s, "z", "Z", 1))
	if err != nil {
		return nil, fmt.Errorf("could not parse %q as RFC3339 timestamp: %w", s, err)
	}
	switch t = t.UTC(); {
	case t.After(nsMaxTime):
		return nsMaxTime.Format(time.RFC3339Nano), nil
	case t.Before(nsMinTime):
		return nsMinTime.Format(time.RFC3339Nano), nil
	}
	return s, nil
}
