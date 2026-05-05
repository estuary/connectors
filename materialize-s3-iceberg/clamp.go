package main

import (
	"fmt"
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
		return s, nil
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
		return s, nil
	}
	year, err := strconv.Atoi(m[1])
	if err != nil {
		return s, nil
	}
	switch {
	case year > pyMaxYear:
		return fmt.Sprintf("%04d-12-31", pyMaxYear), nil
	case year < pyMinYear:
		return fmt.Sprintf("%04d-01-01", pyMinYear), nil
	}
	return s, nil
}
