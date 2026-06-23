// Copyright 2019 The Vitess Authors.
// Licensed under the Apache License, Version 2.0 (see the LICENSE file in the
// parent sqlparser directory).
//
// This file contains code and data derived from
// vitess.io/vitess/go/mysql/datetime (v0.21.3), reduced by Estuary
// Technologies, Inc. to a proto-free subset.

// Package datetime is a trimmed shim of the upstream
// vitess.io/vitess/go/mysql/datetime package. It exposes only the
// IntervalType enum that the SQL grammar and AST reference for INTERVAL
// expressions. None of the date-math evaluation machinery comes along: the
// parser only needs to name interval units, never to compute with them.
package datetime

// IntervalType identifies a MySQL INTERVAL unit. Values are copied verbatim
// from upstream so the bitmask combinations below stay correct.
type IntervalType uint8

const (
	IntervalNone        IntervalType = 0
	IntervalMicrosecond IntervalType = 1 << 0
	IntervalSecond      IntervalType = 1 << 1
	IntervalMinute      IntervalType = 1 << 2
	IntervalHour        IntervalType = 1 << 3
	IntervalDay         IntervalType = 1 << 4
	IntervalMonth       IntervalType = 1 << 5
	IntervalYear        IntervalType = 1 << 6
	intervalMulti       IntervalType = 1 << 7
)

const (
	IntervalWeek    = IntervalDay | intervalMulti
	IntervalQuarter = IntervalMonth | intervalMulti

	IntervalSecondMicrosecond = IntervalSecond | IntervalMicrosecond
	IntervalMinuteMicrosecond = IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalMinuteSecond      = IntervalMinute | IntervalSecond
	IntervalHourMicrosecond   = IntervalHour | IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalHourSecond        = IntervalHour | IntervalMinute | IntervalSecond
	IntervalHourMinute        = IntervalHour | IntervalMinute
	IntervalDayMicrosecond    = IntervalDay | IntervalHour | IntervalMinute | IntervalSecond | IntervalMicrosecond
	IntervalDaySecond         = IntervalDay | IntervalHour | IntervalMinute | IntervalSecond
	IntervalDayMinute         = IntervalDay | IntervalHour | IntervalMinute
	IntervalDayHour           = IntervalDay | IntervalHour
	IntervalYearMonth         = IntervalYear | IntervalMonth
)

// ToString returns the lowercase MySQL keyword for the interval unit, as used
// when formatting INTERVAL, TIMESTAMPDIFF, and EXTRACT expressions.
func (itv IntervalType) ToString() string {
	switch itv {
	case IntervalYear:
		return "year"
	case IntervalQuarter:
		return "quarter"
	case IntervalMonth:
		return "month"
	case IntervalWeek:
		return "week"
	case IntervalDay:
		return "day"
	case IntervalHour:
		return "hour"
	case IntervalMinute:
		return "minute"
	case IntervalSecond:
		return "second"
	case IntervalMicrosecond:
		return "microsecond"
	case IntervalYearMonth:
		return "year_month"
	case IntervalDayHour:
		return "day_hour"
	case IntervalDayMinute:
		return "day_minute"
	case IntervalDaySecond:
		return "day_second"
	case IntervalHourMinute:
		return "hour_minute"
	case IntervalHourSecond:
		return "hour_second"
	case IntervalMinuteSecond:
		return "minute_second"
	case IntervalDayMicrosecond:
		return "day_microsecond"
	case IntervalHourMicrosecond:
		return "hour_microsecond"
	case IntervalMinuteMicrosecond:
		return "minute_microsecond"
	case IntervalSecondMicrosecond:
		return "second_microsecond"
	default:
		return "[unknown IntervalType]"
	}
}
