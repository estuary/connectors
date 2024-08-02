package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/sqlcapture/tests"
	"github.com/sirupsen/logrus"
)

func TestViewDiscovery(t *testing.T) {
	var unique = "18110541"
	var tb, ctx = oracleTestBackend(t), context.Background()
	var tableName = tb.CreateTable(ctx, t, unique, "(id INTEGER PRIMARY KEY, grp INTEGER, data VARCHAR(2000))")

	var view = fmt.Sprintf(`"t%s"`, unique+"_simpleview")
	tb.Query(ctx, t, false, fmt.Sprintf(`DROP VIEW %s`, view))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE VIEW %s AS SELECT id, data FROM %s WHERE grp = 1`, view, tableName))
	t.Cleanup(func() {
		logrus.WithField("view", view).Debug("dropping view")
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP VIEW %s`, view))
	})

	var bindings = tb.CaptureSpec(ctx, t).Discover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(strings.TrimPrefix(tableName, "test."))))
	for _, binding := range bindings {
		logrus.WithField("name", binding.RecommendedName).Debug("discovered stream")
		if strings.Contains(string(binding.RecommendedName), "_simpleview") {
			t.Errorf("view returned by catalog discovery")
		}
		if strings.Contains(string(binding.RecommendedName), "_matview") {
			t.Errorf("materialized view returned by catalog discovery")
		}
	}
}

func TestAllTypes(t *testing.T) {
	var unique = "18110541"
	var tb, ctx = oracleTestBackend(t), context.Background()
	var typesAndValues = [][]any{
		[]any{"nvchar2", "NVARCHAR2(2000)", "nvarchar2 value with unicode characters ❤️ 🔥️'')"},
		[]any{"vcahr2", "VARCHAR2(2000)", "varchar2 value"},
		[]any{"single_nchar", "NCHAR", "a"},
		[]any{"vchar", "VARCHAR(2000)", "varchar value"},
		[]any{"num", "NUMBER(38, 9)", 123456789.123456789},
		[]any{"num19", "NUMBER(19, 0)", 1234567891234567891},
		[]any{"num15", "NUMBER(15, 0)", 123456789123456},
		[]any{"small_int", "SMALLINT", 123456789.123456789},
		[]any{"integ", "INTEGER", NewRawTupleValue("18446744073709551615")},
		[]any{"double_precision", "DOUBLE PRECISION", 123456789.123456789},
		[]any{"float_126", "FLOAT(126)", 123456789.123456789},
		[]any{"real_num", "REAL", 123456789.123456789},
		[]any{"dateonly", "DATE", NewRawTupleValue("DATE '2022-01-01'")},
		[]any{"datetime", "DATE", NewRawTupleValue("TO_DATE('1998-JAN-01 13:00:00', 'YYYY-MON-DD HH24:MI:SS', 'NLS_DATE_LANGUAGE=AMERICAN')")},
		[]any{"ts", "TIMESTAMP", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00'")},
		[]any{"ts_nine", "TIMESTAMP(9)", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00.123456789'")},
		[]any{"ts_tz", "TIMESTAMP WITH TIME ZONE", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00 +01:00'")},
		[]any{"ts_tz_nine", "TIMESTAMP(9) WITH TIME ZONE", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00.123456789 +01:00'")},
		[]any{"ts_local_tz", "TIMESTAMP WITH LOCAL TIME ZONE", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00 +02:00'")},
		[]any{"ts_local_tz_nine", "TIMESTAMP(9) WITH LOCAL TIME ZONE", NewRawTupleValue("TIMESTAMP '2022-01-01 13:00:00 +02:00'")},
		[]any{"interval_year", "INTERVAL YEAR(4) TO MONTH", NewRawTupleValue("INTERVAL '1234-5' YEAR(4) TO MONTH")},
		[]any{"interval_day", "INTERVAL DAY TO SECOND", NewRawTupleValue("INTERVAL '1 2:3:4.567' DAY TO SECOND(3)")},
		[]any{"r", "RAW(1000)", NewRawTupleValue("UTL_RAW.CAST_To_RAW('testing raw value')")},
	}

	var columnDefs = "("
	var vals []any
	for idx, tv := range typesAndValues {
		if idx > 0 {
			columnDefs += ", "
		}
		columnDefs += fmt.Sprintf("%s %s", tv[0].(string), tv[1].(string))
		vals = append(vals, tv[2])
		idx += 1
	}
	columnDefs += ")"
	var tableName = tb.CreateTable(ctx, t, unique, columnDefs)

	tb.Insert(ctx, t, tableName, [][]any{vals})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(unique))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(unique))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableName, [][]any{vals})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestNullValues(t *testing.T) {
	var unique = "18110541"
	var tb, ctx = oracleTestBackend(t), context.Background()
	var typesAndValues = [][]any{
		[]any{"nvchar2", "NVARCHAR2(2000)", NewRawTupleValue("NULL")},
		[]any{"vcahr2", "VARCHAR2(2000)", NewRawTupleValue("NULL")},
		[]any{"single_nchar", "NCHAR", NewRawTupleValue("NULL")},
		[]any{"vchar", "VARCHAR(2000)", NewRawTupleValue("NULL")},
		[]any{"num", "NUMBER(38, 9)", NewRawTupleValue("NULL")},
		[]any{"num19", "NUMBER(19, 0)", NewRawTupleValue("NULL")},
		[]any{"num15", "NUMBER(15, 0)", NewRawTupleValue("NULL")},
		[]any{"small_int", "SMALLINT", NewRawTupleValue("NULL")},
		[]any{"integ", "INTEGER", NewRawTupleValue("NULL")},
		[]any{"double_precision", "DOUBLE PRECISION", NewRawTupleValue("NULL")},
		[]any{"float_126", "FLOAT(126)", NewRawTupleValue("NULL")},
		[]any{"real_num", "REAL", NewRawTupleValue("NULL")},
		[]any{"dateonly", "DATE", NewRawTupleValue("NULL")},
		[]any{"datetime", "DATE", NewRawTupleValue("NULL")},
		[]any{"ts", "TIMESTAMP", NewRawTupleValue("NULL")},
		[]any{"ts_nine", "TIMESTAMP(9)", NewRawTupleValue("NULL")},
		[]any{"ts_tz", "TIMESTAMP WITH TIME ZONE", NewRawTupleValue("NULL")},
		[]any{"ts_tz_nine", "TIMESTAMP(9) WITH TIME ZONE", NewRawTupleValue("NULL")},
		[]any{"ts_local_tz", "TIMESTAMP WITH LOCAL TIME ZONE", NewRawTupleValue("NULL")},
		[]any{"ts_local_tz_nine", "TIMESTAMP(9) WITH LOCAL TIME ZONE", NewRawTupleValue("NULL")},
		[]any{"interval_year", "INTERVAL YEAR(4) TO MONTH", NewRawTupleValue("NULL")},
		[]any{"interval_day", "INTERVAL DAY TO SECOND", NewRawTupleValue("NULL")},
		[]any{"r", "RAW(1000)", NewRawTupleValue("NULL")},
	}

	var columnDefs = "("
	var vals []any
	for idx, tv := range typesAndValues {
		if idx > 0 {
			columnDefs += ", "
		}
		columnDefs += fmt.Sprintf("%s %s", tv[0].(string), tv[1].(string))
		vals = append(vals, tv[2])
		idx += 1
	}
	columnDefs += ")"
	var tableName = tb.CreateTable(ctx, t, unique, columnDefs)

	tb.Insert(ctx, t, tableName, [][]any{vals})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(unique))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(unique))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableName, [][]any{vals})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestLongStrings(t *testing.T) {
	var unique = "18110541"
	var tb, ctx = oracleTestBackend(t), context.Background()
	var fire, ice, normalString string
	for i := 0; i < 250; i++ {
		fire += "🔥️"
		ice += "🧊"
		normalString += fmt.Sprintf("%x%x", i, i)
	}
	var unicode = NewRawTupleValue(fmt.Sprintf("'%s' || '%s'", fire, ice))
	var unicode2 = NewRawTupleValue(fmt.Sprintf("'%s' || '%s'", ice, fire))
	var normal = NewRawTupleValue(fmt.Sprintf("'%s' || '%s'", normalString, normalString))
	var mixed = NewRawTupleValue(fmt.Sprintf("'%s' || '%s'", fire, normalString))
	var tableName = tb.CreateTable(ctx, t, unique, "(UNISTR NVARCHAR2(2000), s NVARCHAR2(2000), UNISTR2 NVARCHAR2(2000), s3 NVARCHAR2(2000), mixed NVARCHAR2(2000))")

	tb.Insert(ctx, t, tableName, [][]any{{unicode, normal, unicode2, normal, mixed}})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(unique))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableName, [][]any{{unicode, normal, unicode2, normal, mixed}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestSkipBackfills(t *testing.T) {
	// Set up three tables with some data in them, a catalog which captures all three,
	// but a configuration which specifies that tables A and C should skip backfilling
	// and only capture new changes.
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB, uniqueC = "18110541", "24310805", "38410024"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableC = tb.CreateTable(ctx, t, uniqueC, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")

	tb.config.Advanced.SkipBackfills = fmt.Sprintf("%s,%s", strings.ReplaceAll(tableA, "\"", ""), strings.ReplaceAll(tableC, "\"", ""))
	tb.Insert(ctx, t, tableA, [][]any{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]any{{4, "four"}, {5, "five"}, {6, "six"}})
	tb.Insert(ctx, t, tableC, [][]any{{7, "seven"}, {8, "eight"}, {9, "nine"}})

	// Run an initial capture, which should capture all three tables but only backfill events from table B
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB), regexp.MustCompile(uniqueC))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Insert additional data and verify that all three tables report new events
	tb.Insert(ctx, t, tableA, [][]any{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	tb.Insert(ctx, t, tableB, [][]any{{13, "thirteen"}, {14, "fourteen"}, {15, "fifteen"}})
	tb.Insert(ctx, t, tableC, [][]any{{16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}})
	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTruncatedTables(t *testing.T) {
	// Set up two tables with some data in them
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB = "14026504", "29415894"
	var tableA = tb.CreateTable(ctx, t, uniqueA, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	var tableB = tb.CreateTable(ctx, t, uniqueB, "(id INTEGER PRIMARY KEY, data VARCHAR(2000))")
	tb.Insert(ctx, t, tableA, [][]any{{1, "one"}, {2, "two"}, {3, "three"}})
	tb.Insert(ctx, t, tableB, [][]any{{4, "four"}, {5, "five"}, {6, "six"}})

	// Set up and run a capture of Table A only
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA))
	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add data to table A and truncate table B. Captures should still succeed because we
	// don't care about truncates to non-active tables.
	tb.Insert(ctx, t, tableA, [][]any{{7, "seven"}, {8, "eight"}, {9, "nine"}})
	tb.Query(ctx, t, true, fmt.Sprintf("TRUNCATE TABLE %s", tableB))
	t.Run("capture1", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Truncating table A will cause the capture to fail though, as it should.
	tb.Query(ctx, t, true, fmt.Sprintf("TRUNCATE TABLE %s", tableA))
	tb.Insert(ctx, t, tableA, [][]any{{10, "ten"}, {11, "eleven"}, {12, "twelve"}})
	t.Run("capture2", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestTrickyColumnNames(t *testing.T) {
	// Create a table with some 'difficult' column names (a reserved word, a capitalized
	// name, and one containing special characters which also happens to be the primary key).
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueA, uniqueB = "39256824", "42531495"
	var tableA = tb.CreateTable(ctx, t, uniqueA, `("`+"`"+`Meta/'wtf'~ID`+"`"+`" INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	var tableB = tb.CreateTable(ctx, t, uniqueB, `("table" INTEGER PRIMARY KEY, data VARCHAR(2000))`)
	tb.Insert(ctx, t, tableA, [][]any{{1, "aaa"}, {2, "bbb"}})
	tb.Insert(ctx, t, tableB, [][]any{{3, "ccc"}, {4, "ddd"}})

	// Discover the catalog and verify that the table schemas looks correct
	t.Run("discover", func(t *testing.T) {
		tb.CaptureSpec(ctx, t).VerifyDiscover(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	})

	// Perform an initial backfill
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB))
	t.Run("backfill", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	// Add more data and read it via replication
	tb.Insert(ctx, t, tableA, [][]any{{5, "eee"}, {6, "fff"}})
	tb.Insert(ctx, t, tableB, [][]any{{7, "ggg"}, {8, "hhh"}})
	t.Run("replication", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

// TestCursorResume sets up a capture with a (string, int) primary key and
// and repeatedly restarts it after each row of capture output.
func TestCursorResume(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "95911555"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(epoch VARCHAR(8), count INTEGER, data VARCHAR(2000), PRIMARY KEY (epoch, count))")
	tb.Insert(ctx, t, tableName, [][]any{
		{"aaa", 1, "bvzf"}, {"aaa", 2, "ukwh"}, {"aaa", 3, "lntg"}, {"bbb", -100, "bycz"},
		{"bbb", 2, "ajgp"}, {"bbb", 333, "zljj"}, {"bbb", 4096, "lhnw"}, {"bbb", 800000, "iask"},
		{"ccc", 1234, "bikh"}, {"ddd", -10000, "dhqc"}, {"x", 1, "djsf"}, {"y", 1, "iwnx"},
		{"z", 1, "qmjp"}, {".", 0, "xakg"}, {".", -1, "kvxr"}, {"   ", 3, "gboj"},
	})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	// Reduce the backfill chunk size to 1 row. Since the capture will be killed and
	// restarted after each scan key update, this means we'll advance over the keys
	// one by one.
	cs.EndpointSpec.(*Config).Advanced.BackfillChunkSize = 1
	var summary, _ = tests.RestartingBackfillCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

func TestCaptureCapitalization(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()

	var uniqueA, uniqueB = "69943814", "73423348"
	var tablePrefix = "test"
	var tableA = tablePrefix + "_AaAaA_" + uniqueA                  // Name containing capital letters
	var tableB = strings.ToUpper(tablePrefix + "_BbBbB_" + uniqueB) // Name which is all uppercase (like all our other test table names)

	var cleanup = func() {
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."%s"`, tb.config.User, tableA))
		tb.Query(ctx, t, false, fmt.Sprintf(`DROP TABLE "%s"."%s"`, tb.config.User, tableB))
	}
	cleanup()
	t.Cleanup(cleanup)

	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data VARCHAR(2000))`, tb.config.User, tableA))
	tb.Query(ctx, t, true, fmt.Sprintf(`CREATE TABLE "%s"."%s" (id INTEGER PRIMARY KEY, data VARCHAR(2000))`, tb.config.User, tableB))

	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (0, 'hello')`, tb.config.User, tableA))
	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (1, 'asdf')`, tb.config.User, tableA))
	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (2, 'world')`, tb.config.User, tableB))
	tb.Query(ctx, t, true, fmt.Sprintf(`INSERT INTO "%s"."%s" VALUES (3, 'fdsa')`, tb.config.User, tableB))

	tests.VerifiedCapture(ctx, t, tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueA), regexp.MustCompile(uniqueB)))
}

func TestSchemaChanges(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "83287013"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2000), fullname VARCHAR(2000), population INTEGER, PRIMARY KEY (year, state))")
	tb.Insert(ctx, t, tableName, [][]any{{1900, "AA", "No Such State", 20000}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]any{{1930, "BB", "No Such State", 10000}})

	tb.Query(ctx, t, true, fmt.Sprintf("ALTER TABLE %s DROP COLUMN population", tableName))

	tb.Query(ctx, t, true, fmt.Sprintf("UPDATE %s SET fullname = 'New ' || fullname WHERE state IN ('NJ', 'NY')", tableName))
	tb.Query(ctx, t, true, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX' AND year = 1970", tableName))

	tb.Insert(ctx, t, tableName, [][]any{{1940, "CC", "No Such State"}})

	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}

func TestSchemaChangesOnlineDictionary(t *testing.T) {
	var tb, ctx = oracleTestBackend(t), context.Background()
	var uniqueID = "83287013"
	var tableName = tb.CreateTable(ctx, t, uniqueID, "(year INTEGER, state VARCHAR(2000), fullname VARCHAR(2000), population INTEGER, PRIMARY KEY (year, state))")
	tb.Insert(ctx, t, tableName, [][]any{{1900, "AA", "No Such State", 20000}})
	var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

	cs.EndpointSpec.(*Config).Advanced.DictionaryMode = "online"

	t.Run("init", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })

	tb.Insert(ctx, t, tableName, [][]any{{1930, "BB", "No Such State", 10000}})

	tb.Query(ctx, t, true, fmt.Sprintf("ALTER TABLE %s DROP COLUMN population", tableName))

	tb.Query(ctx, t, true, fmt.Sprintf("UPDATE %s SET fullname = 'New ' || fullname WHERE state IN ('NJ', 'NY')", tableName))
	tb.Query(ctx, t, true, fmt.Sprintf("DELETE FROM %s WHERE state = 'XX' AND year = 1970", tableName))

	tb.Insert(ctx, t, tableName, [][]any{{1940, "CC", "No Such State"}})

	t.Run("main", func(t *testing.T) { tests.VerifiedCapture(ctx, t, cs) })
}
