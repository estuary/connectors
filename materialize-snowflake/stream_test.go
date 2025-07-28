package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestChannelName(t *testing.T) {
	require.Equal(t, "testing_4E7A62CBF3428987_00000000", channelName("testing", 0))
	require.Equal(t, "testing_4E7A62CBF3428987_000004d2", channelName("testing", 1234))
	require.Equal(t, "some_other_stuff__816D5E80F9E632A7_000004d2", channelName("some/other-stuff!", 1234))
	require.Equal(t, "long_long_long_long_long_long_lo_05A28455EE5956EB_00000000", channelName(strings.Repeat("long/", 10), 0))
	require.Equal(t, "long_long_long_long_long_long_lo_135047533004DC65_00000000", channelName(strings.Repeat("long!", 10), 0))
}

func TestStreamManager(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	dsn, err := cfg.toURI("estuary")
	require.NoError(t, err)

	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	verify := func(t *testing.T, query string, want [][]any) {
		t.Helper()

		rows, err := db.Query(query)
		require.NoError(t, err)
		defer rows.Close()

		cols, err := rows.Columns()
		require.NoError(t, err)

		var got [][]any
		for rows.Next() {
			var data = make([]any, len(cols))
			var ptrs = make([]any, len(cols))
			for i := range data {
				ptrs[i] = &data[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			got = append(got, data)
		}

		require.Equal(t, want, got)
	}

	t.Run("lifecycle", func(t *testing.T) {
		table := sql.Table{
			TableShape: sql.TableShape{
				Binding: 0,
			},
			Identifier: `STREAM_TEST`,
			Keys:       []sql.Column{{Identifier: `key`}},
			Values:     []sql.Column{{Identifier: `firstcol`}, {Identifier: `secondcol`}},
			Document:   nil,
		}

		cleanup := func(t *testing.T) {
			t.Helper()
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS STREAM_TEST;")
		}
		defer cleanup(t)

		cleanup(t)
		_, err = db.ExecContext(ctx, `CREATE TABLE STREAM_TEST (key TEXT, firstcol TEXT, secondcol TEXT);`)
		require.NoError(t, err)

		sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)

		require.NoError(t, sm.addBinding(ctx, cfg.Schema, table.Identifier, table))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", "hello1", "world1"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key2", "hello2", "world2"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key3", "hello3", "world3"}))
		blobs, err := sm.flush("the-token-1")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))

		require.NoError(t, sm.write(ctx, blobs[0]))

		verify(t, "SELECT * FROM STREAM_TEST ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
		})

		// Write the same token again, it is not added to the table.
		require.NoError(t, sm.write(ctx, blobs[0]))

		verify(t, "SELECT * FROM STREAM_TEST ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
		})

		// A second invocation invalidates the first.
		ssm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)
		require.NoError(t, ssm.addBinding(ctx, cfg.Schema, table.Identifier, table))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key6", "hello6", "world6"}))
		blobs, err = sm.flush("the-token-2")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))

		// The original invocation will error and not write any data.
		require.Error(t, sm.write(ctx, blobs[0]))
		verify(t, "SELECT * FROM STREAM_TEST ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
		})

		// But the second one will work.
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key6", "hello6", "world6"}))
		blobs, err = sm.flush("the-token-2")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))

		require.NoError(t, ssm.write(ctx, blobs[0]))

		verify(t, "SELECT * FROM STREAM_TEST ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
			{"key4", "hello4", "world4"},
			{"key5", "hello5", "world5"},
			{"key6", "hello6", "world6"},
		})
	})

	t.Run("multiple blobs", func(t *testing.T) {
		table := sql.Table{
			TableShape: sql.TableShape{
				Binding: 0,
			},
			Identifier: `STREAM_TEST`,
			Keys:       []sql.Column{{Identifier: `key`}},
			Values:     []sql.Column{{Identifier: `firstcol`}, {Identifier: `secondcol`}},
			Document:   nil,
		}

		cleanup := func(t *testing.T) {
			t.Helper()
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS STREAM_TEST;")
		}
		defer cleanup(t)

		cleanup(t)
		_, err = db.ExecContext(ctx, `CREATE TABLE STREAM_TEST (key TEXT, firstcol TEXT, secondcol TEXT);`)
		require.NoError(t, err)

		sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)

		require.NoError(t, sm.addBinding(ctx, cfg.Schema, table.Identifier, table))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", "hello1", "world1"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key2", "hello2", "world2"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key3", "hello3", "world3"}))
		require.NoError(t, sm.finishBlob())
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key6", "hello6", "world6"}))
		blobs, err := sm.flush("the-token-1")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 2, len(blobs[0]))

		require.NoError(t, sm.write(ctx, blobs[0]))

		verify(t, "SELECT * FROM STREAM_TEST ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
			{"key4", "hello4", "world4"},
			{"key5", "hello5", "world5"},
			{"key6", "hello6", "world6"},
		})
	})

	t.Run("multiple bindings and transactions", func(t *testing.T) {
		keys := []sql.Column{{Identifier: `key`}}
		values := []sql.Column{{Identifier: `firstcol`}, {Identifier: `secondcol`}}
		tables := []sql.Table{
			{
				TableShape: sql.TableShape{Binding: 0},
				Identifier: `STREAM_TEST_1`,
				Keys:       keys,
				Values:     values,
				Document:   nil,
			},
			{
				TableShape: sql.TableShape{Binding: 1},
				Identifier: `STREAM_TEST_2`,
				Keys:       keys,
				Values:     values,
				Document:   nil,
			},
			{
				TableShape: sql.TableShape{Binding: 2},
				Identifier: `STREAM_TEST_3`,
				Keys:       keys,
				Values:     values,
				Document:   nil,
			},
		}

		cleanup := func(t *testing.T) {
			t.Helper()
			for _, tbl := range tables {
				_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl.Identifier+";")
				require.NoError(t, err)
			}
		}
		defer cleanup(t)

		cleanup(t)
		for _, tbl := range tables {
			_, err = db.ExecContext(ctx, `CREATE TABLE `+tbl.Identifier+` (key TEXT, firstcol TEXT, secondcol TEXT);`)
			require.NoError(t, err)
		}

		sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)

		for _, tbl := range tables {
			require.NoError(t, sm.addBinding(ctx, cfg.Schema, tbl.Identifier, tbl))
		}

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", "hello1", "world1"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key2", "hello2", "world2"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key3", "hello3", "world3"}))
		blobs, err := sm.flush("the-token-1")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))
		require.NoError(t, sm.write(ctx, blobs[0]))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key6", "hello6", "world6"}))
		require.NoError(t, sm.writeRow(ctx, 1, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 1, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 1, []any{"key6", "hello6", "world6"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key4", "hello4", "world4"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key5", "hello5", "world5"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key6", "hello6", "world6"}))
		blobs, err = sm.flush("the-token-2")
		require.NoError(t, err)
		require.Equal(t, 3, len(blobs))
		for _, blob := range blobs {
			require.Equal(t, 1, len(blob))
			require.NoError(t, sm.write(ctx, blob))
		}

		// Noop commit.
		blobs, err = sm.flush("the-token-3")
		require.NoError(t, err)
		require.Equal(t, 0, len(blobs))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key7", "hello7", "world7"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key8", "hello8", "world8"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key9", "hello9", "world9"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key7", "hello7", "world7"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key8", "hello8", "world8"}))
		require.NoError(t, sm.writeRow(ctx, 2, []any{"key9", "hello9", "world9"}))
		blobs, err = sm.flush("the-token-3")
		require.NoError(t, err)
		require.Equal(t, 2, len(blobs))
		for _, blob := range blobs {
			require.Equal(t, 1, len(blob))
			require.NoError(t, sm.write(ctx, blob))
		}

		verify(t, "SELECT * FROM "+tables[0].Identifier+" ORDER BY key", [][]any{
			{"key1", "hello1", "world1"},
			{"key2", "hello2", "world2"},
			{"key3", "hello3", "world3"},
			{"key4", "hello4", "world4"},
			{"key5", "hello5", "world5"},
			{"key6", "hello6", "world6"},
			{"key7", "hello7", "world7"},
			{"key8", "hello8", "world8"},
			{"key9", "hello9", "world9"},
		})

		verify(t, "SELECT * FROM "+tables[1].Identifier+" ORDER BY key", [][]any{
			{"key4", "hello4", "world4"},
			{"key5", "hello5", "world5"},
			{"key6", "hello6", "world6"},
		})

		verify(t, "SELECT * FROM "+tables[2].Identifier+" ORDER BY key", [][]any{
			{"key4", "hello4", "world4"},
			{"key5", "hello5", "world5"},
			{"key6", "hello6", "world6"},
			{"key7", "hello7", "world7"},
			{"key8", "hello8", "world8"},
			{"key9", "hello9", "world9"},
		})
	})

	t.Run("non-selected columns and quoted columns", func(t *testing.T) {
		keys := []sql.Column{{Identifier: `key`}}
		values := []sql.Column{
			{Identifier: `" Column WiTh ""quotes"" and ""\""spaces"""" ,;{}().-� 𐀀 嶲 "`},
			{Identifier: `"1234startsWithNumbers"`},
			{Identifier: `"$dollar$signs.here#andotherstuff"`},
		}
		tbl := sql.Table{
			TableShape: sql.TableShape{Binding: 0},
			Identifier: `STREAM_TEST_QUOTED_COLUMNS`,
			Keys:       keys,
			Values:     values,
			Document:   nil,
		}

		cleanup := func(t *testing.T) {
			t.Helper()
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tbl.Identifier+";")
			require.NoError(t, err)
		}
		defer cleanup(t)

		cleanup(t)

		createQuery := `
		CREATE TABLE STREAM_TEST_QUOTED_COLUMNS(
			someothercolumn TEXT,
			key TEXT NOT NULL,
			" Column WiTh ""quotes"" and ""\""spaces"""" ,;{}().-� 𐀀 嶲 " TEXT NOT NULL,
			yetanothercolumn TEXT,
			"1234startsWithNumbers" TEXT NOT NULL,
			"$dollar$signs.here#andotherstuff" TEXT NOT NULL,
			"finalColumn" TEXT
		);
		`

		_, err = db.ExecContext(ctx, createQuery)
		require.NoError(t, err)

		sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)
		require.NoError(t, sm.addBinding(ctx, cfg.Schema, tbl.Identifier, tbl))

		require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", "hello1", "goodbye1", "aloha1"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key2", "hello2", "goodbye2", "aloha2"}))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key3", "hello3", "goodbye3", "aloha3"}))
		blobs, err := sm.flush("token")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))
		require.NoError(t, sm.write(ctx, blobs[0]))

		verify(t, "SELECT * FROM "+tbl.Identifier+" ORDER BY key", [][]any{
			{nil, "key1", "hello1", nil, "goodbye1", "aloha1", nil},
			{nil, "key2", "hello2", nil, "goodbye2", "aloha2", nil},
			{nil, "key3", "hello3", nil, "goodbye3", "aloha3", nil},
		})
	})

	t.Run("large VARIANT columns", func(t *testing.T) {
		type hugeThing struct {
			Val string
		}
		fixture := hugeThing{
			Val: strings.Repeat("a", 32*1024*1024),
		}
		data, err := json.Marshal(fixture)
		require.NoError(t, err)

		table := sql.Table{
			TableShape: sql.TableShape{
				Binding: 0,
			},
			Identifier: `STREAM_TEST`,
			Keys:       []sql.Column{{Identifier: `key`}},
			Values:     []sql.Column{{Identifier: `firstcol`}},
			Document:   nil,
		}

		cleanup := func(t *testing.T) {
			t.Helper()
			_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS STREAM_TEST;")
		}
		defer cleanup(t)

		cleanup(t)
		_, err = db.ExecContext(ctx, `CREATE TABLE STREAM_TEST (key TEXT, firstcol VARIANT);`)
		require.NoError(t, err)

		{
			// With enforceVariantMaxLength set to true, a large VARIANT value
			// can't be written.
			sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, true)
			require.NoError(t, err)
			require.NoError(t, sm.addBinding(ctx, cfg.Schema, table.Identifier, table))
			require.Error(t, sm.writeRow(ctx, 0, []any{"key1", json.RawMessage(data)}))
		}

		// But it can be written and read when enforceVariantMaxLength is false.
		sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
		require.NoError(t, err)
		require.NoError(t, sm.addBinding(ctx, cfg.Schema, table.Identifier, table))
		require.NoError(t, sm.writeRow(ctx, 0, []any{"key1", json.RawMessage(data)}))
		blobs, err := sm.flush("the-token-1")
		require.NoError(t, err)
		require.Equal(t, 1, len(blobs))
		require.Equal(t, 1, len(blobs[0]))

		require.NoError(t, sm.write(ctx, blobs[0]))

		var gotData string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT firstcol FROM STREAM_TEST").Scan(&gotData))

		var got hugeThing
		require.NoError(t, json.Unmarshal([]byte(gotData), &got))
		require.Equal(t, fixture, got)
	})
}

func TestStreamDatatypes(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	dsn, err := cfg.toURI("estuary")
	require.NoError(t, err)

	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var templates = renderTemplates(testDialect)

	testTableBaseName := "STREAM_TEST_DATATYPES"

	timestamps := []any{
		"2022-08-17T04:32:19.987654321Z",
		"2022-08-17T04:32:19.987654321+02:00",
		"2022-08-17T04:32:19.987654321+03:30",
		"2022-08-17T04:32:19.987654321-07:00",
		"0000-01-01T00:00:00.000000000Z",
		"0000-01-01T00:00:00.000000000+23:59",
		"0000-01-01T00:00:00.000000000-23:59",
		"9999-12-31T23:59:59.999999999Z",
		"9999-12-31T23:59:59.999999999+23:59",
		"9999-12-31T23:59:59.999999999-23:59",
		nil,
	}

	for _, tt := range []struct {
		ddl  string
		vals []any
	}{
		{
			ddl:  "INTEGER",
			vals: []any{-1, 0, 1, 1234, math.MaxInt64, nil, math.MinInt64, float64(math.MaxUint64) * 10_000, minSnowflakeInteger.BigInt(), maxSnowflakeInteger.BigInt(), uint64(math.MaxUint64)},
		},
		{
			ddl:  "TEXT",
			vals: []any{"hello", "", nil, 1234, true, uint64(4321), 1234.1234, []byte("[1,2,3]"), json.RawMessage([]byte(`{"some":"doc"}`))},
		},
		{
			ddl:  "VARIANT",
			vals: []any{`"hello"`, `""`, nil, 1234, true, uint64(4321), 1234.1234, []byte("[1,2,3]"), json.RawMessage([]byte(`{"some":"doc"}`))},
		},
		{
			ddl:  "BOOLEAN",
			vals: []any{true, false, nil},
		},
		{
			ddl:  "FLOAT",
			vals: []any{int64(1234), nil, 1234.0, 4321.1234, math.MaxFloat64, math.SmallestNonzeroFloat64, "NaN", "inf", "-inf"},
		},
		{
			ddl:  "DATE",
			vals: []any{"2022-03-21", nil, "0000-01-01", "9999-12-31"},
		},
		{
			ddl:  "TIMESTAMP_LTZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_TZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_NTZ",
			vals: timestamps,
		},
	} {
		t.Run(tt.ddl, func(t *testing.T) {
			tbl := sql.Table{
				TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
				Identifier: testTableBaseName + "_" + tt.ddl,
				Keys:       []sql.Column{{Identifier: `key`, MappedType: sql.MappedType{DDL: "INTEGER"}}},
				Values:     []sql.Column{{Identifier: `val`, MappedType: sql.MappedType{DDL: tt.ddl}}},
				Document:   nil,
			}
			cleanup := func(t *testing.T) {
				t.Helper()
				_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tbl.Identifier))
			}
			defer cleanup(t)

			cleanup(t)

			var createQuery strings.Builder
			require.NoError(t, templates.createTargetTable.Execute(&createQuery, &tbl))

			_, err = db.ExecContext(ctx, createQuery.String())
			require.NoError(t, err)

			sm, err := newStreamManager(&cfg, "testing", "testing", accountName, 0, false)
			require.NoError(t, err)

			require.NoError(t, sm.addBinding(ctx, cfg.Schema, tbl.Identifier, tbl))

			for i, val := range tt.vals {
				require.NoError(t, sm.writeRow(ctx, 0, []any{i, val}))
			}

			blobs, err := sm.flush("token")
			require.NoError(t, err)
			require.Equal(t, 1, len(blobs))
			require.Equal(t, 1, len(blobs[0]))
			require.NoError(t, sm.write(ctx, blobs[0]))

			var snap strings.Builder
			eps, err := json.Marshal(blobs[0][0].Chunks[0].EPS)
			require.NoError(t, err)
			snap.WriteString("--- Column Statistics ---\n")
			snap.Write(eps)

			got, err := sql.StdDumpTable(ctx, db, tbl)
			require.NoError(t, err)
			snap.WriteString("\n\n--- Column Values ---\n")
			snap.WriteString(got)

			cupaloy.SnapshotT(t, snap.String())
		})
	}
}

func TestGetNextFileName(t *testing.T) {
	calendar, err := time.Parse(time.RFC3339Nano, "2025-04-03T13:40:36.434227277Z")
	require.NoError(t, err)

	clientPrefix := "asdfasdf_1234"

	sm := &streamManager{keyBegin: 5678}

	for idx := range 5 {
		require.Equal(t,
			fmt.Sprintf("2025/4/3/13/40/su59zo_asdfasdf_1234_5678_%d.bdec", idx),
			string(sm.getNextFileName(calendar, clientPrefix)),
		)
	}
}

func TestShouldWriteNextToken(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	for _, tt := range []struct {
		name         string
		blobToken    string
		currentToken *string
		want         bool
		wantErr      bool
	}{
		{
			name:         "no current token",
			blobToken:    "the-token-1:0",
			currentToken: nil,
			want:         true,
			wantErr:      false,
		},
		{
			name:         "same token",
			blobToken:    "the-token-1:0",
			currentToken: strPtr("the-token-1:0"),
			want:         false,
			wantErr:      false,
		},
		{
			name:         "next in sequence",
			blobToken:    "the-token-1:2",
			currentToken: strPtr("the-token-1:1"),
			want:         true,
			wantErr:      false,
		},
		{
			name:         "prior in sequence",
			blobToken:    "the-token-1:1",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      false,
		},
		{
			name:         "start new token sequence",
			blobToken:    "the-token-2:0",
			currentToken: strPtr("the-token-1:2"),
			want:         true,
			wantErr:      false,
		},
		{
			name:         "wrong token",
			blobToken:    "the-token-2:1",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      true,
		},
		{
			name:         "malformed current",
			blobToken:    "the-token-2:1",
			currentToken: strPtr("wrong:asdf"),
			want:         false,
			wantErr:      true,
		},
		{
			name:         "malformed blob",
			blobToken:    "wrong",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shouldWriteNextToken(tt.blobToken, tt.currentToken)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidWriteBlobs(t *testing.T) {
	makeChunkMeta := func(db, sch, tbl, ch, tok string) uploadChunkMetadata {
		return uploadChunkMetadata{
			Database: db,
			Schema:   sch,
			Table:    tbl,
			Channels: []uploadChunkChannelMetadata{{Channel: ch, OffsetToken: tok}},
		}
	}

	db := "db"
	sch := "sch"
	tbl := "tbl"
	ch := "ch"

	for _, tt := range []struct {
		name  string
		blobs []*blobMetadata
		valid bool
	}{
		{
			name: "single blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
			},
			valid: true,
		},
		{
			name: "multi blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: true,
		},
		{
			name: "wrong base token",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "other:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "out of order",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
			},
			valid: false,
		},
		{
			name: "more than one chunk in a single blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{
					makeChunkMeta(db, sch, tbl, ch, "token:1"),
					makeChunkMeta(db, sch, tbl, ch, "token:2"),
				}},
			},
			valid: false,
		},
		{
			name: "more than one channel in a single chunk",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{
					{
						Database: db,
						Schema:   sch,
						Table:    tbl,
						Channels: []uploadChunkChannelMetadata{
							{Channel: ch, OffsetToken: "token:1"},
							{Channel: ch, OffsetToken: "token:2"},
						},
					},
				}},
			},
			valid: false,
		},
		{
			name: "different databases",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db+"other", sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different schemas",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch+"other", tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different tables",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl+"other", ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different channels",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch+"other", "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "malformed token",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{{Channels: []uploadChunkChannelMetadata{{OffsetToken: "asdf"}}}}},
			},
			valid: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validWriteBlobs(tt.blobs)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
