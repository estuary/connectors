package stream_encode

import (
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
	iso8601 "github.com/senseyeio/duration"
	"github.com/stretchr/testify/require"
)

func duckdbReadFile(t *testing.T, f string, outFormat string) string {
	db, err := sql.Open("duckdb", "") // opens an in-memory database
	require.NoError(t, err)
	defer db.Close()

	bootQueries := []string{
		"INSTALL 'json'",
		"LOAD 'json'",
	}
	for _, q := range bootQueries {
		_, err := db.Exec(q)
		require.NoError(t, err)
	}

	outFile := path.Join(t.TempDir(), "out")

	_, err = db.Exec(fmt.Sprintf("COPY (SELECT * FROM '%s') TO '%s' (FORMAT %s)", f, outFile, outFormat))
	require.NoError(t, err)

	sb, err := os.ReadFile(outFile)
	require.NoError(t, err)

	return string(sb)
}

func makeTestFields() []string {
	return []string{
		"intField",
		"numField",
		"boolField",
		"binaryField",
		"stringField",
		"uuidField",
		"jsonField",
		"dateField",
		"timeField",
		"timestampField",
		"intervalField",
	}
}

func makeTestRow(t testing.TB, seed int) []any {
	// Avoid weirdness with math operations with a seed value of 0.
	seed += 1

	var boolVal bool
	if seed%2 == 0 {
		boolVal = true
	}

	genString := fmt.Sprintf("str_%d", seed)

	tm, err := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z")
	require.NoError(t, err)
	tm = tm.AddDate(seed, seed, seed).Add(time.Duration(seed) * 1500 * time.Millisecond)

	interval := iso8601.Duration{
		Y:  seed,
		M:  seed % 12,
		W:  seed % 4,
		D:  seed % 30,
		TH: seed % 24,
		TM: seed % 60,
		TS: seed % 60,
	}

	jsonBytes, err := json.Marshal(map[string]any{
		"first":  seed*10_000 + 1,
		"second": seed*10_000 + 2,
		"third":  seed*10_000 + 3,
	})
	require.NoError(t, err)

	md5hash := md5.New()
	md5hash.Write([]byte(genString))
	md5string := hex.EncodeToString(md5hash.Sum(nil))
	genUuid, err := uuid.FromBytes([]byte(md5string[0:16]))
	require.NoError(t, err)

	var row = []any{
		seed,
		float64(seed) / 1.1,
		boolVal,
		base64.StdEncoding.EncodeToString([]byte(genString)),
		genString,
		genUuid.String(),
		json.RawMessage(jsonBytes),
		tm.Format(time.DateOnly),
		tm.Format("15:04:05.999999999Z07:00"),
		tm.Format(time.RFC3339Nano),
		interval.String(),
	}

	return row
}

func makeTestParquetSchema(required bool) ParquetSchema {
	return []ParquetSchemaElement{
		{Name: "intField", DataType: PrimitiveTypeInteger, Required: required},
		{Name: "numField", DataType: PrimitiveTypeNumber, Required: required},
		{Name: "boolField", DataType: PrimitiveTypeBoolean, Required: required},
		{Name: "binaryField", DataType: PrimitiveTypeBinary, Required: required},
		{Name: "stringField", DataType: LogicalTypeString, Required: required},
		{Name: "uuidField", DataType: LogicalTypeUuid, Required: required},
		{Name: "jsonField", DataType: LogicalTypeJson, Required: required},
		{Name: "dateField", DataType: LogicalTypeDate, Required: required},
		{Name: "timeField", DataType: LogicalTypeTime, Required: required},
		{Name: "timestampField", DataType: LogicalTypeTimestamp, Required: required},
		{Name: "intervalField", DataType: LogicalTypeInterval, Required: required},
	}
}
