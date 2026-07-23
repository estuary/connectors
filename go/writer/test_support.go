package writer

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	iso8601 "github.com/senseyeio/duration"
	"github.com/stretchr/testify/require"
)

// DuckDBDockerImage pins the DuckDB used in tests as an independent reader of
// parquet variant encoding and Iceberg v3 tables, here and in the connectors
// that build on this writer. DuckDB 1.5.3 is the earliest release that reads
// variant columns.
const DuckDBDockerImage = "duckdb/duckdb:1.5.4"

// duckdbDockerQueryFile runs query against the parquet file f using a pinned
// dockerized DuckDB, substituting the container-side path of f for %s in
// query, and returns the result rows as JSON. Unlike duckdbReadFile's
// in-process DuckDB, this is an independent reader whose version isn't tied
// to the duckdb-go dependency pinned in go.mod.
func duckdbDockerQueryFile(t *testing.T, f string, query string) string {
	dir, name := filepath.Split(f)

	// The file and its host directory must be readable from inside the
	// container regardless of the container's user.
	require.NoError(t, os.Chmod(dir, 0o755))
	require.NoError(t, os.Chmod(f, 0o644))

	// Capture stdout only: docker writes image-pull progress to stderr when
	// the pinned image isn't cached (as on a fresh CI runner), which must not
	// pollute the query result that callers snapshot.
	cmd := exec.Command("docker", "run", "--rm",
		"-v", dir+":/data:ro",
		DuckDBDockerImage,
		"duckdb", "-json", "-c", fmt.Sprintf(query, "/data/"+name),
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, "dockerized duckdb query failed: %s", stderr.String())

	return string(out)
}

func duckdbReadFile(t *testing.T, f string, outFormat string) string {
	db, err := sql.Open("duckdb", "") // opens an in-memory database
	require.NoError(t, err)
	defer db.Close()

	bootQueries := []string{
		"INSTALL 'json'",
		"LOAD 'json'",
		"SET TimeZone = 'UTC';",
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
