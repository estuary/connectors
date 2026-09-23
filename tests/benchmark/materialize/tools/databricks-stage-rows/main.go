// databricks-stage-rows stages the rows of one benchmark transaction as
// materialize-databricks would, in gzipped JSON, gzipped CSV or parquet, and
// uploads the 128MB files to a Unity Catalog volume directory. The rows follow
// the uuidv7-recency scenario: keys are the generator's uuid_ordered keys, a
// leading fraction of them updates the newest 40% of an existing table of N
// rows, the rest are fresh keys above N.
//
//	go run ./tests/benchmark/materialize/tools/databricks-stage-rows \
//	  -config materialize-databricks/testdata/config.local.yaml \
//	  -rows 2621440 -existing 2621440 -update-fraction 0.25 -format csv \
//	  -dest /Volumes/<catalog>/<schema>/flow_staging/flow_temp_tables/explain/csv-rows
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/files"
	"github.com/estuary/connectors/go/writer"
	"github.com/google/uuid"
)

const fileSizeLimit = 128 * 1024 * 1024

// Same constants as tests/benchmark/materialize/generate.py.
var uuidNamespace = uuid.MustParse("a3f2b8c1-7d4e-4f9a-b6c8-1e2d3f4a5b6c")

const uuidOrderedEpochMs = 1_767_225_600_000
const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func orderedUUID(key int64) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(key))
	tail := uuid.NewSHA1(uuidNamespace, b[:])
	hi := binary.BigEndian.Uint64(tail[:8])
	lo := binary.BigEndian.Uint64(tail[8:])
	ts := uint64(uuidOrderedEpochMs + key)
	var out uuid.UUID
	binary.BigEndian.PutUint64(out[:8], ts<<16|0x7000|(hi&0x0FFF))
	binary.BigEndian.PutUint64(out[8:], 0x8000000000000000|(lo&0x3FFFFFFFFFFFFFFF))
	return out.String()
}

type rowWriter interface {
	Write([]any) error
	Written() int
	Close() error
}

func main() {
	var configPath = flag.String("config", "", "sops-encrypted connector config")
	var rows = flag.Int64("rows", 2621440, "rows to stage")
	var existing = flag.Int64("existing", 2621440, "rows in the target table (keys 0..existing-1)")
	var updateFraction = flag.Float64("update-fraction", 0.25, "fraction of rows that update keys from the newest 40% of the table")
	var format = flag.String("format", "parquet", "json, csv or parquet")
	var dest = flag.String("dest", "", "volume directory for the uploaded files")
	flag.Parse()

	raw, err := exec.Command("sops", "-d", "--output-type", "json", *configPath).Output()
	if err != nil {
		fail("decrypting config: %v", err)
	}
	var cfg struct {
		Address     string `json:"address"`
		HTTPPath    string `json:"http_path"`
		Credentials struct {
			Token     string `json:"personal_access_token"`
			TokenSops string `json:"personal_access_token_sops"`
		} `json:"credentials"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		fail("parsing config: %v", err)
	}
	var token = cfg.Credentials.Token
	if token == "" {
		token = cfg.Credentials.TokenSops
	}
	ws, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host: fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath), Token: token, Credentials: dbConfig.PatCredentials{},
	})
	if err != nil {
		fail("workspace client: %v", err)
	}
	ctx := context.Background()
	if err := ws.Files.CreateDirectory(ctx, files.CreateDirectoryRequest{DirectoryPath: *dest}); err != nil {
		fail("creating directory: %v", err)
	}

	var fields = []string{"id", "_meta/op", "flow_published_at", "payload", "val", "flow_document", "_flow_delete"}
	var schema = writer.ParquetSchema{
		{Name: "id", DataType: writer.LogicalTypeString},
		{Name: "_meta/op", DataType: writer.LogicalTypeString},
		{Name: "flow_published_at", DataType: writer.LogicalTypeString},
		{Name: "payload", DataType: writer.LogicalTypeString},
		{Name: "val", DataType: writer.PrimitiveTypeInteger},
		{Name: "flow_document", DataType: writer.LogicalTypeString},
		{Name: "_flow_delete", DataType: writer.PrimitiveTypeBoolean},
	}
	var ext = map[string]string{"json": ".json.gz", "csv": ".csv.gz", "parquet": ".parquet"}[*format]
	if ext == "" {
		fail("unknown format %q", *format)
	}

	// Keys: updates are a deterministic sample of the newest 40% of the table.
	var updates = int64(float64(*rows) * *updateFraction)
	var bandStart = int64(float64(*existing) * 0.6)
	var perm = rand.New(rand.NewSource(0)).Perm(int(*existing - bandStart))
	keyOf := func(i int64) (int64, string) {
		if i < updates {
			return bandStart + int64(perm[i]), "u"
		}
		return *existing + (i - updates), "c"
	}

	uploads := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var uploaded []string
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for local := range uploads {
				f, err := os.Open(local)
				if err != nil {
					fail("opening %s: %v", local, err)
				}
				remote := filepath.Join(*dest, filepath.Base(local))
				if err := ws.Files.Upload(ctx, files.UploadRequest{FilePath: remote, Contents: f, Overwrite: true}); err != nil {
					fail("uploading %s: %v", remote, err)
				}
				os.Remove(local)
				mu.Lock()
				uploaded = append(uploaded, remote)
				mu.Unlock()
			}
		}()
	}

	var w rowWriter
	var local string
	var n int
	newFile := func() {
		n++
		local = filepath.Join(os.TempDir(), fmt.Sprintf("stage-rows-%s-%03d%s", *format, n, ext))
		f, err := os.Create(local)
		if err != nil {
			fail("creating %s: %v", local, err)
		}
		switch *format {
		case "json":
			w = writer.NewJsonWriter(f, fields)
		case "csv":
			w = writer.NewCsvWriter(f, fields)
		default:
			pw, err := writer.NewParquetWriter(f, schema, writer.WithParquetCompression(writer.Snappy),
				writer.WithParquetRowGroupByteLimit(32*1024*1024), writer.WithParquetBufferSize(8*1024*1024))
			if err != nil {
				fail("parquet writer: %v", err)
			}
			w = pw
		}
	}
	finishFile := func() {
		if err := w.Close(); err != nil {
			fail("closing %s: %v", local, err)
		}
		uploads <- local
		w = nil
	}

	var payload = make([]byte, 3900)
	var start = time.Now()
	for i := int64(0); i < *rows; i++ {
		if w == nil {
			newFile()
		}
		key, op := keyOf(i)
		id := orderedUUID(key)
		rng := rand.New(rand.NewSource(key))
		for j := range payload {
			payload[j] = alphabet[rng.Intn(len(alphabet))]
		}
		published := time.Unix(1_767_225_600+i, 0).UTC().Format("2006-01-02T15:04:05Z")
		doc, _ := json.Marshal(map[string]any{
			"_meta": map[string]string{"op": op, "uuid": uuid.NewSHA1(uuidNamespace, []byte(id)).String()},
			"id":    id, "payload": string(payload), "val": key,
		})
		if err := w.Write([]any{id, op, published, string(payload), key, json.RawMessage(doc), false}); err != nil {
			fail("writing row: %v", err)
		}
		if w.Written() >= fileSizeLimit {
			finishFile()
		}
	}
	if w != nil {
		finishFile()
	}
	close(uploads)
	wg.Wait()
	fmt.Printf("%s: %d rows in %d files, %s, updates=%d fresh=%d\n", *format, *rows, len(uploaded), time.Since(start).Round(time.Second), updates, *rows-updates)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
