// databricks-stage-keys writes a list of string keys as a staged file in the
// format materialize-databricks uses (gzipped JSON or parquet) and uploads it
// to a Unity Catalog volume path, for running load-query variants by hand.
//
//	go run ./tests/benchmark/materialize/tools/databricks-stage-keys \
//	  -config materialize-databricks/testdata/config.local.yaml \
//	  -keys keys.txt -field id -format parquet \
//	  -dest /Volumes/<catalog>/<schema>/flow_staging/flow_temp_tables/explain/k.parquet
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"

	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/files"
	"github.com/estuary/connectors/go/writer"
)

func main() {
	var configPath = flag.String("config", "", "sops-encrypted connector config")
	var keysPath = flag.String("keys", "", "file with one key per line")
	var field = flag.String("field", "id", "key column name")
	var format = flag.String("format", "parquet", "json or parquet")
	var dest = flag.String("dest", "", "volume path of the uploaded file")
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

	var local = "/tmp/stage-keys-" + *format
	f, err := os.Create(local)
	if err != nil {
		fail("creating local file: %v", err)
	}
	var w interface {
		Write([]any) error
		Close() error
	}
	if *format == "parquet" {
		w, err = writer.NewParquetWriter(f, writer.ParquetSchema{{Name: *field, DataType: writer.LogicalTypeString}}, writer.WithParquetCompression(writer.Snappy))
		if err != nil {
			fail("parquet writer: %v", err)
		}
	} else {
		w = writer.NewJsonWriter(f, []string{*field})
	}
	in, err := os.Open(*keysPath)
	if err != nil {
		fail("opening keys: %v", err)
	}
	var n int
	for sc := bufio.NewScanner(in); sc.Scan(); {
		if sc.Text() == "" {
			continue
		}
		if err := w.Write([]any{sc.Text()}); err != nil {
			fail("writing key: %v", err)
		}
		n++
	}
	if err := w.Close(); err != nil {
		fail("closing writer: %v", err)
	}
	st, _ := os.Stat(local)

	ws, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       token,
		Credentials: dbConfig.PatCredentials{},
	})
	if err != nil {
		fail("workspace client: %v", err)
	}
	body, err := os.Open(local)
	if err != nil {
		fail("reopening local file: %v", err)
	}
	if err := ws.Files.Upload(context.Background(), files.UploadRequest{FilePath: *dest, Contents: body, Overwrite: true}); err != nil {
		fail("uploading: %v", err)
	}
	fmt.Printf("uploaded %d keys, %d bytes, to %s\n", n, st.Size(), *dest)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
