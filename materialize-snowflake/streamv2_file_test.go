package connector

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestStreamV2FileKey(t *testing.T) {
	var raw = bytes.Repeat([]byte{1}, 32)
	var key, err = streamV2FileKey(base64.StdEncoding.EncodeToString(raw))
	require.NoError(t, err)
	require.Equal(t, raw, key)

	_, err = streamV2FileKey("not base64!")
	require.ErrorContains(t, err, "decoding the pipe key")
}

func TestStreamV2CryptFileRoundTrips(t *testing.T) {
	var key = bytes.Repeat([]byte{2}, 16)
	var iv = bytes.Repeat([]byte{3}, 16)
	// Larger than one chunk, so that the keystream carries across chunks.
	var plain = bytes.Repeat([]byte(`{"ID":"a"}`+"\n"), 10_000)

	var path = filepath.Join(t.TempDir(), "batch")
	require.NoError(t, os.WriteFile(path, plain, 0o600))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer file.Close()

	sum, err := streamV2CryptFile(key, iv, file, int64(len(plain)))
	require.NoError(t, err)
	encrypted, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEqual(t, plain, encrypted)
	var want = md5.Sum(encrypted)
	require.Equal(t, want[:], sum)

	// The keystream is the one a single pass over the whole file produces.
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	var oneShot = make([]byte, len(plain))
	cipher.NewCTR(block, iv).XORKeyStream(oneShot, plain)
	require.Equal(t, oneShot, encrypted)

	_, err = streamV2CryptFile(key, iv, file, int64(len(plain)))
	require.NoError(t, err)
	decrypted, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, plain, decrypted)
}

func TestStreamV2UploaderRejectsAnUnknownLocationType(t *testing.T) {
	var _, err = newStreamV2Uploader(context.Background(), streamV2StageLocation{LocationType: "FAKE"})
	require.ErrorContains(t, err, `location type "FAKE"`)
}

// TestStreamV2S3OptionsFollowUseFIPS covers an S3 stage that asks for FIPS: its
// client sends requests to the bucket's FIPS endpoint.
func TestStreamV2S3OptionsFollowUseFIPS(t *testing.T) {
	for _, useFIPS := range []bool{false, true} {
		var host string
		var client = s3.New(s3.Options{
			Region:      "us-gov-west-1",
			Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
			HTTPClient: streamV2DoerFunc(func(r *http.Request) (*http.Response, error) {
				host = r.URL.Host
				return nil, errors.New("not sent")
			}),
			RetryMaxAttempts: 1,
		}, streamV2S3Options(streamV2StageLocation{LocationType: "S3", UseFIPS: useFIPS})...)
		var _, err = client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("stage"), Key: aws.String("k")})
		require.ErrorContains(t, err, "not sent")
		require.Equal(t, useFIPS, strings.Contains(host, "s3-fips."), host)
	}
}

type streamV2DoerFunc func(*http.Request) (*http.Response, error)

func (f streamV2DoerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// TestStreamV2FileModeLive drives file mode against live Snowflake: a document
// whose compressed size exceeds a rows request is stored through the pipe's stage,
// encrypted under the pipe's key, and reads back whole.
func TestStreamV2FileModeLive(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)

	var dsn, err = cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	singleChannelLayout(t)
	sm, err := newStreamManager(&cfg, "test/streamV2FileMode", accountName, 0)
	require.NoError(t, err)
	m, err := newStreamV2Manager(&cfg, db, testDialect, sm, "test/streamV2FileMode", accountName,
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	require.NoError(t, err)

	var tbl = sql.Table{
		TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
		Identifier: "STREAMV2_FILEMODE_" + testRunNonce,
		Keys:       []sql.Column{{Identifier: `KEY`, MappedType: sql.MappedType{DDL: "INTEGER"}}},
		Values:     []sql.Column{{Identifier: `VAL`, MappedType: sql.MappedType{DDL: "TEXT"}}},
		StateKey:   "filemode",
	}
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tbl.Identifier))
	}
	cleanup()
	t.Cleanup(cleanup)

	var createQuery strings.Builder
	require.NoError(t, renderTemplates(testDialect).createTargetTable.Execute(&createQuery, &tbl))
	_, err = db.ExecContext(ctx, createQuery.String())
	require.NoError(t, err)

	// Random letters compress to about three quarters of their size, so 6 MiB of
	// them stays over the 4 MiB limit of a rows request.
	var big = make([]byte, 6*1024*1024)
	for i := range big {
		big[i] = 'a' + byte(rand.IntN(26))
	}
	m.addBinding(cfg.Database, cfg.Schema, tbl.Identifier, tbl, nil)
	require.NoError(t, m.writeRow(ctx, 0, packKey(1), []any{1, "small"}))
	require.NoError(t, m.writeRow(ctx, 0, packKey(2), []any{2, string(big)}))
	require.NoError(t, m.writeRow(ctx, 0, packKey(3), []any{3, "after"}))

	entries, err := m.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), soleCheckpointItem(t, entries, 0).Routed)

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT KEY, LENGTH(VAL), MD5(VAL) FROM %s ORDER BY KEY", tbl.Identifier))
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var key, length int
		var sum string
		require.NoError(t, rows.Scan(&key, &length, &sum))
		got = append(got, fmt.Sprintf("%d:%d:%s", key, length, sum))
	}
	require.Equal(t, []string{
		fmt.Sprintf("1:5:%x", md5.Sum([]byte("small"))),
		fmt.Sprintf("2:%d:%x", len(big), md5.Sum(big)),
		fmt.Sprintf("3:5:%x", md5.Sum([]byte("after"))),
	}, got)
}

// TestStreamV2RedactURLQuery covers a stage upload that fails before reaching the
// service, whose error repeats the request URL with an Azure SAS token as its query.
func TestStreamV2RedactURLQuery(t *testing.T) {
	var err error = &url.Error{
		Op:  "Put",
		URL: "https://acct.blob.core.windows.net/c/k?sig=SECRETSIG&sv=1",
		Err: fmt.Errorf("dial tcp: connection refused"),
	}
	err = fmt.Errorf("uploading: %w", streamV2RedactURLQuery(err))
	require.NotContains(t, err.Error(), "SECRETSIG")
	require.Contains(t, err.Error(), `"https://acct.blob.core.windows.net/c/k?REDACTED"`)
}

// TestStreamV2PipeInfoRequiresBase64Binary covers the binary input format a pipe
// reports. Binary values are written as base64, so a pipe that reads them in any
// other format is refused before a file is appended to it.
func TestStreamV2PipeInfoRequiresBase64Binary(t *testing.T) {
	for _, format := range []string{"", "BASE64", "HEX"} {
		t.Run(format, func(t *testing.T) {
			var c = newTestStreamV2Client(t, func(w http.ResponseWriter, r *http.Request) {
				var res = map[string]any{
					"rowset_stage_location": map[string]any{"location_type": "FAKE", "expiry_time_ms": time.Now().Add(time.Hour).UnixMilli()},
				}
				if format != "" {
					res["parameter_overrides"] = map[string]string{"binary_input_format": format}
				}
				require.NoError(t, json.NewEncoder(w).Encode(res))
			})
			c.newUploader = func(context.Context, streamV2StageLocation) (streamV2Uploader, error) {
				return &fakeStage{objects: make(map[string][]byte)}, nil
			}

			var _, err = c.pipeInfo(context.Background(), streamV2PipePath("DB", "SCH", "T"))
			if format == "HEX" {
				require.ErrorContains(t, err, `binary input format "HEX"`)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
