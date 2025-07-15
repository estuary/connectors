package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	stdsql "database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"path"
	"strings"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql-v2"
	"github.com/stretchr/testify/require"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestBdecWriter(t *testing.T) {
	mappedColumns := []*sql.Column{
		{Identifier: "col1"},
		{Identifier: "col2"},
	}
	bl := 1 << 18
	existingColumns := []tableColumn{
		{Name: "col1", Type: "text", LogicalType: "text", PhysicalType: "LOB", ByteLength: &bl, Ordinal: 1},
		{Name: "col2", Type: "text", LogicalType: "text", PhysicalType: "LOB", ByteLength: &bl, Ordinal: 2},
	}

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-file.parquet")
	require.NoError(t, err)
	defer tmpFile.Close()

	encryptionKey := "aGVsbG8K"
	fileName := blobFileName(tmpFile.Name())
	key, err := deriveKey(encryptionKey, fileName)
	require.NoError(t, err)

	w, err := newBdecWriter(tmpFile, mappedColumns, existingColumns, encryptionKey, fileName, false)
	require.NoError(t, err)

	require.NoError(t, w.encodeRow([]any{"hello1", "world1"}))
	require.NoError(t, w.encodeRow([]any{"hello2", "world2"}))
	require.NoError(t, w.encodeRow([]any{"hello3", "world3"}))
	require.NoError(t, w.close())

	got, err := os.ReadFile(tmpFile.Name())
	require.NoError(t, err)

	outputFile := path.Join(t.TempDir(), "out.parquet")
	dec := decrypt(t, got, key, 0)
	require.NoError(t, os.WriteFile(outputFile, dec, 0644))

	db, err := stdsql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM '%s' ORDER BY col1", outputFile))
	require.NoError(t, err)
	defer rows.Close()

	gotRows := [][]string{}
	for rows.Next() {
		var col1, col2 string
		require.NoError(t, rows.Scan(&col1, &col2))
		gotRows = append(gotRows, []string{col1, col2})
	}
	require.NoError(t, rows.Err())

	require.Equal(t, [][]string{
		{"hello1", "world1"},
		{"hello2", "world2"},
		{"hello3", "world3"}},
		gotRows,
	)
}

func BenchmarkBlobStatsWrite(b *testing.B) {
	encryptionKey := "aGVsbG8K"
	diversifier := blobFileName("test-file.parquet")
	derivedKey, err := deriveKey(encryptionKey, diversifier)
	require.NoError(b, err)

	block, err := aes.NewCipher(derivedKey)
	require.NoError(b, err)

	w := &blobStatsTracker{
		md5:    md5.New(),
		w:      discardWriteCloser{},
		stream: cipher.NewCTR(block, make([]byte, aes.BlockSize)),
	}

	chunkSize := 4 * 1024
	data := make([]byte, chunkSize)
	if _, err := rand.Read(data); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		if _, err := w.Write(data); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(chunkSize))
	}
}

func TestReencrypt(t *testing.T) {
	input := make([]byte, 1024*1024+3)
	_, err := rand.Read(input)
	require.NoError(t, err)
	encryptionKey := "aGVsbG8K"
	oldFileName := blobFileName("old")
	newFileName := blobFileName("new")

	// Simulate the originally encrypted data.
	encryptStream, err := getCipherStream(encryptionKey, oldFileName)
	if err != nil {
		t.Fatalf("getCipherStream (encrypt): %v", err)
	}

	encrypted := make([]byte, len(input))
	encryptStream.XORKeyStream(encrypted, input)

	sum := md5.Sum(encrypted)
	hsh := hex.EncodeToString(sum[:])

	blob := &blobMetadata{
		Path:   string(oldFileName),
		MD5:    hsh,
		Chunks: []uploadChunkMetadata{{ChunkMD5: hsh}},
	}

	var in = bytes.NewReader(encrypted)
	var out bytes.Buffer

	require.NoError(t, reencrypt(in, &out, blob, encryptionKey, newFileName))

	// The re-encrypted file matches the original input.
	newKey, err := deriveKey(encryptionKey, newFileName)
	require.NoError(t, err)
	got := decrypt(t, out.Bytes(), newKey, 0)
	require.Equal(t, input, got)

	// The blob metadata has been updated.
	sum = md5.Sum(out.Bytes())
	hsh = hex.EncodeToString(sum[:])
	require.Equal(t, string(newFileName), blob.Path)
	require.Equal(t, hsh, blob.MD5)
	require.Equal(t, hsh, blob.Chunks[0].ChunkMD5)
}

func TestTruncateBytesAsHex(t *testing.T) {
	for _, tt := range []struct {
		input      string
		want       string
		truncateUp bool
	}{
		{input: "", want: "", truncateUp: true},
		{input: "", want: "", truncateUp: false},
		{input: "aaaa", want: "61616161", truncateUp: true},
		{input: "aaaa", want: "61616161", truncateUp: false},
		{input: strings.Repeat("a", 31), want: strings.Repeat("61", 31), truncateUp: true},
		{input: strings.Repeat("a", 31), want: strings.Repeat("61", 31), truncateUp: false},
		{input: strings.Repeat("a", 32), want: strings.Repeat("61", 32), truncateUp: true},
		{input: strings.Repeat("a", 32), want: strings.Repeat("61", 32), truncateUp: false},
		{input: strings.Repeat("a", 33), want: strings.Repeat("61", 31) + "62", truncateUp: true},
		{input: strings.Repeat("a", 33), want: strings.Repeat("61", 32), truncateUp: false},
		{input: strings.Repeat(string([]byte{0xff}), 33), want: "Z", truncateUp: true},
		{input: strings.Repeat(string([]byte{0xff}), 33), want: strings.Repeat("ff", 32), truncateUp: false},
	} {
		require.Equal(t, tt.want, truncateBytesAsHex([]byte(tt.input), tt.truncateUp))
	}
}

func TestGetInt(t *testing.T) {
	tooBig, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	tooBig = tooBig.Add(tooBig, big.NewInt(1))
	tooSmall, _ := new(big.Int).SetString("99999999999999999999999999999999999999", 10)
	tooSmall = tooSmall.Sub(tooSmall, big.NewInt(-1))

	var err error
	_, err = getInt(tooBig)
	require.Error(t, err)
	_, err = getInt(tooSmall)
	require.Error(t, err)
}

func TestGetTimestamp(t *testing.T) {
	for _, tt := range []struct {
		input       string
		want        string
		logicalType string
	}{
		{input: "2022-08-17T04:32:19.987654321Z", want: "1660710739987654321", logicalType: "timestamp_ltz"},
		{input: "2022-08-17T04:32:19.987654321+02:00", want: "1660703539987654321", logicalType: "timestamp_ltz"},
		{input: "2022-08-17T04:32:19.987654321+03:30", want: "1660698139987654321", logicalType: "timestamp_ltz"},
		{input: "2022-08-17T04:32:19.987654321-07:00", want: "1660735939987654321", logicalType: "timestamp_ltz"},
		{input: "0000-01-01T00:00:00.000000000Z", want: "-62167219200000000000", logicalType: "timestamp_ltz"},
		{input: "0000-01-01T00:00:00.000000000+23:59", want: "-62167305540000000000", logicalType: "timestamp_ltz"},
		{input: "0000-01-01T00:00:00.000000000-23:59", want: "-62167132860000000000", logicalType: "timestamp_ltz"},
		{input: "9999-12-31T23:59:59.999999999Z", want: "253402300799999999999", logicalType: "timestamp_ltz"},
		{input: "9999-12-31T23:59:59.999999999+23:59", want: "253402214459999999999", logicalType: "timestamp_ltz"},
		{input: "9999-12-31T23:59:59.999999999-23:59", want: "253402387139999999999", logicalType: "timestamp_ltz"},

		{input: "2022-08-17T04:32:19.987654321Z", want: "1660710739987654321", logicalType: "timestamp_ntz"},
		{input: "2022-08-17T04:32:19.987654321+02:00", want: "1660710739987654321", logicalType: "timestamp_ntz"},
		{input: "2022-08-17T04:32:19.987654321+03:30", want: "1660710739987654321", logicalType: "timestamp_ntz"},
		{input: "2022-08-17T04:32:19.987654321-07:00", want: "1660710739987654321", logicalType: "timestamp_ntz"},
		{input: "0000-01-01T00:00:00.000000000Z", want: "-62167219200000000000", logicalType: "timestamp_ntz"},
		{input: "0000-01-01T00:00:00.000000000+23:59", want: "-62167219200000000000", logicalType: "timestamp_ntz"},
		{input: "0000-01-01T00:00:00.000000000-23:59", want: "-62167219200000000000", logicalType: "timestamp_ntz"},
		{input: "9999-12-31T23:59:59.999999999Z", want: "253402300799999999999", logicalType: "timestamp_ntz"},
		{input: "9999-12-31T23:59:59.999999999+23:59", want: "253402300799999999999", logicalType: "timestamp_ntz"},
		{input: "9999-12-31T23:59:59.999999999-23:59", want: "253402300799999999999", logicalType: "timestamp_ntz"},

		{input: "2022-08-17T04:32:19.987654321Z", want: "27209084763957728396704", logicalType: "timestamp_tz"},
		{input: "2022-08-17T04:32:19.987654321+02:00", want: "27208966799157728396824", logicalType: "timestamp_tz"},
		{input: "2022-08-17T04:32:19.987654321+03:30", want: "27208878325557728396914", logicalType: "timestamp_tz"},
		{input: "2022-08-17T04:32:19.987654321-07:00", want: "27209497640757728396284", logicalType: "timestamp_tz"},
		{input: "0000-01-01T00:00:00.000000000Z", want: "-1018547719372799999998560", logicalType: "timestamp_tz"},
		{input: "0000-01-01T00:00:00.000000000+23:59", want: "-1018549133967359999997121", logicalType: "timestamp_tz"},
		{input: "0000-01-01T00:00:00.000000000-23:59", want: "-1018546304778239999999999", logicalType: "timestamp_tz"},
		{input: "9999-12-31T23:59:59.999999999Z", want: "4151743296307199999985056", logicalType: "timestamp_tz"},
		{input: "9999-12-31T23:59:59.999999999+23:59", want: "4151741881712639999986495", logicalType: "timestamp_tz"},
		{input: "9999-12-31T23:59:59.999999999-23:59", want: "4151744710901759999983617", logicalType: "timestamp_tz"},
	} {
		t.Run(tt.input+"_"+tt.logicalType, func(t *testing.T) {
			got, err := getTimestamp(tt.input, tt.logicalType, 9)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.BigInt().String())
		})
	}
}

func BenchmarkGetTimestamp(b *testing.B) {
	input := "2025-04-15T19:23:47.583294617Z"
	b.Run("fast path", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			if _, err := getTimestamp(input, "timestamp_tz", 9); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("slow path", func(b *testing.B) {
		b.ResetTimer()
		for b.Loop() {
			if _, err := getTimestamp(input, "timestamp_tz", 8); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type discardWriteCloser struct{}

func (discardWriteCloser) Write(in []byte) (int, error) { return len(in), nil }
func (discardWriteCloser) Close() error                 { return nil }

func decrypt(t *testing.T, enc []byte, key []byte, iv uint64) []byte {
	t.Helper()

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	ivBytes := make([]byte, aes.BlockSize)
	binary.BigEndian.PutUint64(ivBytes[8:], iv)
	decStream := cipher.NewCTR(block, ivBytes)

	dec := make([]byte, len(enc))
	decStream.XORKeyStream(dec, enc)

	return dec
}
