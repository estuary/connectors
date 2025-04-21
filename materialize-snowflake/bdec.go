package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"math/big"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	sql "github.com/estuary/connectors/materialize-sql"
)

const (
	// BYTES_16_MB is the common Snowflake "maximum string length" of 16 MiB.
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/DataValidationUtil.java#L73
	BYTES_16_MB = 16 * 1024 * 1024

	// MAX_SEMI_STRUCTURED_LENGTH is the same as maxStrLen, but includes room
	// for a little bit of overhead, apparently.
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/DataValidationUtil.java#L77
	MAX_SEMI_STRUCTURED_LENGTH = BYTES_16_MB - 64

	// MAX_LOB_LENGTH is the maximum length allowed for the "longest string"
	// value of blob metadata. Values longer than this are truncated by
	// `truncateBytesAsHex`.
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/BinaryStringUtils.java#L7
	MAX_LOB_LENGTH = 32

	// Bit mask for ensuring timezone offsets fit in 14 bits. Included here for
	// parity with the Java SDK.
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/TimestampWrapper.java#L31-L41
	MASK_OF_TIMEZONE = (1 << 14) - 1

	// Always "bdec".
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/utils/Constants.java#L55
	BLOB_EXTENSION_TYPE = "bdec"

	// For blobs we only write a single chunk and a chunk can only have a single
	// row group, so this ends up being the size limit for the single row group
	// written by the bdec writer.
	// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/utils/ParameterProvider.java#L64C62-L64C79
	MAX_CHUNK_SIZE_IN_BYTES_DEFAULT = 256 * 1024 * 1024
)

type bdecWriter struct {
	pq        *enc.ParquetEncoder
	blobStats *blobStatsTracker
	cols      []tableColumn
	done      bool
}

// bdecWriter writes bdec files, which are basically Parquet files with values
// written in Snowflake's specific way. Various statistics about the file are
// computed as well, the particulars of which are from from BlobBuilder.java of
// the Java SDK. We've got our own streaming version of the encrypting and
// hashing to match what is needed from that.
func newBdecWriter(
	w io.WriteCloser,
	mappedColumns []*sql.Column,
	existingColumns []tableColumn,
	encryptionKey string,
	fileName blobFileName,
) (*bdecWriter, error) {
	orderedCols, err := orderExistingColumns(mappedColumns, existingColumns)
	if err != nil {
		return nil, fmt.Errorf("making stream columns: %w", err)
	}

	metadata := map[string]string{"primaryFileId": path.Base(string(fileName))}
	sch := make(enc.ParquetSchema, 0, len(orderedCols))
	for _, col := range orderedCols {
		e, err := makeSchemaElement(col)
		if err != nil {
			return nil, err
		}
		sch = append(sch, e)

		if slices.Contains([]string{"array", "object", "variant"}, col.LogicalType) {
			metadata[fmt.Sprintf("%d:obj_enc", col.Ordinal)] = "1"
		}
		metadata[strconv.Itoa(col.Ordinal)] = logicalTypeOrdinals[col.LogicalType] +
			"," +
			physicalTypeOrdinals[col.PhysicalType]
	}

	cipherStream, err := getCipherStream(encryptionKey, fileName)
	if err != nil {
		return nil, fmt.Errorf("getting cipher stream: %w", err)
	}

	blobStats := &blobStatsTracker{
		fileName: fileName,
		md5:      md5.New(),
		start:    time.Now(),
		w:        w,
		stream:   cipherStream,
	}

	for _, c := range orderedCols {
		blobStats.columns = append(blobStats.columns, &columnStatsTracker{
			name:    c.Name,
			ordinal: c.Ordinal,
			// Text columns are obviously string-encoded, but variant columns
			// are too, and must be included in the metadata statistics.
			isStr: c.LogicalType == "text" || c.LogicalType == "variant",
			// Similarly, more column types than you might expect are stored as
			// 128-bit integers. This applies to even timestamp columns which
			// use a decimal logical annotation.
			isInt: slices.Contains([]string{"fixed", "date", "timestamp_ltz", "timestamp_ntz", "timestamp_tz"}, c.LogicalType),
			isNum: c.LogicalType == "real",
		})
	}

	pq := enc.NewParquetEncoder(
		blobStats,
		sch,
		enc.WithParquetCompression(enc.Snappy),
		enc.WithDisableDictionaryEncoding(), // not only for performance, but also to support some column types (VARIANT)
		enc.WithParquetMetadata(metadata),
		// We are effectively disabling the new row group creation logic in the
		// Parquet writer and handling when the file and its single row group
		// should be closed here.
		enc.WithParquetRowGroupRowLimit(math.MaxInt64),
		enc.WithParquetRowGroupByteLimit(math.MaxInt64),
	)

	return &bdecWriter{
		pq:        pq,
		blobStats: blobStats,
		cols:      orderedCols,
	}, nil
}

func (bw *bdecWriter) encodeRow(row []any) error {
	for i, col := range bw.cols {
		stats := bw.blobStats.columns[i]

		// Write null values for table columns that aren't in our field
		// selection, in addition to null values for fields in the field
		// selection with null values.
		if i >= len(row) || row[i] == nil {
			if !col.Nullable {
				return fmt.Errorf("attempted to write null value to non-nullable column %q", col.Name)
			}
			stats.nullCount++
			continue
		}

		switch col.LogicalType {
		case "fixed":
			v, err := getInt(row[i])
			if err != nil {
				return fmt.Errorf("getInt for column %q: %w", col.Name, err)
			}
			row[i] = v
			stats.nextInt(v)
		case "text":
			maxLength := BYTES_16_MB
			if col.ByteLength != nil {
				maxLength = *col.ByteLength
			}
			v, err := getStr(row[i], maxLength)
			if err != nil {
				return fmt.Errorf("getStr for column %q: %w", col.Name, err)
			}
			row[i] = v
			stats.nextStr(v)
		case "variant":
			v, err := getStr(row[i], MAX_SEMI_STRUCTURED_LENGTH)
			if err != nil {
				return fmt.Errorf("getStr for column %q: %w", col.Name, err)
			}
			row[i] = v
			stats.nextStr(v)
		case "real":
			v, err := getNum(row[i])
			if err != nil {
				return fmt.Errorf("getNum for column %q: %w", col.Name, err)
			}
			row[i] = v
			stats.nextNum(v)
		case "date":
			// We are using the default parquet writer handling for dates here
			// to write the value as an int32 primitive type with the "date"
			// logical type as unix days. But we need to include it in the
			// integer stats, and must parse it into an int128 here for that.
			// The original value (a date string) is left as-is in the row, so
			// we'll end up parsing it twice: Once here for stats, and once in
			// the parquet writer.
			v, err := getDateInt(row[i])
			if err != nil {
				return fmt.Errorf("getDateInt for column %q: %w", col.Name, err)
			}
			stats.nextInt(v)
		case "timestamp_ltz", "timestamp_ntz", "timestamp_tz":
			v, err := getTimestamp(row[i], col.LogicalType, *col.Scale)
			if err != nil {
				return fmt.Errorf("getTimestamp for column %q: %w", col.Name, err)
			}
			row[i] = v
			stats.nextInt(v)
		case "boolean":
			// Boolean values use the default parquet writer as-is.
		default:
			return fmt.Errorf("unhandled Snowflake logical type %q for column %q", col.LogicalType, col.Name)
		}
	}

	// If needed, extend the row of selected field values with null values for
	// all the other pre-existing columns which aren't selected.
	if len(row) < len(bw.cols) {
		row = append(row, make([]any, len(bw.cols)-len(row))...)
	}

	if err := bw.pq.Encode(row); err != nil {
		return fmt.Errorf("encoding row as parquet: %w", err)
	}
	if sz := bw.pq.ScratchSize(); sz >= MAX_CHUNK_SIZE_IN_BYTES_DEFAULT {
		// Since only a single row group can be in a chunk and we only write one
		// chunk per blob, this blob must be closed out now.
		bw.blobStats.lengthUncompressed = sz
		bw.done = true
	}
	bw.blobStats.rows++

	return nil
}

func (bw *bdecWriter) close() error {
	// TODO(whb): When calling Close() on the parquet writer, it will start
	// furiously writing bytes of the output to its writer, which is a network
	// pipe to object storage. This kind of spiky network traffic is probably
	// not optimal for throughput, and it may be worth implementing a kind of
	// async closing strategy here and/or in the parquet writer itself so that
	// the completion of Close() can be awaited in the background while the next
	// file is in progress.
	if err := bw.pq.Close(); err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}

	return nil
}

type blobStatsTracker struct {
	// The Java SDK has separate concepts for flush start / build duration /
	// upload duration, but we do them all simultaneously so there's really only
	// a start and a finish.
	start  time.Time
	finish time.Time

	fileName           blobFileName // from `getNextFileName`, does not include the common path prefix
	md5                hash.Hash    // md5 checksum of the entire blob
	rows               int          // total number of rows
	length             int          // total number of bytes in the blob
	lengthUncompressed int          // estimate of the uncompressed byte size of the blob's data

	columns []*columnStatsTracker

	// These are used for writing & encrypting bytes through the stats tracker.
	w      io.WriteCloser
	stream cipher.Stream
	buf    []byte
}

func (bs *blobStatsTracker) Write(p []byte) (int, error) {
	n := len(p)
	if n > cap(bs.buf) {
		bs.buf = slices.Grow(bs.buf, n-len(bs.buf))
	}

	bs.stream.XORKeyStream(bs.buf[:n], p)
	if written, err := bs.w.Write(bs.buf[:n]); err != nil {
		return written, fmt.Errorf("blobStatsTracker error writing to output: %w", err)
	} else if written != n {
		return written, fmt.Errorf("written bytes %d != expected bytes %d", written, n)
	} else if hashed, err := bs.md5.Write(bs.buf[:n]); err != nil {
		return written, fmt.Errorf("failed to write md5 for blob: %w", err)
	} else if hashed != n {
		return written, fmt.Errorf("hashed bytes %d != expected bytes %d", hashed, n)
	}

	bs.length += n
	return n, nil
}

func (bs *blobStatsTracker) Close() error {
	if err := bs.w.Close(); err != nil {
		return fmt.Errorf("blobStatsTracker error closing output: %w", err)
	}
	bs.finish = time.Now()

	return nil
}

type columnStatsTracker struct {
	name    string
	ordinal int

	nullCount int
	hasData   bool

	// The isStr, isInt, and isNum flags are sanity checks and shouldn't
	// strictly be necessary.
	isStr    bool
	strStats struct {
		maxVal string
		minVal string
		maxLen int
	}

	isInt    bool
	intStats struct {
		maxVal decimal128.Num
		minVal decimal128.Num
	}

	isNum    bool
	numStats struct {
		maxVal float64
		minVal float64
	}
}

func (cs *columnStatsTracker) nextStr(v string) {
	if !cs.isStr {
		panic(fmt.Sprintf("internal error: nextStr called on non-string column %q", cs.name))
	}

	if !cs.hasData {
		cs.strStats.maxLen = len(v)
		cs.strStats.maxVal = v
		cs.strStats.minVal = v
		cs.hasData = true
		return
	}

	cs.strStats.maxLen = max(len(v), cs.strStats.maxLen)

	if len(v) > MAX_LOB_LENGTH {
		// These ultimately get truncated per truncateBytesAsHex, so there is no
		// reason to buffer more than this length.
		v = v[:MAX_LOB_LENGTH]
	}
	cs.strStats.maxVal = max(cs.strStats.maxVal, v)
	cs.strStats.minVal = min(cs.strStats.minVal, v)
}

func (cs *columnStatsTracker) nextInt(v decimal128.Num) {
	if !cs.isInt {
		panic(fmt.Sprintf("internal error: nextInt called on non-int column %q", cs.name))
	}

	if !cs.hasData {
		cs.intStats.maxVal = v
		cs.intStats.minVal = v
		cs.hasData = true
		return
	}

	if v.Greater(cs.intStats.maxVal) {
		cs.intStats.maxVal = v
	}
	if v.Less(cs.intStats.minVal) {
		cs.intStats.minVal = v
	}
}

func (cs *columnStatsTracker) nextNum(v float64) {
	if !cs.isNum {
		panic(fmt.Sprintf("internal error: nextNum called on non-num column %q", cs.name))
	}

	if !cs.hasData {
		cs.numStats.maxVal = v
		cs.numStats.minVal = v
		cs.hasData = true
		return
	}

	if v > cs.numStats.maxVal {
		cs.numStats.maxVal = v
	}
	if v < cs.numStats.minVal {
		cs.numStats.minVal = v
	}
}

func getCipherStream(encryptionKey string, fileName blobFileName) (cipher.Stream, error) {
	derivedKey, err := deriveKey(encryptionKey, fileName)
	if err != nil {
		return nil, fmt.Errorf("deriving key: %w", err)
	}

	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("creating cipher: %w", err)
	}

	// The initialization vector (iv) is always 0, since there is a single chunk
	// per blob.
	return cipher.NewCTR(block, make([]byte, aes.BlockSize)), nil
}

// reencrypt reads encrypted blob data from r, decrypts it using the encryption
// key and original file name, and then re-encrypts it using the new file name
// and writes the output to w. The blob metadata is updated in place.
func reencrypt(
	r io.Reader,
	w io.Writer,
	blob *blobMetadata,
	encryptionKey string,
	newFileName blobFileName,
) error {
	decryptStream, err := getCipherStream(encryptionKey, blobFileName(blob.Path))
	if err != nil {
		return fmt.Errorf("getting decryptStream: %w", err)
	}

	encryptStream, err := getCipherStream(encryptionKey, newFileName)
	if err != nil {
		return fmt.Errorf("getting encryptStream: %w", err)
	}

	downMd5 := md5.New()
	upMd5 := md5.New()

	buf := make([]byte, 64*1024)
	for {
		n, err := r.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("reading from r: %w", err)
		} else if n == 0 {
			break
		}

		if m, err := downMd5.Write(buf[:n]); err != nil {
			return fmt.Errorf("writing to downMd5: %w", err)
		} else if m != n {
			return fmt.Errorf("written to downMd5 %d != expected bytes %d", m, n)
		}

		decryptStream.XORKeyStream(buf[:n], buf[:n])
		encryptStream.XORKeyStream(buf[:n], buf[:n])

		if m, err := upMd5.Write(buf[:n]); err != nil {
			return fmt.Errorf("writing to upMd5: %w", err)
		} else if m != n {
			return fmt.Errorf("written to upMd5 %d != expected bytes %d", m, n)
		} else if written, err := w.Write(buf[:n]); err != nil {
			return fmt.Errorf("writing to w: %w", err)
		} else if written != n {
			return fmt.Errorf("written bytes %d != expected bytes %d", written, n)
		}
	}

	downHash := hex.EncodeToString(downMd5.Sum(nil))
	upHash := hex.EncodeToString(upMd5.Sum(nil))
	if downHash != blob.MD5 || downHash != blob.Chunks[0].ChunkMD5 {
		return fmt.Errorf("md5 mismatch: %s != %s", downHash, blob.MD5)
	}

	blob.Path = string(newFileName)
	blob.MD5 = upHash
	blob.Chunks[0].ChunkMD5 = upHash

	return nil
}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/utils/Cryptor.java#L61-L74
func deriveKey(secretKey string, diversifier blobFileName) ([]byte, error) {
	decodedString, err := base64.StdEncoding.DecodeString(secretKey)
	if err != nil {
		return nil, err
	}
	concat := append(decodedString, []byte(diversifier)...)
	derivedBytes := sha256.Sum256(concat)
	return derivedBytes[:], nil
}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/BinaryStringUtils.java
func truncateBytesAsHex(bytes []byte, truncateUp bool) string {
	if len(bytes) <= MAX_LOB_LENGTH {
		return hex.EncodeToString(bytes)
	}

	result := make([]byte, MAX_LOB_LENGTH)
	copy(result, bytes[:MAX_LOB_LENGTH])
	if truncateUp {
		var idx int
		for idx = MAX_LOB_LENGTH - 1; idx >= 0; idx-- {
			result[idx]++
			if result[idx] != 0 {
				break
			}
		}
		if idx < 0 {
			return "Z"
		}
	}

	return hex.EncodeToString(result)
}

type unhandledColError struct {
	msg string
}

func (u unhandledColError) Error() string {
	return u.msg
}

func newUnhandledColError(format string, a ...any) error {
	return &unhandledColError{
		msg: fmt.Sprintf(format, a...),
	}
}

// Loosely adapted from
// https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/ParquetTypeGenerator.java#L77-L148
func makeSchemaElement(col tableColumn) (enc.ParquetSchemaElement, error) {
	fieldId := int32(col.Ordinal)
	e := enc.ParquetSchemaElement{
		Name:     col.Name,
		Required: !col.Nullable,
		FieldId:  &fieldId,
	}

	nilOrScale := func(s *int) string {
		if s == nil {
			return "<nil>"
		}
		return strconv.Itoa(*s)
	}

	switch col.LogicalType {
	case "fixed":
		if col.PhysicalType == "SB16" && col.Scale == nil || *col.Scale == 0 {
			// A decimal with 0 precision, like a DECIMAL(38,0) that we use for
			// integer columns.
			e.DataType = enc.LogicalTypeDecimal
			e.Scale = 0
		} else {
			return e, newUnhandledColError("fixed column with physical type %q and scale %s not supported", col.PhysicalType, nilOrScale(col.Scale))
		}
	case "text", "variant":
		e.DataType = enc.LogicalTypeString
	case "timestamp_ltz", "timestamp_ntz", "timestamp_tz":
		if col.PhysicalType == "SB16" && col.Scale != nil && *col.Scale == 9 {
			// A timestamp with nanosecond precision.
			e.DataType = enc.LogicalTypeDecimal
			e.Scale = 9
		} else {
			return e, newUnhandledColError("%s column with physical type %q and scale %s not supported", col.LogicalType, col.PhysicalType, nilOrScale(col.Scale))
		}
	case "date":
		e.DataType = enc.LogicalTypeDate
	case "boolean":
		e.DataType = enc.PrimitiveTypeBoolean
	case "real":
		e.DataType = enc.PrimitiveTypeNumber
	default:
		return e, newUnhandledColError("unhandled type %q", col.Type)
	}

	return e, nil
}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/AbstractRowBuffer.java#L46-L71
var logicalTypeOrdinals = map[string]string{
	"boolean":       "1",
	"null":          "15",
	"real":          "8",
	"fixed":         "2",
	"text":          "9",
	"binary":        "10",
	"date":          "7",
	"time":          "6",
	"timestamp_ltz": "3",
	"timestamp_ntz": "4",
	"timestamp_tz":  "5",
	"array":         "13",
	"object":        "12",
	"variant":       "11",
}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/AbstractRowBuffer.java#L107-L119
var physicalTypeOrdinals = map[string]string{
	"DOUBLE": "7",
	"SB1":    "1",
	"SB2":    "2",
	"SB4":    "3",
	"SB8":    "4",
	"SB16":   "5",
	"LOB":    "8",
	"ROW":    "10",
}

// orderExistingColumns orders the existing columns by first aligning them with
// the mapped columns from the field selection, followed by the rest of them in
// the order they came back from the channel open request. Ordering them in this
// way allows for rows of data to easily be correlated with the target column
// for selected fields, and adding null values at the end for the rest.
func orderExistingColumns(mappedColumns []*sql.Column, existingColumns []tableColumn) ([]tableColumn, error) {
	findExisting := func(col *sql.Column) (tableColumn, error) {
		var out tableColumn
		idx := slices.IndexFunc(existingColumns, func(c tableColumn) bool {
			// If the column was created with quotes, it will be reported as
			// quoted. Otherwise column matching is case-insensitive.
			if strings.HasPrefix(c.Name, `"`) && strings.HasSuffix(c.Name, `"`) {
				return c.Name == col.Identifier
			} else if strings.HasPrefix(c.Name, `"`) || strings.HasSuffix(c.Name, `"`) {
				panic(fmt.Sprintf("internal error: malformed column name %q", c.Name))
			} else {
				return strings.EqualFold(c.Name, col.Identifier)
			}
		})
		if idx == -1 {
			return out, fmt.Errorf("existing column for %q not found", col.Identifier)
		}

		return existingColumns[idx], nil
	}

	out := make([]tableColumn, 0, len(existingColumns))
	for _, mapped := range mappedColumns {
		existing, err := findExisting(mapped)
		if err != nil {
			return nil, err
		}
		out = append(out, existing)
	}

	for _, existing := range existingColumns {
		if !slices.Contains(out, existing) {
			out = append(out, existing)
		}
	}

	return out, nil
}

var (
	maxSnowflakeInteger = func() decimal128.Num {
		n, _ := decimal128.FromString(strings.Repeat("9", 38), 38, 0)
		return n
	}()

	minSnowflakeInteger = func() decimal128.Num {
		n, _ := decimal128.FromString("-"+strings.Repeat("9", 38), 38, 0)
		return n
	}()
)

func getInt(val any) (got decimal128.Num, err error) {
	switch v := val.(type) {
	case int:
		got = decimal128.FromI64(int64(v))
	case int64:
		got = decimal128.FromI64(v)
	case uint64:
		got = decimal128.FromU64(v)
	case float64:
		if got, err = decimal128.FromFloat64(v, 38, 0); err != nil {
			return
		} else if got.Greater(maxSnowflakeInteger) || got.Less(minSnowflakeInteger) {
			err = fmt.Errorf("float64-encoded integer value %q outside of Snowflake integer range", strconv.FormatFloat(v, 'f', -1, 64))
		}
	case *big.Int:
		if v.BitLen() > 127 {
			err = fmt.Errorf("big.Int value %q has more than 127 bits", v.String())
			return
		}
		got = decimal128.FromBigInt(v)
		if got.Greater(maxSnowflakeInteger) || got.Less(minSnowflakeInteger) {
			err = fmt.Errorf("big.Int integer value %q outside of Snowflake integer range", v.String())
		}
	default:
		err = fmt.Errorf("getInt unhandled type: %T", v)
	}

	return
}

func getStr(val any, maxByteLength int) (got string, err error) {
	switch v := val.(type) {
	case string:
		got = v
	case []byte:
		got = string(v)
	case json.RawMessage:
		got = string(v)
	case bool:
		got = strconv.FormatBool(v)
	case int:
		got = strconv.Itoa(v)
	case int64:
		got = strconv.Itoa(int(v))
	case uint64:
		got = strconv.FormatUint(v, 10)
	case float64:
		got = strconv.FormatFloat(v, 'f', -1, 64)
	default:
		err = fmt.Errorf("getStr unhandled type: %T", v)
	}

	if len(got) > maxByteLength {
		err = fmt.Errorf("string with length %d is too long: max allowed for this column is %d", len(got), maxByteLength)
	}

	return
}

func getNum(val any) (got float64, err error) {
	switch v := val.(type) {
	case float64:
		got = v
	case float32:
		got = float64(v)
	case int64:
		got = float64(v)
	case string:
		// The sqlgen element converter converts these to strings for JSON
		// serialization. It's kind of silly to have to convert them back again
		// here, but the performance impact should be negligible.
		switch v {
		case "NaN":
			got = math.NaN()
		case "inf":
			got = math.Inf(1)
		case "-inf":
			got = math.Inf(-1)
		default:
			err = fmt.Errorf("invalid string value %q for float64", v)
		}
	default:
		err = fmt.Errorf("getNum unhandled type: %T", v)
	}

	return
}

// This is a copy from the parquet writer's "getDate" function, except it
// returns a decimal128.
func getDateInt(val any) (got decimal128.Num, err error) {
	switch v := val.(type) {
	case string:
		if d, parseErr := time.Parse(time.DateOnly, v); parseErr != nil {
			err = fmt.Errorf("unable to parse string %q as time.DateOnly: %w", v, parseErr)
		} else {
			unixSeconds := d.Unix()
			unixDays := unixSeconds / 60 / 60 / 24
			got = decimal128.FromI64(int64(unixDays))
		}
	default:
		err = fmt.Errorf("getDateInt unhandled type: %T", v)
	}

	return

}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/TimestampWrapper.java
func getTimestamp(val any, logicalType string, scale int) (got decimal128.Num, err error) {
	ts, ok := val.(string)
	if !ok {
		return got, fmt.Errorf("getTimestamp unhandled type: %T", val)
	}

	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return got, fmt.Errorf("getTimestamp failed to parse input %q: %w", ts, err)
	}

	if logicalType == "timestamp_ltz" {
		// These are always stored internally as UTC, and do not include the
		// original timezone offset.
		parsed = parsed.UTC()
	} else if logicalType == "timestamp_ntz" {
		// These ignore the zone offset completely and interpret the timestamp
		// as a wall-clock time.
		parsed = time.Date(
			parsed.Year(), parsed.Month(), parsed.Day(),
			parsed.Hour(), parsed.Minute(), parsed.Second(), parsed.Nanosecond(),
			time.UTC,
		)
	}

	fraction := (int64(parsed.Nanosecond()) / int64(math.Pow10(9-scale))) * int64(math.Pow10(9-scale))
	got = decimal128.FromI64(parsed.Unix()).
		Mul(decimal128.FromI64(int64(math.Pow10(9)))). // seconds to nanoseconds
		Add(decimal128.FromI64(int64(fraction)))       // add in the truncated nanoseconds fraction

	if scale != 9 {
		// Downscale nanoseconds to the desired scale. We only need to do this
		// if the scale is not 9 (for nanoseconds), and it should always be 9
		// until we implement support for other scales. This is a (relatively)
		// slow(er) operation because Div does allocations from creating new
		// *big.Int's, although it probably won't be noticeable in practice. We
		// may consider optimizing this in the future if it becomes an issue -
		// allocation-free algorithms for this division do exist.
		got, _ = got.Div(decimal128.FromI64(int64(math.Pow10(9 - scale))))
	}

	if logicalType == "timestamp_tz" {
		// Must embed the timezone offset in the value.
		_, timezoneOffsetSeconds := parsed.Zone()
		offsetMin := timezoneOffsetSeconds / 60
		if offsetMin < -1440 || offsetMin > 1440 {
			// I don't know if this is possible to happen, but it is a check the
			// Java SDK does so we'll do it too.
			return got, fmt.Errorf("unexpected timezone offset %d", offsetMin)
		}
		offsetMin += 1440 // normalizes the range of possible offsets from [-1440, 1440] to [0, 2880]

		// Shift left by 14 bits to make room for the offset, and then add the
		// offset.
		shift := 14
		hi := got.HighBits()
		lo := got.LowBits()
		shifted := decimal128.New(
			hi<<shift|int64(lo>>(64-shift)),
			lo<<shift,
		)
		// The bitmask here shouldn't really be needed since we have already
		// verified the offset is within [0, 2880], but oh well.
		got = shifted.Add(decimal128.FromI64(int64(offsetMin & MASK_OF_TIMEZONE)))
	}

	return
}
