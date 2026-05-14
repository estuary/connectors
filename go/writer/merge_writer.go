package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// parquetMagic is the 4-byte file marker that opens and closes every parquet file.
var parquetMagic = []byte{'P', 'A', 'R', '1'}

// mergeWriter writes a parquet file by importing already-encoded data pages into row groups. It
// is built on top of arrow-go's metadata builders and PageWriter, but skips arrow-go's
// *file.Writer entirely. *file.Writer assumes data is fed via WriteBatch — it owns dictionary
// emission, page buffering, and chunk-stats aggregation, none of which apply when we are pumping
// pre-encoded pages from one parquet file into another. mergeWriter exposes a smaller surface
// shaped to that import use case: callers feed pre-encoded data pages directly and tell the row
// group its row count explicitly.
//
// The sink must satisfy arrow-go's parquet/internal/utils.WriterTell (io.Writer + Tell() int64);
// countingWriteCloser does after we add Tell. Go's structural typing lets us pass a value of an
// external concrete type into arrow-go's internal-interface-typed parameter, since interface
// satisfaction depends only on method sets.
type mergeWriter struct {
	sink     mergeWriterSink
	schema   *schema.Schema
	props    *parquet.WriterProperties
	metadata *metadata.FileMetaDataBuilder

	rowGroupCount int
	open          bool
	rowGroup      *mergeRowGroupWriter
}

// mergeWriterSink is the contract the sink must satisfy for arrow-go's PageWriter to use it.
// In practice this is countingWriteCloser, which adds Tell() returning the byte count.
type mergeWriterSink interface {
	io.Writer
	Tell() int64
}

func newMergeWriter(sink mergeWriterSink, sc *schema.GroupNode, props *parquet.WriterProperties, kv metadata.KeyValueMetadata) (*mergeWriter, error) {
	if props == nil {
		props = parquet.NewWriterProperties()
	}
	fileSchema := schema.NewSchema(sc)
	if _, err := sink.Write(parquetMagic); err != nil {
		return nil, fmt.Errorf("writing parquet magic header: %w", err)
	}
	return &mergeWriter{
		sink:     sink,
		schema:   fileSchema,
		props:    props,
		metadata: metadata.NewFileMetadataBuilder(fileSchema, props, kv),
		open:     true,
	}, nil
}

// AppendRowGroup begins a new row group. Any currently-open row group is closed first.
func (mw *mergeWriter) AppendRowGroup() (*mergeRowGroupWriter, error) {
	if !mw.open {
		return nil, fmt.Errorf("mergeWriter: AppendRowGroup on closed writer")
	}
	if mw.rowGroup != nil {
		if err := mw.rowGroup.Close(); err != nil {
			return nil, fmt.Errorf("closing prior row group: %w", err)
		}
	}
	rgMeta := mw.metadata.AppendRowGroup()
	rg := &mergeRowGroupWriter{
		parent:   mw,
		metadata: rgMeta,
		ordinal:  int16(mw.rowGroupCount),
	}
	mw.rowGroupCount++
	mw.rowGroup = rg
	return rg, nil
}

// Close finalizes any open row group, writes the parquet footer (file metadata, footer length,
// and trailing magic), and marks the writer closed. The underlying sink is not itself closed.
func (mw *mergeWriter) Close() error {
	if !mw.open {
		return nil
	}
	mw.open = false

	if mw.rowGroup != nil {
		if err := mw.rowGroup.Close(); err != nil {
			return fmt.Errorf("closing row group: %w", err)
		}
		mw.rowGroup = nil
	}

	fmd, err := mw.metadata.Snapshot()
	if err != nil {
		return fmt.Errorf("snapshotting file metadata: %w", err)
	}
	// FileMetaData.WriteTo accepts a nil encryptor for unencrypted files; the encryption.Encryptor
	// type is internal to arrow-go but Go infers nil at the call site.
	n, err := fmd.WriteTo(mw.sink, nil)
	if err != nil {
		return fmt.Errorf("writing parquet footer metadata: %w", err)
	}
	if err := binary.Write(mw.sink, binary.LittleEndian, uint32(n)); err != nil {
		return fmt.Errorf("writing footer length: %w", err)
	}
	if _, err := mw.sink.Write(parquetMagic); err != nil {
		return fmt.Errorf("writing parquet magic trailer: %w", err)
	}
	return nil
}

// FileMetadata returns a snapshot of the current file metadata. Useful for callers that want to
// inspect the file structure between row groups.
func (mw *mergeWriter) FileMetadata() (*metadata.FileMetaData, error) {
	return mw.metadata.Snapshot()
}

// mergeRowGroupWriter writes columns sequentially into one row group. Use NextColumn to advance
// through columns in schema order. Set the row count via SetNumRows before Close.
type mergeRowGroupWriter struct {
	parent     *mergeWriter
	metadata   *metadata.RowGroupMetaDataBuilder
	ordinal    int16
	nextCol    int
	currentCol *mergeColumnWriter
	nrows      int
	closed     bool
}

// SetNumRows records the row count for this row group; required before Close. The same value
// must also describe every column written into the row group.
func (rg *mergeRowGroupWriter) SetNumRows(n int) {
	rg.nrows = n
}

// NextColumn closes the prior column writer (if any) and returns a writer for the next column
// in schema order. Columns must be written in order and exactly once each.
func (rg *mergeRowGroupWriter) NextColumn() (*mergeColumnWriter, error) {
	if rg.closed {
		return nil, fmt.Errorf("mergeRowGroupWriter: NextColumn on closed row group")
	}
	if rg.currentCol != nil {
		if err := rg.currentCol.Close(); err != nil {
			return nil, fmt.Errorf("closing prior column: %w", err)
		}
	}

	cmb := rg.metadata.NextColumnChunk()
	colPath := cmb.Descr().Path()
	pw, err := file.NewPageWriter(
		rg.parent.sink,
		rg.parent.props.CompressionFor(colPath),
		rg.parent.props.CompressionLevelFor(colPath),
		cmb,
		rg.ordinal,
		int16(rg.nextCol),
		rg.parent.props.Allocator(),
		false, // not buffered: pages flow straight through to the sink
		nil,   // no metadata encryptor
		nil,   // no data encryptor
	)
	if err != nil {
		return nil, fmt.Errorf("creating page writer for col %d: %w", rg.nextCol, err)
	}

	cw := &mergeColumnWriter{pageWriter: pw}
	rg.currentCol = cw
	rg.nextCol++
	return cw, nil
}

// Close finalizes the row group: closes the in-flight column writer (if any), validates that
// every column has been written, and writes row group metadata.
func (rg *mergeRowGroupWriter) Close() error {
	if rg.closed {
		return nil
	}
	rg.closed = true

	if rg.currentCol != nil {
		if err := rg.currentCol.Close(); err != nil {
			return fmt.Errorf("closing column: %w", err)
		}
		rg.currentCol = nil
	}

	if rg.nextCol != rg.parent.schema.NumColumns() {
		return fmt.Errorf("only %d of %d columns written before row group close",
			rg.nextCol, rg.parent.schema.NumColumns())
	}

	rg.metadata.SetNumRows(rg.nrows)
	// The first parameter to Finish is unused inside arrow-go (it computes totals from the
	// column-chunk builders); pass 0.
	if err := rg.metadata.Finish(0, rg.ordinal); err != nil {
		return fmt.Errorf("finishing row group metadata: %w", err)
	}
	return nil
}

// mergeColumnWriter accepts pre-encoded data pages and accumulates them into one column chunk.
// Stats (compressed size, encoding stats, page offsets) are tracked internally by arrow-go's
// PageWriter; on Close we let it finalize the column chunk metadata.
type mergeColumnWriter struct {
	pageWriter file.PageWriter
	closed     bool
}

// WriteDataPage writes a pre-encoded data page. The page's encoding and statistics are preserved.
// Compression is applied if the PageWriter has a compressor: arrow-go's PageReader decompresses
// pages when reading from the scratch file, and PageWriter.WriteDataPage writes bytes verbatim
// without re-compressing, so we must compress here to keep the page bytes consistent with the
// column chunk metadata (which records the sink's configured compression codec).
func (cw *mergeColumnWriter) WriteDataPage(page file.DataPage) error {
	if cw.closed {
		return fmt.Errorf("mergeColumnWriter: WriteDataPage on closed column")
	}
	if cw.pageWriter.HasCompressor() {
		var buf bytes.Buffer
		compressed := cw.pageWriter.Compress(&buf, page.Data())
		compressedData := make([]byte, len(compressed))
		copy(compressedData, compressed)
		newBuf := memory.NewBufferBytes(compressedData)
		switch dp := page.(type) {
		case *file.DataPageV1:
			page = file.NewDataPageV1WithStats(newBuf, dp.NumValues(), parquet.Encoding(dp.Encoding()),
				dp.DefinitionLevelEncoding(), dp.RepetitionLevelEncoding(),
				dp.UncompressedSize(), dp.Statistics())
		case *file.DataPageV2:
			page = file.NewDataPageV2WithStats(newBuf, dp.NumValues(), dp.NumNulls(), dp.NumRows(),
				parquet.Encoding(dp.Encoding()), dp.DefinitionLevelByteLen(), dp.RepetitionLevelByteLen(),
				dp.UncompressedSize(), true, dp.Statistics())
		default:
			return fmt.Errorf("mergeColumnWriter: unexpected page type %T", page)
		}
		defer page.Release()
	}
	if _, err := cw.pageWriter.WriteDataPage(page); err != nil {
		return fmt.Errorf("writing data page: %w", err)
	}
	return nil
}

// Close finalizes the column chunk: writes its metadata to the sink. After Close, no more pages
// may be written. We pass hasDict=false / fallback=false because mergeWriter never accepts
// dictionary pages — the upstream scratch writer is configured with dictionary encoding off so
// the inputs are always plain-encoded data pages.
func (cw *mergeColumnWriter) Close() error {
	if cw.closed {
		return nil
	}
	cw.closed = true
	if err := cw.pageWriter.Close(false, false); err != nil {
		return fmt.Errorf("closing page writer: %w", err)
	}
	return nil
}
