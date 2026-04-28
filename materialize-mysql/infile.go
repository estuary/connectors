package main

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-sql-driver/mysql"
)

const (
	// Limit the maximum size of buffered data before flushing a batch.
	batchSizeThreshold = 10 * 1024 * 1024

	// File name representations used both here and in the SQL templates.
	loadInfileName   = "flow_batch_data_load"
	insertInfileName = "flow_batch_data_insert"
	updateInfileName = "flow_batch_data_update"
	deleteInfileName = "flow_batch_data_delete"
)

type infile struct {
	buff *bytes.Buffer
}

func newInfile(readerName string) *infile {
	var buff bytes.Buffer

	mysql.RegisterReaderHandler(readerName,
		func() io.Reader {
			return &buff
		},
	)

	return &infile{
		buff: &buff,
	}
}

// write writes a single row to the infile buffer. If the buffer is sufficiently larger after the
// write, it will flush the batch to MySQL per `drainQuery`.
//
// Rows are encoded with backslash escaping rather than RFC4180-style doubled
// quotes because SingleStore's LOAD DATA does not recognize "" as an escape for
// a literal " inside an enclosed field when ESCAPED BY is empty. The matching
// SQL template uses `ESCAPED BY '\\'` and no `ENCLOSED BY`, which both MySQL
// and SingleStore parse identically.
func (i *infile) write(ctx context.Context, converted []any, txn *stdsql.Tx, drainQuery string) error {
	for j, v := range converted {
		if j > 0 {
			i.buff.WriteByte(',')
		}
		if err := writeInfileField(i.buff, v); err != nil {
			return fmt.Errorf("encoding row to infile: %w", err)
		}
	}
	i.buff.WriteByte('\n')

	if i.buff.Len() > batchSizeThreshold {
		if err := i.drain(ctx, txn, drainQuery); err != nil {
			return fmt.Errorf("draining infile after write: %w", err)
		}
	}

	return nil
}

func (i *infile) drain(ctx context.Context, txn *stdsql.Tx, drainQuery string) error {
	if i.buff.Len() == 0 {
		// Simplification for callers when there is nothing to drain, which would happen if the
		// infile was drained based on size just prior to cycling through to a different binding.
		return nil
	}

	if _, err := txn.ExecContext(ctx, drainQuery); err != nil {
		return fmt.Errorf("executing infile drain query: %w", err)
	}

	return nil
}

func writeInfileField(buf *bytes.Buffer, v any) error {
	switch value := v.(type) {
	case nil:
		// MySQL/SingleStore interpret \N as SQL NULL when ESCAPED BY '\\'.
		buf.WriteString(`\N`)
		return nil
	case []byte:
		writeEscapedField(buf, string(value))
		return nil
	case string:
		writeEscapedField(buf, value)
		return nil
	case bool:
		if value {
			buf.WriteByte('1')
		} else {
			buf.WriteByte('0')
		}
		return nil
	default:
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encoding value as json: %w", err)
		}
		writeEscapedField(buf, string(b))
		return nil
	}
}

// writeEscapedField writes a string field, escaping characters that the
// LOAD DATA parser would otherwise interpret as separators or NULL markers.
func writeEscapedField(buf *bytes.Buffer, s string) {
	for j := 0; j < len(s); j++ {
		c := s[j]
		switch c {
		case '\\':
			buf.WriteString(`\\`)
		case '\n':
			buf.WriteString(`\n`)
		case '\r':
			buf.WriteString(`\r`)
		case '\t':
			buf.WriteString(`\t`)
		case 0:
			buf.WriteString(`\0`)
		case ',':
			buf.WriteString(`\,`)
		default:
			buf.WriteByte(c)
		}
	}
}
