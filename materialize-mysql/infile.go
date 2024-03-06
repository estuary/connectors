package main

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/csv"
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
)

type infile struct {
	w    *csv.Writer
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
		w:    csv.NewWriter(&buff),
		buff: &buff,
	}
}

// write writes a single row to the infile buffer. If the buffer is sufficiently larger after the
// write, it will flush the batch to MySQL per `drainQuery`.
func (i *infile) write(ctx context.Context, converted []any, txn *stdsql.Tx, drainQuery string) error {
	if record, err := rowToCSVRecord(converted); err != nil {
		return fmt.Errorf("error encoding row to CSV: %w", err)
	} else if err := i.w.Write(record); err != nil {
		return fmt.Errorf("writing csv record: %w", err)
	}

	i.w.Flush()
	if err := i.w.Error(); err != nil {
		return fmt.Errorf("flushing csv to buffer: %w", err)
	}

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

func rowToCSVRecord(row []any) ([]string, error) {
	var record = make([]string, len(row))
	for i, v := range row {
		switch value := v.(type) {
		case []byte:
			record[i] = string(value)
		case string:
			record[i] = value
		case nil:
			// See https://dev.mysql.com/doc/refman/8.0/en/problems-with-null.html
			record[i] = "NULL"
		case bool:
			if !value {
				record[i] = "0"
			} else {
				record[i] = "1"
			}
		default:
			b, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("encoding value as json: %w", err)
			}
			record[i] = string(b)
		}
	}

	return record, nil
}
