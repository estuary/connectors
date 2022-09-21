package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
)

type pgBatch struct {
	buf      []byte
	handlers []pgResultFn
}

func newPGBatch() pgBatch {
	return pgBatch{
		buf:      []byte{'Q', 0, 0, 0, 0},
		handlers: nil,
	}
}

type pgResultFn func(*pgconn.ResultReader) error

func (b *pgBatch) queue(fn pgResultFn, sql string) {
	b.buf = append(append(b.buf, sql...), '\n')
	b.handlers = append(b.handlers, fn)
}

func (b *pgBatch) queueParams(fn pgResultFn, partialSQL string, params ...interface{}) {
	b.buf = append(append(b.buf, partialSQL...), '(')

	for i, p := range params {
		if i > 0 {
			b.buf = append(b.buf, ',')
		}

		switch pp := p.(type) {
		case int:
			b.buf = strconv.AppendInt(b.buf, int64(pp), 10)
		case int64:
			b.buf = strconv.AppendInt(b.buf, pp, 10)
		case float64:
			b.buf = strconv.AppendFloat(b.buf, pp, 'E', -1, 64)
		case string:
			b.buf = append(append(append(b.buf, '\''), strings.ReplaceAll(pp, "'", "''")...), '\'')
		case []byte:
			b.buf = append(append(append(b.buf, '\''), bytes.ReplaceAll(pp, []byte("'"), []byte("''"))...), '\'')
		case json.RawMessage:
			if len(pp) == 0 {
				b.buf = append(b.buf, "null"...)
			} else {
				b.buf = append(append(append(b.buf, '\''), bytes.ReplaceAll(pp, []byte("'"), []byte("''"))...), '\'')
			}
		case bool:
			b.buf = strconv.AppendBool(b.buf, pp)
		case nil:
			b.buf = append(b.buf, "null"...)
		default:
			panic(fmt.Sprintf("unsupported type %T", p))
		}
	}

	b.buf = append(b.buf, ");\n"...)
	b.handlers = append(b.handlers, fn)
}

func (b *pgBatch) roundTrip(ctx context.Context, conn *pgconn.PgConn) error {
	if len(b.handlers) == 0 {
		return nil // Nothing to do.
	}

	binary.BigEndian.PutUint32(b.buf[1:], uint32(len(b.buf)))
	b.buf = append(b.buf, 0)

	if err := conn.SendBytes(ctx, b.buf); err != nil {
		return fmt.Errorf("sending SQL body: %w", err)
	}

	var mrr = conn.ReceiveResults(ctx)
	defer mrr.Close() // Okay to call 2x.

	var handlers = b.handlers

	for mrr.NextResult() {
		var rr = mrr.ResultReader()

		if len(handlers) == 0 {
			var tag, _ = rr.Close()

			var names []string
			var types []int
			for _, f := range rr.FieldDescriptions() {
				names = append(names, string(f.Name))
				types = append(types, int(f.DataTypeOID))
			}
			return fmt.Errorf(
				"all queued queries are processed but have extra server result %s with fields %v (OID %v)",
				tag, names, types)
		}
		if handlers[0] != nil {
			if err := handlers[0](rr); err != nil {
				return err
			}
		}
		handlers = handlers[1:] // Pop.

		if _, err := rr.Close(); err != nil {
			return fmt.Errorf("while reading end of SQL result set: %w", err)
		}
	}

	if err := mrr.Close(); err != nil {
		return fmt.Errorf("while reading end of batch SQL result sets: %w", err)
	}

	if l := len(handlers); l != 0 {
		return fmt.Errorf("all server results received, but still have %d remaining handlers", l)
	}

	b.buf = b.buf[:5]
	b.handlers = b.handlers[:0]

	return nil
}

func scanRow(
	connInfo *pgtype.ConnInfo,
	fields []pgproto3.FieldDescription,
	values [][]byte,
	outputs ...interface{},
) error {

	if l, r1 := len(fields), len(values); l != r1 {
		return fmt.Errorf("have %d fields with %d column values", l, r1)
	} else if r2 := len(outputs); l != r2 {
		return fmt.Errorf("have %d fields with %d output parameters", l, r2)
	}

	for i, value := range values {
		if err := connInfo.Scan(fields[i].DataTypeOID, fields[i].Format, value, outputs[i]); err != nil {
			return fmt.Errorf("scanning field %s (index %d): %w", fields[i].Name, i, err)
		}
	}
	return nil
}
