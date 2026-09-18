package main

import (
	"bytes"
	"fmt"
	"strconv"
)

// dsdgen ends every line with a delimiter (-TERMINATE Y, its default).
func countFields(line []byte) int {
	return bytes.Count(trimTerminator(line), []byte{'|'}) + 1
}

func trimTerminator(line []byte) []byte {
	if n := len(line); n > 0 && line[n-1] == '|' {
		return line[:n-1]
	}
	return line
}

func decodeLine(t *tableDef, line []byte) ([]byte, error) {
	var fields = bytes.Split(trimTerminator(line), []byte{'|'})
	if len(fields) != len(t.Columns) {
		return nil, fmt.Errorf("%s: expected %d fields, got %d", t.Name, len(t.Columns), len(fields))
	}
	var out = make([]byte, 0, len(line)*2)
	out = append(out, '{')
	var first = true
	for i, col := range t.Columns {
		var f = fields[i]
		if len(f) == 0 {
			if col.NotNull {
				return nil, fmt.Errorf("%s: NOT NULL column %s is empty", t.Name, col.Name)
			}
			continue
		}
		if !first {
			out = append(out, ',')
		}
		first = false
		out = append(out, '"')
		out = append(out, col.Name...)
		out = append(out, '"', ':')
		switch col.Kind {
		case kindInteger:
			if !isInteger(f) {
				return nil, fmt.Errorf("%s: column %s: %q is not an integer", t.Name, col.Name, f)
			}
			out = append(out, f...)
		case kindDecimal:
			if !isDecimal(f) {
				return nil, fmt.Errorf("%s: column %s: %q is not a decimal", t.Name, col.Name, f)
			}
			out = append(out, '"')
			out = append(out, f...)
			out = append(out, '"')
		case kindDate:
			if !isDate(f) {
				return nil, fmt.Errorf("%s: column %s: %q is not a YYYY-MM-DD date", t.Name, col.Name, f)
			}
			out = append(out, '"')
			out = append(out, f...)
			out = append(out, '"')
		case kindString:
			out = appendJSONString(out, f)
		}
	}
	out = append(out, '}')
	return out, nil
}

func isInteger(f []byte) bool {
	if len(f) > 0 && f[0] == '-' {
		f = f[1:]
	}
	if len(f) == 0 {
		return false
	}
	for _, c := range f {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isDecimal(f []byte) bool {
	if len(f) > 0 && f[0] == '-' {
		f = f[1:]
	}
	var digits, dots int
	for _, c := range f {
		switch {
		case c >= '0' && c <= '9':
			digits++
		case c == '.':
			dots++
		default:
			return false
		}
	}
	return digits > 0 && dots <= 1
}

func isDate(f []byte) bool {
	if len(f) != 10 || f[4] != '-' || f[7] != '-' {
		return false
	}
	for i, c := range f {
		if i == 4 || i == 7 {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// dsdgen only emits ASCII, so quote, backslash and control characters are the
// only escapes needed.
func appendJSONString(out, f []byte) []byte {
	out = append(out, '"')
	var start int
	for i, c := range f {
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		out = append(out, f[start:i]...)
		switch c {
		case '"', '\\':
			out = append(out, '\\', c)
		case '\n':
			out = append(out, '\\', 'n')
		case '\r':
			out = append(out, '\\', 'r')
		case '\t':
			out = append(out, '\\', 't')
		default:
			out = append(out, `\u00`...)
			out = append(out, strconv.FormatUint(uint64(c)>>4, 16)[0], "0123456789abcdef"[c&0xf])
		}
		start = i + 1
	}
	out = append(out, f[start:]...)
	return append(out, '"')
}
