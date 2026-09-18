package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type generator struct {
	bin   string // dsdgen binary
	idx   string // tpcds.idx distributions file, passed on every invocation
	scale string // scale factor as given to -SCALE
}

func newGenerator(scale string) (*generator, error) {
	var bin = os.Getenv("DSDGEN")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("dsdgen"); err != nil {
			return nil, fmt.Errorf("dsdgen binary not found: set $DSDGEN or put it on $PATH")
		}
	}
	var idx = os.Getenv("DSDGEN_IDX")
	if idx == "" {
		idx = filepath.Join(filepath.Dir(bin), "tpcds.idx")
	}
	for _, p := range []string{bin, idx} {
		if _, err := os.Stat(p); err != nil {
			return nil, fmt.Errorf("dsdgen: %w", err)
		}
	}
	return &generator{bin: bin, idx: idx, scale: scale}, nil
}

func (g *generator) args(table string, extra ...string) []string {
	return append([]string{"-SCALE", g.scale, "-TABLE", table, "-DISTRIBUTIONS", g.idx}, extra...)
}

// For the sales tables dsdgen counts tickets or orders, not line items.
func (g *generator) rowCount(ctx context.Context, table string) (int64, error) {
	var stderr bytes.Buffer
	var cmd = exec.CommandContext(ctx, g.bin, g.args(table, "-_ROWCOUNT", "Y")...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("dsdgen row count for %s: %w: %s", table, err, strings.TrimSpace(stderr.String()))
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("dsdgen row count for %s: unexpected output %q", table, out)
	}
	return n, nil
}

func (g *generator) stream(ctx context.Context, table string, chunks, chunk int, fn func(line []byte) error) error {
	var args = g.args(table, "-_FILTER", "Y")
	// To split each binding's work into resumable chunks, the only mechanism
	// dsdgen offers is to divide a table's whole row space into N parts. So we
	// take the table's row count, split it by our chunk size, pass that as
	// -PARALLEL N, and to resume from the kth part add -CHILD k.
	if chunks > 1 {
		args = append(args, "-PARALLEL", strconv.Itoa(chunks), "-CHILD", strconv.Itoa(chunk))
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var cmd = exec.CommandContext(cctx, g.bin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting dsdgen: %w", err)
	}
	var scanner = bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1<<20), 1<<20)
	var scanErr error
	for scanner.Scan() {
		if scanErr = fn(scanner.Bytes()); scanErr != nil {
			break
		}
	}
	if scanErr == nil {
		scanErr = scanner.Err()
	}
	if scanErr != nil {
		cancel()
		_ = cmd.Wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return scanErr
	}
	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("dsdgen %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	log.WithFields(log.Fields{"table": table, "chunk": chunk, "chunks": chunks, "stderr": strings.TrimSpace(stderr.String())}).Debug("dsdgen finished")
	return nil
}
