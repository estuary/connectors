// Command tpcds records how long a production materialization of the TPC-DS
// dataset (captured by source-tpc-ds) took to load in full, and checks that
// every table received exactly the number of documents dsdgen produces at
// that scale factor. It reads only Flow's own specs and per-transaction stats
// through flowctl, so it works for any materialization connector.
//
//	go run ./tests/benchmark/tpcds -materialization estuary/bench-tpc-100/materialize-databricks
//
// Results land under tests/benchmark/tpcds/results/<connector>/sf<scale>/ and
// are meant to be committed, so runs of the same connector and scale can be
// compared over time.
package main

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed oracle/*.json
var oracleFS embed.FS

func main() {
	var (
		materialization = flag.String("materialization", "", "name of the materialization task to record (required)")
		since           = flag.String("since", "14d", "how far back to read stats; must cover the whole run")
		resultsDir      = flag.String("results", "", "directory to write the result under (default: tests/benchmark/tpcds/results next to this program)")
		fromDir         = flag.String("from-dir", "", "offline mode: read flow.yaml, mat-stats.jsonl and cap-stats.jsonl from this directory instead of calling flowctl")
	)
	flag.Parse()
	if *materialization == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *resultsDir == "" {
		_, self, _, _ := runtimeCaller()
		*resultsDir = filepath.Join(filepath.Dir(self), "results")
	}

	in, err := gather(*materialization, *since, *fromDir)
	if err != nil {
		fail(err)
	}
	oracle, err := loadOracle(in.scale)
	if err != nil {
		fail(err)
	}
	res := analyze(in, oracle)
	printSummary(res)
	if !res.Complete {
		fmt.Fprintln(os.Stderr, "run is not complete; nothing recorded")
		os.Exit(1)
	}
	out, err := writeResult(*resultsDir, res)
	if err != nil {
		fail(err)
	}
	fmt.Printf("\nrecorded %s\n", out)
	if !res.OK {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// inputs is everything the analysis needs, however it was obtained.
type inputs struct {
	materialization string
	capture         string
	matImage        string
	capImage        string
	scale           string
	tables          map[string]string // collection -> TPC-DS table, from the capture bindings
	matStats        []statsRecord
	capStats        []statsRecord
}

type statsRecord struct {
	TS          time.Time
	Materialize map[string]bindingStats `json:"materialize"`
	Capture     map[string]bindingStats `json:"capture"`
	TxnCount    int                     `json:"txnCount"`
}

type bindingStats struct {
	Out *struct {
		DocsTotal  int64 `json:"docsTotal"`
		BytesTotal int64 `json:"bytesTotal"`
	} `json:"out"`
}

func gather(materialization, since, fromDir string) (*inputs, error) {
	var dir = fromDir
	if dir == "" {
		var err error
		if dir, err = os.MkdirTemp("", "tpcds-bench"); err != nil {
			return nil, err
		}
		defer os.RemoveAll(dir)
		if err := flowctl(nil, "catalog", "pull-specs", "--name", materialization, "--target", filepath.Join(dir, "flow.yaml"), "--overwrite"); err != nil {
			return nil, err
		}
	}
	in, err := readSpecs(dir, materialization)
	if err != nil {
		return nil, err
	}
	if fromDir == "" {
		// The capture spec is needed for the scale factor and table names.
		if err := flowctl(nil, "catalog", "pull-specs", "--name", in.capture, "--target", filepath.Join(dir, "flow.yaml"), "--overwrite"); err != nil {
			return nil, err
		}
		if in, err = readSpecs(dir, materialization); err != nil {
			return nil, err
		}
		for _, s := range []struct{ task, file string }{{materialization, "mat-stats.jsonl"}, {in.capture, "cap-stats.jsonl"}} {
			f, err := os.Create(filepath.Join(dir, s.file))
			if err != nil {
				return nil, err
			}
			err = flowctl(f, "raw", "stats", "--task", s.task, "--since", since)
			f.Close()
			if err != nil {
				return nil, err
			}
		}
	}
	if in.matStats, err = readStats(filepath.Join(dir, "mat-stats.jsonl")); err != nil {
		return nil, err
	}
	if in.capStats, err = readStats(filepath.Join(dir, "cap-stats.jsonl")); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	return in, nil
}

func flowctl(stdout *os.File, args ...string) error {
	var cmd = exec.Command("flowctl", args...)
	cmd.Stdout = stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("flowctl %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

// readSpecs walks the pulled specs (flowctl writes one flow.yaml per prefix,
// linked by imports) for the materialization, its capture and their configs.
func readSpecs(dir, materialization string) (*inputs, error) {
	type connector struct {
		Image  string `yaml:"image"`
		Config any    `yaml:"config"`
	}
	type spec struct {
		Captures map[string]struct {
			Endpoint struct{ Connector connector } `yaml:"endpoint"`
			Bindings []struct {
				Resource struct{ Table string } `yaml:"resource"`
				Target   string                 `yaml:"target"`
			} `yaml:"bindings"`
		} `yaml:"captures"`
		Materializations map[string]struct {
			Source   struct{ Capture string }      `yaml:"source"`
			Endpoint struct{ Connector connector } `yaml:"endpoint"`
		} `yaml:"materializations"`
	}
	var in = &inputs{materialization: materialization, tables: map[string]string{}}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".config.yaml") {
			return err
		}
		bs, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var s spec
		if err := yaml.Unmarshal(bs, &s); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		if m, ok := s.Materializations[materialization]; ok {
			in.capture = m.Source.Capture
			in.matImage = m.Endpoint.Connector.Image
		}
		for name, c := range s.Captures {
			if name != in.capture && in.capture != "" {
				continue
			}
			in.capImage = c.Endpoint.Connector.Image
			for _, b := range c.Bindings {
				in.tables[b.Target] = b.Resource.Table
			}
			cfg, err := resolveConfig(filepath.Dir(p), c.Endpoint.Connector.Config)
			if err != nil {
				return err
			}
			if v, ok := cfg["scale"]; ok {
				in.scale = fmt.Sprint(v)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if in.matImage == "" {
		return nil, fmt.Errorf("materialization %s not found in pulled specs", materialization)
	}
	if in.capture == "" {
		return nil, fmt.Errorf("materialization %s has no source.capture; it must be bound to a source-tpc-ds capture", materialization)
	}
	return in, nil
}

// resolveConfig returns a connector config given inline or as a file path
// relative to the spec. Only the non-secret keys are needed here, so an
// encrypted file is fine.
func resolveConfig(dir string, cfg any) (map[string]any, error) {
	switch c := cfg.(type) {
	case map[string]any:
		return c, nil
	case string:
		bs, err := os.ReadFile(filepath.Join(dir, c))
		if err != nil {
			return nil, err
		}
		var out map[string]any
		return out, yaml.Unmarshal(bs, &out)
	}
	return map[string]any{}, nil
}

func readStats(p string) ([]statsRecord, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []statsRecord
	var sc = bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		var raw struct {
			statsRecord
			TS string `json:"ts"`
		}
		if err := json.Unmarshal(sc.Bytes(), &raw); err != nil {
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		if raw.Materialize == nil && raw.Capture == nil {
			continue // heartbeat / interval records
		}
		ts, err := time.Parse(time.RFC3339Nano, raw.TS)
		if err != nil {
			return nil, fmt.Errorf("%s: bad ts %q", p, raw.TS)
		}
		raw.statsRecord.TS = ts
		out = append(out, raw.statsRecord)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TS.Before(out[j].TS) })
	return out, sc.Err()
}

func loadOracle(scale string) (map[string]int64, error) {
	bs, err := oracleFS.ReadFile("oracle/sf" + scale + ".json")
	if err != nil {
		var names []string
		fs.WalkDir(oracleFS, "oracle", func(p string, d fs.DirEntry, _ error) error {
			if !d.IsDir() {
				names = append(names, strings.TrimSuffix(strings.TrimPrefix(path.Base(p), "sf"), ".json"))
			}
			return nil
		})
		return nil, fmt.Errorf("no row-count oracle for scale %q (have %s); generate one with dsdgen and add it to oracle/", scale, strings.Join(names, ", "))
	}
	var out map[string]int64
	return out, json.Unmarshal(bs, &out)
}

// Result is what gets persisted for one complete run.
type Result struct {
	Materialization string           `json:"materialization"`
	Capture         string           `json:"capture"`
	MaterializeImg  string           `json:"materialization_image"`
	CaptureImg      string           `json:"capture_image"`
	Scale           string           `json:"scale"`
	RecordedAt      time.Time        `json:"recorded_at"`
	Complete        bool             `json:"complete"`
	OK              bool             `json:"ok"`
	Materialize     phase            `json:"materialize"`
	CaptureP        *phase           `json:"capture_phase,omitempty"`
	Tables          map[string]table `json:"tables"`
}

type phase struct {
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	Seconds      float64   `json:"seconds"`
	Transactions int       `json:"transactions"`
	Docs         int64     `json:"docs"`
	Bytes        int64     `json:"bytes"`
	DocsPerSec   float64   `json:"docs_per_sec"`
	MBPerSec     float64   `json:"mb_per_sec"`
}

type table struct {
	Expected     int64      `json:"expected"`
	Materialized int64      `json:"materialized"`
	Captured     int64      `json:"captured,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	Seconds      float64    `json:"seconds,omitempty"`
	OK           bool       `json:"ok"`
}

// analyze replays the per-transaction stats. A table is complete at the first
// transaction where its cumulative stored documents reach the oracle's count;
// the run is complete when every table is, and timed from the first
// transaction that stored anything to the last completion.
func analyze(in *inputs, oracle map[string]int64) *Result {
	var res = &Result{
		Materialization: in.materialization, Capture: in.capture,
		MaterializeImg: in.matImage, CaptureImg: in.capImage,
		Scale: in.scale, RecordedAt: time.Now().UTC(), Tables: map[string]table{},
	}
	for _, tbl := range in.tables {
		res.Tables[tbl] = table{Expected: oracle[tbl]}
	}
	var matPhase, capPhase phase
	matPhase, res.Tables = replay(in.matStats, in.tables, res.Tables, true)
	res.Materialize = matPhase
	if len(in.capStats) > 0 {
		capPhase, res.Tables = replay(in.capStats, in.tables, res.Tables, false)
		res.CaptureP = &capPhase
	}
	res.Complete, res.OK = true, true
	for name, t := range res.Tables {
		if t.Expected == 0 {
			t.OK = false
		} else if t.Materialized == t.Expected {
			t.OK = true
		} else {
			t.OK = false
			if t.Materialized < t.Expected {
				res.Complete = false
			}
		}
		res.OK = res.OK && t.OK
		res.Tables[name] = t
	}
	if res.Complete && res.OK {
		res.Materialize.CompletedAt = latestCompletion(res.Tables)
		res.Materialize.Seconds = res.Materialize.CompletedAt.Sub(res.Materialize.StartedAt).Seconds()
		res.Materialize.DocsPerSec = float64(res.Materialize.Docs) / res.Materialize.Seconds
		res.Materialize.MBPerSec = float64(res.Materialize.Bytes) / res.Materialize.Seconds / 1e6
	}
	return res
}

func replay(stats []statsRecord, tables map[string]string, acc map[string]table, materialize bool) (phase, map[string]table) {
	var ph phase
	var cum = map[string]int64{}
	for _, rec := range stats {
		var m = rec.Capture
		if materialize {
			m = rec.Materialize
		}
		var stored bool
		for coll, bs := range m {
			tbl, ok := tables[coll]
			if !ok || bs.Out == nil || bs.Out.DocsTotal == 0 {
				continue
			}
			stored = true
			if ph.StartedAt.IsZero() {
				ph.StartedAt = rec.TS
			}
			cum[tbl] += bs.Out.DocsTotal
			ph.Docs += bs.Out.DocsTotal
			ph.Bytes += bs.Out.BytesTotal
			var t = acc[tbl]
			if materialize {
				t.Materialized = cum[tbl]
				if t.CompletedAt == nil && t.Expected > 0 && cum[tbl] >= t.Expected {
					var ts = rec.TS
					t.CompletedAt = &ts
					t.Seconds = ts.Sub(ph.StartedAt).Seconds()
				}
			} else {
				t.Captured = cum[tbl]
			}
			acc[tbl] = t
		}
		if stored {
			ph.CompletedAt = rec.TS
			ph.Transactions += max(rec.TxnCount, 1)
		}
	}
	if !materialize && !ph.StartedAt.IsZero() {
		ph.Seconds = ph.CompletedAt.Sub(ph.StartedAt).Seconds()
		if ph.Seconds > 0 {
			ph.DocsPerSec = float64(ph.Docs) / ph.Seconds
			ph.MBPerSec = float64(ph.Bytes) / ph.Seconds / 1e6
		}
	}
	return ph, acc
}

func latestCompletion(tables map[string]table) time.Time {
	var out time.Time
	for _, t := range tables {
		if t.CompletedAt != nil && t.CompletedAt.After(out) {
			out = *t.CompletedAt
		}
	}
	return out
}

func printSummary(res *Result) {
	var names []string
	for n := range res.Tables {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Printf("%s (%s) <- %s (%s), scale %s\n\n", res.Materialization, res.MaterializeImg, res.Capture, res.CaptureImg, res.Scale)
	fmt.Printf("%-24s %14s %14s %10s  %s\n", "table", "expected", "materialized", "seconds", "status")
	for _, n := range names {
		var t = res.Tables[n]
		var status = "ok"
		switch {
		case t.Expected == 0:
			status = "no oracle entry"
		case t.Materialized < t.Expected:
			status = fmt.Sprintf("incomplete (%.1f%%)", 100*float64(t.Materialized)/float64(t.Expected))
		case t.Materialized > t.Expected:
			status = "TOO MANY"
		}
		fmt.Printf("%-24s %14d %14d %10.0f  %s\n", n, t.Expected, t.Materialized, t.Seconds, status)
	}
	if res.Complete && res.OK {
		var m = res.Materialize
		fmt.Printf("\nmaterialized %d docs (%.1f GB) in %s: %.0f docs/s, %.1f MB/s, %d transactions\n",
			m.Docs, float64(m.Bytes)/1e9, time.Duration(m.Seconds*float64(time.Second)).Round(time.Second), m.DocsPerSec, m.MBPerSec, m.Transactions)
	}
	if res.CaptureP != nil && res.CaptureP.Seconds > 0 {
		var c = res.CaptureP
		fmt.Printf("captured %d docs in %s: %.0f docs/s, %.1f MB/s\n", c.Docs, time.Duration(c.Seconds*float64(time.Second)).Round(time.Second), c.DocsPerSec, c.MBPerSec)
	}
}

// writeResult stores the run as results/<connector>/sf<scale>/<date>-<tag>.json.
func writeResult(dir string, res *Result) (string, error) {
	var image = path.Base(res.MaterializeImg)
	connector, tag, _ := strings.Cut(image, ":")
	if tag == "" {
		tag = "untagged"
	}
	var out = filepath.Join(dir, connector, "sf"+res.Scale, res.Materialize.CompletedAt.UTC().Format("2006-01-02T150405Z")+"-"+tag+".json")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return "", err
	}
	var buf bytes.Buffer
	var enc = json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return "", err
	}
	return out, os.WriteFile(out, buf.Bytes(), 0o644)
}
