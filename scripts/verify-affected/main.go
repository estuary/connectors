// The verify-affected script determines which production collections and which
// live materialization tasks are affected by a new connector feature, in a way
// that eliminates the most common errors of hand-written ad-hoc queries.
//
// It connects to the control-plane Postgres database (via the DATABASE_URL
// environment variable) and runs a fixed, tested query that links collections to
// the materializations that actually read them via the `reads_from` dependency
// array on `live_specs` -- never a tenant-prefix heuristic. The only
// developer-supplied part is a predicate selecting "affected" collections, which
// is either a curated, snapshot-tested registry check (--check) or an ad-hoc SQL
// fragment (--check-sql).
//
// Every run is gated by two independent reviews:
//
//	1. A local Claude Opus 4.8 review of the rendered query and results. This is
//	   a hard gate: a CONCERNS verdict blocks the run unless --override-ai-review.
//	2. An explicit human sign-off (interactive confirmation or --approver).
//
// Both reviews, the rendered SQL, and the full affected-task list are written to
// a markdown audit report.
//
// See docs/feature_flags.md for how this fits into the bulk-editing workflow.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

var scriptDescription = `
The verify-affected script finds the production collections and live
materialization tasks affected by a connector feature, linking collections to
the tasks that actually read them (via reads_from, not a name-prefix guess).
Every run is gated by a local Claude Opus 4.8 review and a human sign-off, and
writes a markdown audit report.

See docs/feature_flags.md for the intended workflow.
`

var (
	logLevel = flag.String("log_level", "info", "The log level to print at.")

	connector   = flag.String("connector", "", "Required. Connector short name (e.g. 'materialize-postgres'; variants auto-resolved) or a full 'ghcr.io/estuary/...' image URL.")
	checkName   = flag.String("check", "", "Name of a curated registry check (see --list-checks). Mutually exclusive with --check-sql.")
	checkSQL    = flag.String("check-sql", "", "Ad-hoc SQL boolean predicate over the collection row (alias 'c'), e.g. \"c.spec::text LIKE '%\\\"enum\\\":%'\". Triggers mandatory AI + human review.")
	column      = flag.String("column", "", "Column the predicate inspects: 'spec' or 'built_spec'. Defaults to the registry check's column, or 'built_spec' for ad-hoc predicates.")
	approver    = flag.String("approver", "", "Human reviewer name. Records explicit, non-interactive sign-off (marks the report APPROVED).")
	overrideAI  = flag.Bool("override-ai-review", false, "Proceed despite a CONCERNS verdict from the AI review (the override is recorded in the report).")
	claudeModel = flag.String("claude-model", "claude-opus-4-8", "Model passed to `claude -p --model`.")
	output      = flag.String("output", "", "Path for the markdown audit report. Defaults to ./verify-affected-<connector>-<timestamp>.md")
	listChecks  = flag.Bool("list-checks", false, "Print the available registry checks and exit.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\nusage of %s:\n", scriptDescription, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if lvl, err := log.ParseLevel(*logLevel); err != nil {
		log.WithFields(log.Fields{"level": *logLevel, "err": err}).Fatal("invalid log level")
	} else {
		log.SetLevel(lvl)
	}
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, PadLevelText: true})

	if *listChecks {
		fmt.Print(registryListing())
		return
	}

	if err := run(context.Background()); err != nil {
		log.WithField("err", err).Fatal("verification failed")
	}
}

// resolvedCheck is the validated, ready-to-render verification criteria.
type resolvedCheck struct {
	Name        string
	Description string
	Predicate   string
	Column      string
	AdHoc       bool
}

type affectedRow struct {
	Task       string
	Image      string
	Collection string
}

type tenantCount struct {
	Tenant      string
	Tasks       int
	Collections int
}

type summary struct {
	Rows            []affectedRow
	TaskCount       int
	CollectionCount int
	ByTenant        []tenantCount
}

func run(ctx context.Context) error {
	check, err := resolveCheck()
	if err != nil {
		return err
	}

	images, err := resolveImages(*connector)
	if err != nil {
		return fmt.Errorf("resolving connector images: %w", err)
	}
	images = sortedStrings(images)
	log.WithField("images", images).Info("resolved connector images")

	sql := renderQuery(check.Predicate, check.Column)

	// Print the exact query (and its $1 binding) before running it, so the
	// developer can see precisely what is being executed against the control plane.
	fmt.Printf("\nExecuting query against DATABASE_URL (read-only):\n\n%s\n\n  $1 = %v\n\n",
		sql, images)

	rows, err := queryAffected(ctx, images, sql)
	if err != nil {
		return err
	}
	sum := summarize(rows)
	log.WithFields(log.Fields{"tasks": sum.TaskCount, "collections": sum.CollectionCount}).Info("query complete")

	// First pair of eyes: the AI review, a hard gate.
	prompt := buildReviewPrompt(check, images, sql, sum)
	log.WithField("model", *claudeModel).Info("running AI review (this calls the local claude CLI)")
	verdict, err := runReview(ctx, *claudeModel, prompt)
	if err != nil {
		return fmt.Errorf("AI review error: %w", err)
	}

	report := reportData{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Connector:  *connector,
		Images:     images,
		Check:      check,
		SQL:        sql,
		AI:         verdict,
		AIModel:    *claudeModel,
		Overridden: *overrideAI,
		Summary:    sum,
	}

	blocked := verdict.Verdict == verdictConcerns && !*overrideAI
	if blocked {
		report.Status = "BLOCKED (AI CONCERNS)"
		if path, werr := writeReport(report); werr == nil {
			fmt.Printf("\nReport written to %s\n", path)
		}
		fmt.Fprintln(os.Stderr, "\nAI review returned CONCERNS:")
		for _, n := range verdict.Notes {
			fmt.Fprintf(os.Stderr, "  - %s\n", n)
		}
		return fmt.Errorf("blocked by AI review; revise the check or re-run with --override-ai-review")
	}

	// Show the report (with a sampled task list) before asking for sign-off; the
	// complete list is preserved in the audit report file.
	fmt.Println()
	fmt.Println(renderReport(report, terminalRowSample))

	// Second pair of eyes: the human sign-off.
	status, name := confirmHuman(*approver, len(rows))
	report.Status = status
	report.Approver = name

	path, err := writeReport(report)
	if err != nil {
		return fmt.Errorf("writing report: %w", err)
	}
	fmt.Printf("\nStatus: %s\nReport written to %s\n", status, path)
	if status == "PENDING" {
		fmt.Println("A human reviewer must confirm these results. Re-run with --approver=\"<name>\" or approve interactively.")
	}
	return nil
}

func resolveCheck() (resolvedCheck, error) {
	if *connector == "" {
		return resolvedCheck{}, fmt.Errorf("--connector is required")
	}
	if (*checkName == "") == (*checkSQL == "") {
		return resolvedCheck{}, fmt.Errorf("exactly one of --check or --check-sql must be provided (see --list-checks)")
	}

	if *checkName != "" {
		c, ok := lookupCheck(*checkName)
		if !ok {
			return resolvedCheck{}, fmt.Errorf("unknown check %q; see --list-checks", *checkName)
		}
		col := c.Column
		if *column != "" {
			col = *column
		}
		if err := validateColumn(col); err != nil {
			return resolvedCheck{}, err
		}
		return resolvedCheck{Name: c.Name, Description: c.Description, Predicate: c.Predicate, Column: col}, nil
	}

	col := "built_spec"
	if *column != "" {
		col = *column
	}
	if err := validateColumn(col); err != nil {
		return resolvedCheck{}, err
	}
	return resolvedCheck{
		Name:        "(ad-hoc)",
		Description: "Ad-hoc predicate supplied on the command line: " + *checkSQL,
		Predicate:   *checkSQL,
		Column:      col,
		AdHoc:       true,
	}, nil
}

func validateColumn(col string) error {
	if col != "spec" && col != "built_spec" {
		return fmt.Errorf("--column must be 'spec' or 'built_spec', got %q", col)
	}
	return nil
}

// requiredColumns are the live_specs columns the query depends on. They are
// verified to exist before running so that schema drift fails loudly rather than
// producing silently-wrong results.
var requiredColumns = []string{"catalog_name", "spec_type", "spec", "built_spec", "connector_image_name", "reads_from"}

func queryAffected(ctx context.Context, images []string, sql string) ([]affectedRow, error) {
	dburl := os.Getenv("DATABASE_URL")
	if dburl == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set; it must be a postgresql://... connection string for the control plane")
	}

	conn, err := pgx.Connect(ctx, dburl)
	if err != nil {
		return nil, fmt.Errorf("connecting to DATABASE_URL: %w", err)
	}
	defer conn.Close(ctx)

	if err := verifySchema(ctx, conn); err != nil {
		return nil, err
	}

	// A read-only transaction guarantees the (intentionally free-form) predicate
	// cannot mutate the control plane, regardless of what SQL it contains.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("starting read-only transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, sql, images)
	if err != nil {
		return nil, annotatePredicateError(err)
	}
	defer rows.Close()

	var out []affectedRow
	for rows.Next() {
		var r affectedRow
		if err := rows.Scan(&r.Task, &r.Image, &r.Collection); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, annotatePredicateError(err)
	}
	return out, nil
}

// annotatePredicateError reframes the planner-level errors that a malformed
// predicate produces (the predicate is the only free-form part of the query)
// into an actionable message, rather than leaking a bare SQLSTATE.
func annotatePredicateError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42804": // datatype_mismatch, e.g. "argument of AND must be type boolean"
			return fmt.Errorf("the predicate must be a boolean expression over the collection row (alias `c`), "+
				"e.g. `c.built_spec::text LIKE '%%enum%%'`; a bare value like `c.built_spec::text` is not boolean "+
				"(postgres: %s)", pgErr.Message)
		case "42703", "42883", "42601", "42P01": // undefined column/function, syntax, undefined table
			return fmt.Errorf("the predicate appears invalid (%s: %s); check the --check-sql expression references "+
				"alias `c` and existing columns", pgErr.Code, pgErr.Message)
		}
		return fmt.Errorf("running verification query (postgres %s: %s)", pgErr.Code, pgErr.Message)
	}
	return fmt.Errorf("running verification query: %w", err)
}

func verifySchema(ctx context.Context, conn *pgx.Conn) error {
	rows, err := conn.Query(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_name = 'live_specs'`)
	if err != nil {
		return fmt.Errorf("introspecting live_specs schema: %w", err)
	}
	defer rows.Close()

	present := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scanning column name: %w", err)
		}
		present[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var missing []string
	for _, c := range requiredColumns {
		if !present[c] {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("live_specs is missing expected column(s) %s; the control-plane schema may have changed. "+
			"If reads_from is gone, switch the join to live_spec_flows (source_id=collection, target_id=materialization)",
			strings.Join(missing, ", "))
	}
	return nil
}

func writeReport(r reportData) (string, error) {
	path := *output
	if path == "" {
		stamp := strings.ReplaceAll(r.Timestamp, ":", "")
		path = fmt.Sprintf("./verify-affected-%s-%s.md", sanitize(*connector), stamp)
	}
	return path, os.WriteFile(path, []byte(renderReport(r, 0)), 0644)
}

func sanitize(s string) string {
	return strings.NewReplacer("/", "_", ":", "_").Replace(s)
}
