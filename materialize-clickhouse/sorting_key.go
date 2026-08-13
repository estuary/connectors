package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
)

// tableKeyMeta is the table-level metadata that decides whether a table can hold
// a binding's rows without destroying them.
type tableKeyMeta struct {
	engine string
	// sortingKey is system.tables.sorting_key: the table's ORDER BY rendered as a
	// comma-separated expression list, empty for ORDER BY tuple().
	sortingKey string
}

// mergingEngines are the MergeTree variants that combine rows sharing a sorting
// key. The connector only ever creates ReplacingMergeTree tables, but any of
// these can be adopted, and all of them are destructive under a sorting key that
// does not match the binding's.
var mergingEngines = []string{"Replacing", "Summing", "Aggregating", "Collapsing", "Graphite"}

// mergesRows reports whether the table's engine combines rows that share a
// sorting key. Matching on a substring covers the Replicated* and Shared*
// (ClickHouse Cloud) prefixes: the production table that destroyed rows was a
// SharedReplacingMergeTree, which an exact comparison would have missed.
func (m tableKeyMeta) mergesRows() bool {
	for _, engine := range mergingEngines {
		if strings.Contains(m.engine, engine) {
			return true
		}
	}
	return false
}

// tableKeyMetaSQL reads every table in a database. One query covers every
// binding, which matters on materializations with hundreds of them.
const tableKeyMetaSQL = "SELECT name, engine, sorting_key FROM system.tables WHERE database = %s"

// keyMetaRows is the intersection of *database/sql.Rows and the ClickHouse native
// driver's rows, letting one scan loop serve callers holding either. Validate
// works over a *stdsql.DB and the transactor over a native connection.
type keyMetaRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// scanTableKeyMeta collects the engine and sorting key of every table returned,
// keyed by table name.
func scanTableKeyMeta(rows keyMetaRows) (map[string]tableKeyMeta, error) {
	defer rows.Close()

	var out = make(map[string]tableKeyMeta)
	for rows.Next() {
		var name string
		var meta tableKeyMeta
		if err := rows.Scan(&name, &meta.engine, &meta.sortingKey); err != nil {
			return nil, fmt.Errorf("scanning table sorting key: %w", err)
		}
		out[name] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table sorting keys: %w", err)
	}
	return out, nil
}

func readTableKeyMeta(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, database string) (map[string]tableKeyMeta, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(tableKeyMetaSQL, dialect.Literal(database)))
	if err != nil {
		return nil, fmt.Errorf("querying table sorting keys: %w", err)
	}
	return scanTableKeyMeta(rows)
}

var plainIdentifier = regexp.MustCompile("^[a-zA-Z_][0-9a-zA-Z_]*$")

// parseSortingKey splits a system.tables.sorting_key expression list into the
// column names of its plain-column terms, reporting whether every term was one.
//
// A term that is not a plain column reference -- toYear(DateTime),
// cityHash64(id), a nested tuple -- groups rows by something other than the
// columns it mentions, so it is dropped rather than interpreted: a key is only
// safe if its plain terms alone already separate the binding's key fields.
func parseSortingKey(sortingKey string) (plainCols []string, allPlain bool) {
	if strings.TrimSpace(sortingKey) == "" {
		return nil, true
	}

	var cols []string
	var cur strings.Builder
	var depth int
	var quoted bool
	// Split on commas outside both backticks and parentheses: column names can
	// contain either, and a comma inside parentheses belongs to a function call.
	for i := 0; i < len(sortingKey); i++ {
		var c = sortingKey[i]
		switch {
		case c == '`':
			// Two backticks are an escaped backtick within a quoted name, so
			// quoting only toggles on a single one.
			if quoted && i+1 < len(sortingKey) && sortingKey[i+1] == '`' {
				cur.WriteByte(c)
				i++
				continue
			}
			quoted = !quoted
			cur.WriteByte(c)
		case quoted:
			cur.WriteByte(c)
		case c == '(':
			depth++
			cur.WriteByte(c)
		case c == ')':
			depth--
			cur.WriteByte(c)
		case c == ',' && depth == 0:
			cols = append(cols, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	cols = append(cols, cur.String())

	var out = make([]string, 0, len(cols))
	allPlain = true
	for _, col := range cols {
		col = strings.TrimSpace(col)
		if strings.HasPrefix(col, "`") && strings.HasSuffix(col, "`") && len(col) > 1 {
			// Real key columns have names like `Contract Code`, which no
			// unquoted comparison against a Flow field would match.
			col = strings.ReplaceAll(col[1:len(col)-1], "``", "`")
		} else if !plainIdentifier.MatchString(col) {
			allPlain = false
			continue
		}
		out = append(out, col)
	}
	return out, allPlain
}

// sortingKeyContainsAll reports whether the sorting key's plain-column terms
// include every one of the binding's key fields. That is the safety property: as
// long as each key field is a term of its own, two rows with different Flow keys
// differ in at least one term and a merge can never combine them.
//
// Comparison is by set, not sequence: which rows a merge combines depends only on
// which columns the key contains, so a differently ordered key is a storage-layout
// choice and must keep working.
func sortingKeyContainsAll(want []string, got []string) bool {
	for _, w := range want {
		if !slices.Contains(got, w) {
			return false
		}
	}
	return true
}

func sortingKeyMatches(want []string, got []string) bool {
	if len(want) != len(got) {
		return false
	}
	var sortedWant = slices.Clone(want)
	var sortedGot = slices.Clone(got)
	slices.Sort(sortedWant)
	slices.Sort(sortedGot)
	return slices.Equal(sortedWant, sortedGot)
}

// checkSortingKey classifies a target table's sorting key against the key fields
// of the binding materializing into it. It returns an error when writing to the
// table would destroy rows, and a warning message when the key is merely wider
// than the binding's keys. Both are empty for a table that does not exist yet
// (Apply creates it correctly), for a binding with no key fields to compare, and
// for an engine that never combines rows.
//
// The distinction is whether every one of the binding's key fields appears as a
// term of the sorting key. If one does not -- because the key omits it, or names it
// only inside an expression that groups rows more coarsely, as toYear(DateTime)
// does -- then rows with different Flow keys can share a sorting key and a merge
// destroys them. If they all do, nothing can be destroyed, and extra terms only
// stop the engine combining rows that ARE duplicates: the table accumulates
// superseded versions of a key and reads without FINAL return more than one row
// per key. Tables in that state arise when a collection is re-keyed and the table
// keeps a column from its older sorting key, and they work, so that warns.
func checkSortingKey(identifier string, want []string, meta tableKeyMeta, exists bool) (warning string, err error) {
	if !exists || len(want) == 0 || !meta.mergesRows() {
		return "", nil
	}

	got, allPlain := parseSortingKey(meta.sortingKey)
	if allPlain && sortingKeyMatches(want, got) {
		return "", nil
	}

	var describeKey = "(empty -- ORDER BY tuple())"
	if meta.sortingKey != "" {
		describeKey = meta.sortingKey
	}

	if sortingKeyContainsAll(want, got) {
		return fmt.Sprintf(
			"table %s has a sorting key wider than this binding's key fields (ORDER BY %s vs key "+
				"fields %s), so its %s engine will not reduce all versions of a key. The table will "+
				"accumulate superseded rows, and reads that do not use FINAL will return more than one "+
				"row per key. No data is lost. ClickHouse cannot ALTER a sorting key, so correcting this "+
				"requires dropping or renaming the table and backfilling this binding",
			identifier, describeKey, strings.Join(want, ", "), meta.engine), nil
	}

	return "", fmt.Errorf(
		"table %s cannot be materialized into: its sorting key does not match this binding's key "+
			"fields.\n\n"+
			"  binding key fields : %s\n"+
			"  table ORDER BY     : %s\n"+
			"  table engine       : %s\n\n"+
			"The connector materializes standard-updates tables as ReplacingMergeTree, which "+
			"deduplicates rows by the table's sorting key. It also stages each transaction in a table "+
			"created AS this table, inheriting its sorting key. Because the sorting key does not cover "+
			"this binding's key fields, ClickHouse would collapse rows that are NOT duplicates -- "+
			"destroying data both while staging and during background merges of the target table. This "+
			"connector refuses to write to such a table.\n\n"+
			"ClickHouse cannot ALTER a table's sorting key, so to resolve this either:\n"+
			"  * backfill this binding (increment its backfill counter) after dropping or renaming the "+
			"table, so the connector re-creates it with ORDER BY (%s); or\n"+
			"  * point this binding at a different table name; or\n"+
			"  * re-create the table yourself with a sorting key covering exactly the binding's key "+
			"fields, and re-load its history",
		identifier, strings.Join(want, ", "), describeKey, meta.engine, strings.Join(want, ", "))
}
