package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"
)

// text renders a compact, human- and AI-readable summary of the result set.
func (s summary) text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "  affected tasks:       %d\n", s.TaskCount)
	fmt.Fprintf(&b, "  affected collections: %d\n", s.CollectionCount)
	fmt.Fprintf(&b, "  matched rows:         %d\n", len(s.Rows))
	if len(s.ByTenant) > 0 {
		b.WriteString("  by tenant:\n")
		for _, t := range s.ByTenant {
			fmt.Fprintf(&b, "    %-30s tasks=%d collections=%d\n", t.Tenant, t.Tasks, t.Collections)
		}
	}
	return b.String()
}

// reportData is the full record of a verification run, written to the audit file.
type reportData struct {
	Timestamp  string
	Connector  string
	Images     []string
	Check      resolvedCheck
	SQL        string
	AI         aiVerdict
	AIModel    string
	Overridden bool
	Summary    summary
	Status     string // APPROVED | PENDING | BLOCKED (AI CONCERNS)
	Approver   string
}

// terminalRowSample bounds how many affected-task rows are printed to the
// terminal. The full list always goes to the audit report file.
const terminalRowSample = 20

// renderReport renders the audit report. rowLimit caps the affected-tasks table;
// a value <= 0 emits every row (used for the file, which is the complete record).
func renderReport(r reportData, rowLimit int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Feature-impact verification report\n\n")
	fmt.Fprintf(&b, "- **Status:** %s\n", r.Status)
	fmt.Fprintf(&b, "- **Timestamp:** %s\n", r.Timestamp)
	fmt.Fprintf(&b, "- **Connector:** %s\n", r.Connector)
	fmt.Fprintf(&b, "- **Check:** %s%s\n", r.Check.Name, ternary(r.Check.AdHoc, " (ad-hoc)", ""))
	fmt.Fprintf(&b, "- **Predicate column:** %s\n", r.Check.Column)
	fmt.Fprintf(&b, "- **Human reviewer:** %s\n", orNone(r.Approver))
	fmt.Fprintf(&b, "- **AI reviewer:** %s — verdict **%s**%s\n\n", r.AIModel, orNone(r.AI.Verdict),
		ternary(r.Overridden, " (overridden by --override-ai-review)", ""))

	fmt.Fprintf(&b, "## Intent\n\n%s\n\n", r.Check.Description)

	fmt.Fprintf(&b, "## Connector images matched\n\n")
	for _, img := range r.Images {
		fmt.Fprintf(&b, "- `%s`\n", img)
	}

	fmt.Fprintf(&b, "\n## Rendered SQL\n\n```sql\n%s\n```\n\n", r.SQL)

	fmt.Fprintf(&b, "## AI review (%s)\n\n", r.AIModel)
	if len(r.AI.Notes) > 0 {
		for _, n := range r.AI.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		b.WriteString("\n")
	}
	if r.AI.Raw != "" {
		fmt.Fprintf(&b, "<details><summary>Full review output</summary>\n\n```\n%s\n```\n\n</details>\n\n", r.AI.Raw)
	}

	fmt.Fprintf(&b, "## Result summary\n\n```\n%s```\n\n", r.Summary.text())

	fmt.Fprintf(&b, "## Affected tasks and collections\n\n")
	rows := r.Summary.Rows
	if len(rows) == 0 {
		b.WriteString("_No affected tasks found._\n")
		return b.String()
	}
	shown := rows
	if rowLimit > 0 && len(rows) > rowLimit {
		shown = rows[:rowLimit]
	}
	b.WriteString("| Task | Collection | Image |\n|------|------------|-------|\n")
	for _, row := range shown {
		fmt.Fprintf(&b, "| %s | %s | %s |\n", row.Task, row.Collection, row.Image)
	}
	if len(shown) < len(rows) {
		fmt.Fprintf(&b, "\n_Showing %d of %d rows; see the full list in the audit report file._\n", len(shown), len(rows))
	}
	return b.String()
}

// confirmHuman obtains the second pair of eyes. A non-empty approver name is
// treated as an explicit, non-interactive sign-off. Otherwise, if stdin is a
// terminal, the user is prompted. With neither, the run stays PENDING so a human
// must revisit it before acting on the results.
func confirmHuman(approver string, rowCount int) (status, name string) {
	if approver != "" {
		return "APPROVED", approver
	}
	if !stdinIsTerminal() {
		return "PENDING", ""
	}
	fmt.Printf("\nReviewed the %d affected task(s) above? Approve this verification? [y/N]: ", rowCount)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	if strings.EqualFold(strings.TrimSpace(line), "y") || strings.EqualFold(strings.TrimSpace(line), "yes") {
		fmt.Print("Your name (for the audit record): ")
		nameLine, _ := reader.ReadString('\n')
		if n := strings.TrimSpace(nameLine); n != "" {
			return "APPROVED", n
		}
		return "APPROVED", "(interactive)"
	}
	return "PENDING", ""
}

func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// summarize collapses the raw rows into counts and a per-tenant breakdown,
// where the tenant is the first path segment of the catalog name.
func summarize(rows []affectedRow) summary {
	tasks := map[string]bool{}
	collections := map[string]bool{}
	tenantTasks := map[string]map[string]bool{}
	tenantColls := map[string]map[string]bool{}

	for _, r := range rows {
		tasks[r.Task] = true
		collections[r.Collection] = true
		tt := tenantOf(r.Task)
		if tenantTasks[tt] == nil {
			tenantTasks[tt] = map[string]bool{}
			tenantColls[tt] = map[string]bool{}
		}
		tenantTasks[tt][r.Task] = true
		tenantColls[tt][r.Collection] = true
	}

	var byTenant []tenantCount
	for t := range tenantTasks {
		byTenant = append(byTenant, tenantCount{Tenant: t, Tasks: len(tenantTasks[t]), Collections: len(tenantColls[t])})
	}
	sort.Slice(byTenant, func(i, j int) bool { return byTenant[i].Tenant < byTenant[j].Tenant })

	return summary{
		Rows:            rows,
		TaskCount:       len(tasks),
		CollectionCount: len(collections),
		ByTenant:        byTenant,
	}
}

func tenantOf(catalogName string) string {
	if i := strings.Index(catalogName, "/"); i >= 0 {
		return catalogName[:i]
	}
	return catalogName
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
