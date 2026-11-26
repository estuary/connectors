package connector

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestIntegration runs the connector end to end: `flowctl preview` reads a fixture of
// synthetic documents and runs the connector as a local command, which writes companies, people
// linked to them, and notes to a Twenty on localhost. It needs flowctl, and TWENTY_TEST_URL and
// TWENTY_TEST_API_KEY as for the live contract tests. Records are left in place, tagged with a
// run id.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires flowctl and a local Twenty")
	}
	base, key := os.Getenv("TWENTY_TEST_URL"), os.Getenv("TWENTY_TEST_API_KEY")
	if base == "" || key == "" {
		t.Skip("set TWENTY_TEST_URL and TWENTY_TEST_API_KEY to run the integration test against a local Twenty")
	}
	u, err := url.Parse(base)
	require.NoError(t, err)
	if h := u.Hostname(); h != "localhost" && h != "127.0.0.1" {
		t.Fatalf("the integration test writes test records, so it runs only against a local Twenty, not %q", h)
	}
	flowctl, err := exec.LookPath("flowctl")
	require.NoError(t, err, "flowctl must be on the PATH")

	c := newClient(base, key, 600, 20)
	ensureOwnFields(t, c)
	ctx := context.Background()

	tag := uuid.NewString()[:8]
	id := func(n string) string { return "it-" + tag + "-" + n }
	domain := func(n string) string { return n + "-" + tag + ".example.com" }
	const (
		collection = "acmeCo/tests/twenty-companies"
		peopleColl = "acmeCo/tests/twenty-people"
		notesColl  = "acmeCo/tests/twenty-notes"
	)
	plain := func(properties map[string]any) map[string]any {
		return map[string]any{"key": []string{"/id"}, "schema": map[string]any{"type": "object", "required": []string{"id"}, "properties": properties}}
	}
	nullable := map[string]any{"type": []string{"string", "null"}}

	dir := t.TempDir()
	bin := filepath.Join(dir, "materialize-twenty")
	build, err := exec.Command("go", "build", "-o", bin, "./cmd/connector").CombinedOutput()
	require.NoError(t, err, "building the connector:\n%s", build)
	cfg := map[string]any{"base_url": base, "api_key": key, "advanced": map[string]any{"requests_per_minute": 600}}
	writeJSON(t, filepath.Join(dir, "config.json"), cfg)
	notes := plain(map[string]any{"id": map[string]any{"type": "string"}, "title": nullable, "body": nullable})
	notes["projections"] = map[string]any{"bodyV2": "/body"}
	writeJSON(t, filepath.Join(dir, "flow.yaml"), map[string]any{
		"collections": map[string]any{
			peopleColl: plain(map[string]any{"id": map[string]any{"type": "string"}, "jobTitle": nullable, "company": nullable}),
			notesColl:  notes,
			collection: map[string]any{
				"key": []string{"/id"},
				"schema": map[string]any{
					"type": "object", "required": []string{"id"},
					// As in captured collections, a document with _meta.op "d" deletes its key.
					"if":   map[string]any{"properties": map[string]any{"_meta": map[string]any{"properties": map[string]any{"op": map[string]any{"const": "d"}}}}},
					"then": map[string]any{"reduce": map[string]any{"delete": true, "strategy": "lastWriteWins"}},
					"properties": map[string]any{
						"id": map[string]any{"type": "string"}, "name": map[string]any{"type": []string{"string", "null"}},
						"web": map[string]any{"type": []string{"string", "null"}}, "city": map[string]any{"type": []string{"string", "null"}},
						"_meta": map[string]any{"type": "object", "properties": map[string]any{"op": map[string]any{"type": "string"}}},
					},
				},
				"projections": map[string]any{
					"domainName": "/web", "address/addressCity": "/city",
				},
			}},
		"materializations": map[string]any{"acmeCo/tests/materialize-twenty": map[string]any{
			"endpoint": map[string]any{"local": map[string]any{
				"command": []string{bin}, "protobuf": true, "config": "config.json",
			}},
			// People come first, so a person's company may be written after the person.
			"bindings": []any{map[string]any{
				"resource": map[string]any{"name": peopleColl, "object": "people", "id_field": "externalId",
					"relations": []any{map[string]any{"field": "company", "id_field": "externalId"}}},
				"source": peopleColl,
				"fields": map[string]any{"recommended": false, "include": map[string]any{"jobTitle": map[string]any{}, "company": map[string]any{}}},
			}, map[string]any{
				"resource": map[string]any{"name": collection, "object": "companies", "id_field": "externalId",
					"delete_mode": "flag", "deleted_field": "externalDeleted", "deleted_at_field": "externalDeletedAt"},
				"source": collection,
				"fields": map[string]any{"recommended": false, "include": map[string]any{"name": map[string]any{}, "domainName": map[string]any{}, "address/addressCity": map[string]any{}}},
			}, map[string]any{
				"resource": map[string]any{"name": notesColl, "object": "notes", "id_field": "externalId"},
				"source":   notesColl,
				"fields":   map[string]any{"recommended": false, "include": map[string]any{"title": map[string]any{}, "bodyV2": map[string]any{}}},
			}},
		}},
	})

	doc := func(n string, op string, fields map[string]any) []any {
		d := map[string]any{"id": id(n), "_meta": map[string]any{"op": op}}
		for k, v := range fields {
			d[k] = v
		}
		return []any{collection, d}
	}
	other := func(coll, n string, fields map[string]any) []any {
		fields["id"] = id(n)
		return []any{coll, fields}
	}
	commit := map[string]any{"commit": true}
	writeLines(t, filepath.Join(dir, "fixture.ndjson"),
		other(peopleColl, "p1", map[string]any{"jobTitle": "CTO", "company": id("a")}),
		other(peopleColl, "p2", map[string]any{"jobTitle": "CFO", "company": id("missing")}),
		other(peopleColl, "p3", map[string]any{"jobTitle": "CEO", "company": id("b")}),
		doc("a", "c", map[string]any{"name": "Acme", "web": domain("a"), "city": "Singapore"}),
		doc("b", "c", map[string]any{"name": "Beta", "web": domain("b")}),
		other(notesColl, "n1", map[string]any{"title": "Call", "body": "# Call\n\n- one"}),
		commit,
		other(peopleColl, "p1", map[string]any{"company": id("c")}),
		other(peopleColl, "p3", map[string]any{"company": nil}),
		doc("a", "u", map[string]any{"name": "Acme renamed", "web": domain("a"), "city": nil}),
		doc("b", "d", nil),
		doc("c", "c", map[string]any{"name": "Gamma", "web": domain("a")}), // a already holds this domain
		other(notesColl, "n2", map[string]any{"title": "Empty", "body": nil}),
		commit,
	)

	cmd := exec.Command(flowctl, "preview", "--source", filepath.Join(dir, "flow.yaml"),
		"--fixture", filepath.Join(dir, "fixture.ndjson"), "--timeout", "30s")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	// The log holds no key: the endpoint config stays in config.json.
	require.NoError(t, err, "flowctl preview:\n%s", out)
	require.NotContains(t, string(out), key, "the preview log must not print the API key")
	for _, l := range strings.Split(string(out), "\n") {
		if strings.Contains(l, "stored documents") || strings.Contains(l, "skipped") || strings.Contains(l, "left out") {
			t.Log(l)
		}
	}

	in := func(object, n string) record {
		rows, err := c.find(ctx, object, "externalId", []string{id(n)}, true)
		require.NoError(t, err)
		require.Len(t, rows, 1, "records for %s", id(n))
		return rows[0]
	}
	byID := func(n string) record { return in("companies", n) }
	a, b, gamma := byID("a"), byID("b"), byID("c")

	require.Equal(t, gamma["id"], in("people", "p1")["companyId"], "a link moves to a company the same transaction creates")
	require.Equal(t, "CTO", in("people", "p1")["jobTitle"])
	require.Nil(t, in("people", "p2")["companyId"], "a link to a company that isn't in Twenty is left out")
	require.Contains(t, string(out), "links to records that aren't in Twenty yet wait for them")
	require.Contains(t, string(out), id("p2")+" -> "+id("missing"))
	require.Nil(t, in("people", "p3")["companyId"], "a null company clears the link")
	n1, n2 := in("notes", "n1")["bodyV2"].(map[string]any), in("notes", "n2")["bodyV2"].(map[string]any)
	require.Equal(t, "# Call\n\n- one", n1["markdown"])
	require.NotEmpty(t, n1["tiptap"], "Twenty makes the editor's format from the markdown")
	require.Empty(t, n2["markdown"])

	require.Equal(t, "Acme renamed", a["name"])
	require.Equal(t, domain("a"), a["domainName"].(map[string]any)["primaryLinkUrl"])
	require.Empty(t, a["address"].(map[string]any)["addressCity"], "a null city clears it")
	require.Nil(t, a["deletedAt"])

	require.Equal(t, true, b["externalDeleted"], "a delete flags the record")
	require.NotEmpty(t, b["externalDeletedAt"])
	require.Nil(t, b["deletedAt"], "a delete never deletes the record in Twenty")

	require.Equal(t, "Gamma", gamma["name"])
	require.Empty(t, gamma["domainName"].(map[string]any)["primaryLinkUrl"], "a domain another record holds is left off")
	require.True(t, strings.Contains(string(out), "stored documents"), "the connector logs its counts")
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, b, 0o600))
}

func writeLines(t *testing.T, path string, lines ...any) {
	t.Helper()
	var sb strings.Builder
	for _, l := range lines {
		b, err := json.Marshal(l)
		require.NoError(t, err)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(path, []byte(sb.String()), 0o600))
}
