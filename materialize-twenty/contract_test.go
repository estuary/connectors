package connector

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The contract tests check the Twenty behavior the connector relies on, through the connector's
// own client. They always run against the fake. With TWENTY_TEST_URL and TWENTY_TEST_API_KEY set,
// they also run against that Twenty, which must be on localhost, or Twenty Cloud with
// TWENTY_TEST_CLOUD=1 (use a test workspace only): they write test records and, if missing,
// create the connector's own fields on companies, people and notes, and connectorTypeProbes, an
// object with a field of each type. A test that passes on the fake and fails live means the fake
// no longer models Twenty. Records are left in place, tagged with a run id.

// contractTwenty is what a contract test needs from either Twenty.
type contractTwenty struct {
	c          *client
	softDelete func(t *testing.T, object, id string)
	dropField  func(t *testing.T, object, name string) // removes a field a test created
	live       bool
	tag        string
}

func TestContractFake(t *testing.T) {
	runContract(t, func(t *testing.T) contractTwenty {
		f, c := newFakeTwenty(t)
		f.version = "v2.52.0" // the fake follows the probes of v2.52
		f.seed("workspaceMembers", record{"userEmail": "member@example.com", "name": record{"firstName": "Ada", "lastName": "Lovelace"}})
		return contractTwenty{c: c, softDelete: func(_ *testing.T, object, id string) { f.softDelete(object, id) },
			dropField: func(_ *testing.T, object, name string) {
				f.mu.Lock()
				defer f.mu.Unlock()
				delete(f.objects[object].fields, name)
			}}
	})
}

func TestContractLive(t *testing.T) {
	base, key := os.Getenv("TWENTY_TEST_URL"), os.Getenv("TWENTY_TEST_API_KEY")
	if base == "" || key == "" {
		t.Skip("set TWENTY_TEST_URL and TWENTY_TEST_API_KEY to run the contract tests against a local Twenty")
	}
	u, err := url.Parse(base)
	require.NoError(t, err)
	rpm, burst := 600.0, 20
	switch h := u.Hostname(); {
	case h == "localhost" || h == "127.0.0.1":
	case h == "api.twenty.com" && os.Getenv("TWENTY_TEST_CLOUD") == "1":
		// Twenty Cloud allows about 100 requests a minute per workspace.
		rpm, burst = 40, 5
	default:
		t.Fatalf("the live contract tests write test records and fields, so they run only against a local Twenty or, with TWENTY_TEST_CLOUD=1, a Twenty Cloud test workspace, not %q", h)
	}
	c := newClient(base, key, rpm, burst)
	ensureOwnFields(t, c)
	ensureTypeProbes(t, c)
	runContract(t, func(t *testing.T) contractTwenty {
		return contractTwenty{c: c, live: true, softDelete: func(t *testing.T, object, id string) {
			require.NoError(t, c.request(context.Background(), http.MethodDelete, "/rest/"+object+"/"+id+"?soft_delete=true", nil, nil))
		}, dropField: func(t *testing.T, object, name string) {
			// Twenty deletes only an inactive field.
			ctx := context.Background()
			objects, err := c.objects(ctx)
			require.NoError(t, err)
			obj, err := findObject(objects, object)
			require.NoError(t, err)
			if f, ok := obj.field(name); ok {
				require.NoError(t, c.request(ctx, http.MethodPatch, "/rest/metadata/fields/"+f.ID, map[string]any{"isActive": false}, nil))
				require.NoError(t, c.request(ctx, http.MethodDelete, "/rest/metadata/fields/"+f.ID, nil, nil))
			}
		}}
	})
}

// ensureOwnFields creates the fields the fake gives companies and people, and the ID field of
// notes, where a live Twenty lacks them.
func ensureOwnFields(t *testing.T, c *client) {
	ctx := context.Background()
	objects, err := c.objects(ctx)
	require.NoError(t, err)
	want := []struct {
		name, typ string
		unique    bool
	}{{"externalId", "TEXT", true}, {"externalDeleted", "BOOLEAN", false}, {"externalDeletedAt", "DATE_TIME", false}, {"externalAccountKey", "TEXT", false}}
	for _, objName := range []string{"companies", "people", "notes", "noteTargets"} {
		obj, err := findObject(objects, objName)
		require.NoError(t, err)
		for _, w := range want {
			if f, ok := obj.field(w.name); ok {
				require.Equal(t, w.typ, f.Type, "%s.%s", objName, w.name)
				continue
			}
			if w.name == "externalAccountKey" && objName == "people" || w.name != "externalId" && (objName == "notes" || objName == "noteTargets") {
				continue
			}
			body := map[string]any{"objectMetadataId": obj.ID, "type": w.typ, "name": w.name, "label": w.name}
			if w.unique {
				body["isUnique"] = true
			}
			require.NoError(t, c.request(ctx, http.MethodPost, "/rest/metadata/fields", body, nil), "creating %s.%s", objName, w.name)
		}
	}
}

// ensureTypeProbes creates connectorTypeProbes and the fields the fake gives it, where a live
// Twenty lacks them, then checks that each field has the fake's type. Twenty makes the system
// fields and "name" itself, and probeChildren as the other side of pParent. It has no NUMERIC
// field: Twenty v2.52 doesn't create them.
func ensureTypeProbes(t *testing.T, c *client) {
	ctx := context.Background()
	objects, err := c.objects(ctx)
	require.NoError(t, err)
	if _, err := findObject(objects, "connectorTypeProbes"); err != nil {
		require.NoError(t, c.request(ctx, http.MethodPost, "/rest/metadata/objects", map[string]any{
			"nameSingular": "connectorTypeProbe", "namePlural": "connectorTypeProbes",
			"labelSingular": "Connector Type Probe", "labelPlural": "Connector Type Probes", "icon": "IconFlask"}, nil))
		objects, err = c.objects(ctx)
		require.NoError(t, err)
	}
	probes, err := findObject(objects, "connectorTypeProbes")
	require.NoError(t, err)
	relation := func(target, label string) map[string]any {
		obj, err := findObject(objects, target)
		require.NoError(t, err)
		return map[string]any{"type": "MANY_TO_ONE", "targetObjectMetadataId": obj.ID, "targetFieldLabel": label, "targetFieldIcon": "IconFlask"}
	}
	fields := fakeObjects()["connectorTypeProbes"].fields
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		fd := fields[name]
		if _, ok := probeField(probes, name); ok || fd.system || fd.many || name == "name" {
			continue
		}
		body := map[string]any{"objectMetadataId": probes.ID, "type": fd.typ, "name": name, "label": name}
		if fd.unique {
			body["isUnique"] = true
		}
		if fd.settings != nil {
			body["settings"] = fd.settings
		}
		var options []map[string]any
		for i, v := range fd.options {
			options = append(options, map[string]any{"value": v, "label": v, "color": []string{"green", "red"}[i%2], "position": i})
		}
		if options != nil {
			body["options"] = options
		}
		switch {
		case name == "pParent":
			body["relationCreationPayload"] = relation(fd.target, "Probe children")
		case fd.typ == "RELATION":
			body["relationCreationPayload"] = relation(fd.target, "Connector type probes")
		case fd.typ == "MORPH_RELATION":
			body["morphRelationsCreationPayload"] = []any{relation("companies", "Probe morphs"), relation("people", "Probe morphs")}
		}
		require.NoError(t, c.request(ctx, http.MethodPost, "/rest/metadata/fields", body, nil), "creating connectorTypeProbes.%s", name)
	}

	// Read the fields again, since creating a relation also makes its other side.
	objects, err = c.objects(ctx)
	require.NoError(t, err)
	probes, err = findObject(objects, "connectorTypeProbes")
	require.NoError(t, err)
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		f, ok := probeField(probes, name)
		require.True(t, ok, "Twenty has no connectorTypeProbes.%s", name)
		require.Equal(t, fields[name].typ, f.Type, "connectorTypeProbes.%s", name)
	}
}

// twentyAtLeast says whether the Twenty under test runs version v or later. One that doesn't say, like
// Twenty Cloud, is taken to run the latest.
func twentyAtLeast(t *testing.T, c *client, v [3]int) bool {
	got, ok := parseVersion(c.appVersion(t.Context()))
	return !ok || got.atLeast(v)
}

// editorFormat names the subfield in which Twenty keeps rich text in its editor's format, which it
// makes from the markdown: tiptap from v2.51, blocknote before.
func editorFormat(t *testing.T, c *client) (made, other string) {
	if twentyAtLeast(t, c, [3]int{2, 51, 0}) {
		return "tiptap", "blocknote"
	}
	return "blocknote", "tiptap"
}

// probeField finds a field of connectorTypeProbes. Twenty before v2.52 lists the relation pMorph only
// as its field per object, like pMorphCompany.
func probeField(probes *objectMeta, name string) (fieldMeta, bool) {
	if f, ok := probes.field(name); ok || name != "pMorph" {
		return f, ok
	}
	return probes.field(name + "Company")
}

// hasGenericMorph says whether Twenty lists a relation that can point at more than one object as a field
// of its own besides the one per object, as v2.52 does. v2.30 lists only the field per object.
func hasGenericMorph(t *testing.T, c *client) bool {
	objects, err := c.objects(context.Background())
	require.NoError(t, err)
	obj, err := findObject(objects, "noteTargets")
	require.NoError(t, err)
	_, ok := obj.field("target")
	return ok
}

// number reads a number that Twenty may answer as a JSON number or a string.
func number(t *testing.T, v any) float64 {
	t.Helper()
	switch v := v.(type) {
	case float64:
		return v
	case string:
		f, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		return f
	}
	t.Fatalf("not a number: %#v", v)
	return 0
}

func runContract(t *testing.T, open func(t *testing.T) contractTwenty) {
	ctx := context.Background()
	setup := func(t *testing.T) (contractTwenty, func(string) string, func(string) string) {
		tw := open(t)
		tw.tag = uuid.NewString()[:8]
		key := func(n string) string { return "ct-" + tw.tag + "-" + n }
		domain := func(n string) string { return n + "-" + tw.tag + ".example.com" }
		return tw, key, domain
	}
	read := func(t *testing.T, c *client, object, id string) record {
		rows, err := c.findWhere(ctx, object, "and(id[eq]:\""+id+"\","+anyDeleted+")")
		require.NoError(t, err)
		require.Len(t, rows, 1)
		return rows[0]
	}
	create := func(t *testing.T, c *client, object string, rec record) record {
		rows, err := c.create(ctx, object, []record{rec})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		return rows[0]
	}
	rejected := func(t *testing.T, err error) *rejectedError {
		var rej *rejectedError
		require.True(t, errors.As(err, &rej), "want a rejection, got %v", err)
		return rej
	}

	t.Run("plain create refuses a value another record holds, even soft-deleted", func(t *testing.T) {
		tw, key, domain := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("a")}})
		tw.softDelete(t, "companies", a["id"].(string))
		for _, rec := range []record{
			{"name": "Contract b", "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("a")}},
			{"name": "Contract b", "externalId": key("a")},
		} {
			_, err := tw.c.create(ctx, "companies", []record{rec})
			require.Contains(t, strings.Join(rejected(t, err).messages, " "), "duplicate entry")
		}
		rows, err := tw.c.find(ctx, "companies", "externalId", []string{key("b")}, true)
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("records may share an empty unique value, so a swap can clear both before writing them", func(t *testing.T) {
		tw, key, domain := setup(t)
		a := create(t, tw.c, "companies", record{"name": "a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("x")}})
		b := create(t, tw.c, "companies", record{"name": "b", "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("y")}})
		empty := record{"primaryLinkUrl": "", "primaryLinkLabel": nil, "secondaryLinks": nil}
		_, err := tw.c.upsert(ctx, "companies", []record{
			{"id": a["id"], "externalId": key("a"), "domainName": empty},
			{"id": b["id"], "externalId": key("b"), "domainName": empty},
		})
		require.NoError(t, err)
		_, err = tw.c.upsert(ctx, "companies", []record{
			{"id": a["id"], "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("y")}},
			{"id": b["id"], "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("x")}},
		})
		require.NoError(t, err)
		require.Contains(t, read(t, tw.c, "companies", a["id"].(string))["domainName"].(map[string]any)["primaryLinkUrl"], "y")
		require.Contains(t, read(t, tw.c, "companies", b["id"].(string))["domainName"].(map[string]any)["primaryLinkUrl"], "x")

		p := create(t, tw.c, "people", record{"externalId": key("p"), "emails": record{"primaryEmail": key("p") + "@example.com"}})
		q := create(t, tw.c, "people", record{"externalId": key("q"), "emails": record{"primaryEmail": key("q") + "@example.com"}})
		_, err = tw.c.upsert(ctx, "people", []record{
			{"id": p["id"], "externalId": key("p"), "emails": record{"primaryEmail": ""}},
			{"id": q["id"], "externalId": key("q"), "emails": record{"primaryEmail": ""}},
		})
		require.NoError(t, err)
	})

	t.Run("an empty link URL loses the label and promotes a secondary link, and a stand-in keeps both", func(t *testing.T) {
		tw, key, domain := setup(t)
		blog := []any{map[string]any{"url": "https://blog-" + tw.tag + ".example.com", "label": "Blog"}}
		link := func(id string) map[string]any {
			return read(t, tw.c, "companies", id)["domainName"].(map[string]any)
		}
		a := create(t, tw.c, "companies", record{"name": "a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("x"), "primaryLinkLabel": "A"}})
		b := create(t, tw.c, "companies", record{"name": "b", "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("y"), "primaryLinkLabel": "B", "secondaryLinks": blog}})
		_, err := tw.c.upsert(ctx, "companies", []record{
			{"id": a["id"], "externalId": key("a"), "domainName": record{"primaryLinkUrl": "", "primaryLinkLabel": "A"}},
			{"id": b["id"], "externalId": key("b"), "domainName": record{"primaryLinkUrl": "", "primaryLinkLabel": "B", "secondaryLinks": blog}},
		})
		require.NoError(t, err)
		require.Equal(t, "", link(a["id"].(string))["primaryLinkLabel"])
		got := link(b["id"].(string))
		require.Contains(t, got["primaryLinkUrl"], "blog-"+tw.tag)
		require.Equal(t, "Blog", got["primaryLinkLabel"])

		c := create(t, tw.c, "companies", record{"name": "c", "externalId": key("c"), "domainName": record{"primaryLinkUrl": domain("z"), "primaryLinkLabel": "C", "secondaryLinks": blog}})
		stand := c["id"].(string) + ".invalid"
		_, err = tw.c.upsert(ctx, "companies", []record{
			{"id": c["id"], "externalId": key("c"), "domainName": record{"primaryLinkUrl": stand, "primaryLinkLabel": "C", "secondaryLinks": blog}},
		})
		require.NoError(t, err)
		got = link(c["id"].(string))
		require.Equal(t, stand, got["primaryLinkUrl"])
		require.Equal(t, "C", got["primaryLinkLabel"])
		require.Len(t, got["secondaryLinks"], 1)

		// An empty primary email keeps the additional ones, without promoting any.
		extra := []any{key("p2") + "@example.com"}
		p := create(t, tw.c, "people", record{"externalId": key("p"), "emails": record{"primaryEmail": key("p") + "@example.com", "additionalEmails": extra}})
		_, err = tw.c.upsert(ctx, "people", []record{{"id": p["id"], "externalId": key("p"), "emails": record{"primaryEmail": ""}}})
		require.NoError(t, err)
		emails := read(t, tw.c, "people", p["id"].(string))["emails"].(map[string]any)
		require.Equal(t, "", emails["primaryEmail"])
		require.Equal(t, extra, emails["additionalEmails"])
	})

	t.Run("a batch holding one unique value twice is rejected whole", func(t *testing.T) {
		tw, key, domain := setup(t)
		_, err := tw.c.create(ctx, "companies", []record{
			{"name": "a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("x")}},
			{"name": "b", "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("x")}},
		})
		rejected(t, err)
		rows, err := tw.c.find(ctx, "companies", "externalId", []string{key("a"), key("b")}, true)
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("upsert by id restores a soft-deleted record", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		id := a["id"].(string)
		tw.softDelete(t, "companies", id)
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "externalId": key("a"), "name": "Changed"}})
		require.NoError(t, err)
		row := read(t, tw.c, "companies", id)
		require.Nil(t, row["deletedAt"])
		require.Equal(t, "Changed", row["name"])
	})

	t.Run("omitted fields are kept, composites merge, null text is empty", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"), "externalAccountKey": "k",
			"address": record{"addressCity": "Singapore", "addressCountry": "SGP"}})
		id := a["id"].(string)
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "address": record{"addressCity": "Jurong"}}})
		require.NoError(t, err)
		row := read(t, tw.c, "companies", id)
		addr := row["address"].(map[string]any)
		require.Equal(t, []any{"Contract a", "k", "Jurong", "SGP"}, []any{row["name"], row["externalAccountKey"], addr["addressCity"], addr["addressCountry"]})
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": id, "externalAccountKey": nil}})
		require.NoError(t, err)
		require.Equal(t, "", read(t, tw.c, "companies", id)["externalAccountKey"])
	})

	t.Run("a null whole composite changes nothing, null subfields clear it", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"),
			"address": record{"addressCity": "Singapore", "addressCountry": "SGP"}})
		id := a["id"].(string)
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "address": nil}})
		require.NoError(t, err)
		require.Equal(t, "Singapore", read(t, tw.c, "companies", id)["address"].(map[string]any)["addressCity"])
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": id, "address": record{"addressCity": nil, "addressCountry": nil}}})
		require.NoError(t, err)
		addr := read(t, tw.c, "companies", id)["address"].(map[string]any)
		require.Empty(t, addr["addressCity"])
		require.Empty(t, addr["addressCountry"])
	})

	t.Run("an update to a link clears the subfields it omits, and one that leaves the link out keeps them", func(t *testing.T) {
		tw, key, domain := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"),
			"domainName": record{"primaryLinkUrl": domain("a"), "primaryLinkLabel": "Main",
				"secondaryLinks": []any{record{"url": domain("c"), "label": ""}}}})
		id := a["id"].(string)
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "name": "Renamed"}})
		require.NoError(t, err)
		link := read(t, tw.c, "companies", id)["domainName"].(map[string]any)
		require.Equal(t, "Main", link["primaryLinkLabel"])
		require.Len(t, link["secondaryLinks"], 1)
		// So the connector sends back the subfields a document leaves out (keepSubfields).
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": id, "domainName": record{"primaryLinkUrl": domain("b")}}})
		require.NoError(t, err)
		link = read(t, tw.c, "companies", id)["domainName"].(map[string]any)
		require.Equal(t, domain("b"), link["primaryLinkUrl"])
		require.Empty(t, link["primaryLinkLabel"])
		require.Empty(t, link["secondaryLinks"])
	})

	t.Run("a relation takes the id of an existing record, in its join column only", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		for _, tc := range []struct {
			rec  record
			want string
		}{
			{record{"companyId": uuid.NewString()}, "violates foreign key constraint"},
			{record{"companyId": ""}, `Invalid UUID value '' for field "companyId"`},
			{record{"company": a["id"]}, `Relation "company" requires`}, // "create, connect, or" from v2.51, "connect or" before
		} {
			tc.rec["externalId"] = key("p")
			_, err := tw.c.create(ctx, "people", []record{tc.rec})
			require.Contains(t, strings.Join(rejected(t, err).messages, " "), tc.want)
		}
	})

	t.Run("a join column keeps its link when omitted and clears on null; soft-deleted records can be linked", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		b := create(t, tw.c, "companies", record{"name": "Contract b", "externalId": key("b")})
		p := create(t, tw.c, "people", record{"externalId": key("p"), "companyId": a["id"]})
		id := p["id"].(string)
		require.Equal(t, a["id"], read(t, tw.c, "people", id)["companyId"])
		for _, step := range []struct {
			rec  record
			want any
		}{
			{record{"jobTitle": "Kept"}, a["id"]},
			{record{"companyId": b["id"]}, b["id"]},
			{record{"companyId": nil}, nil},
		} {
			step.rec["id"] = id
			_, err := tw.c.upsert(ctx, "people", []record{step.rec})
			require.NoError(t, err)
			require.Equal(t, step.want, read(t, tw.c, "people", id)["companyId"], "after %v", step.rec)
		}
		tw.softDelete(t, "companies", a["id"].(string))
		_, err := tw.c.upsert(ctx, "people", []record{{"id": id, "companyId": a["id"]}})
		require.NoError(t, err)
		require.Equal(t, a["id"], read(t, tw.c, "people", id)["companyId"])
		// Soft-deleting a linked record keeps the link.
		_, err = tw.c.upsert(ctx, "people", []record{{"id": id, "companyId": b["id"]}})
		require.NoError(t, err)
		tw.softDelete(t, "companies", b["id"].(string))
		require.Equal(t, b["id"], read(t, tw.c, "people", id)["companyId"])
	})

	t.Run("the metadata says which relations point at one record, and the GraphQL metadata at which object", func(t *testing.T) {
		tw, _, _ := setup(t)
		objects, err := tw.c.objects(ctx)
		require.NoError(t, err)
		field := func(object, name string) fieldMeta {
			obj, err := findObject(objects, object)
			require.NoError(t, err)
			f, ok := obj.field(name)
			require.True(t, ok, "%s.%s", object, name)
			return f
		}
		// Twenty lists a relation that can point at more than one object as a field of its own from v2.51.
		require.Equal(t, twentyAtLeast(t, tw.c, [3]int{2, 51, 0}), hasGenericMorph(t, tw.c))
		company := field("people", "company")
		require.Equal(t, []string{"RELATION", "MANY_TO_ONE", "companyId"},
			[]string{company.Type, company.Settings.RelationType, company.Settings.JoinColumnName})
		// Twenty v2.52 says "OPEN" or "SYSTEM"; Twenty Cloud (Nov 2026) leaves writability out.
		require.Contains(t, []string{"OPEN", ""}, company.Writability)
		require.Equal(t, "ONE_TO_MANY", field("companies", "people").Settings.RelationType)
		ok, _ := writable(field("connectorTypeProbes", "searchVector"))
		require.False(t, ok, "Twenty manages searchVector")
		for _, tc := range []struct{ object, field, want string }{
			{"people", "company", "companies"},
			{"companies", "people", "people"},
			{"connectorTypeProbes", "pCompany", "companies"},
			{"connectorTypeProbes", "pParent", "connectorTypeProbes"},
		} {
			got, err := tw.c.relationTarget(ctx, field(tc.object, tc.field).ID)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "%s.%s", tc.object, tc.field)
		}
		noTarget := []fieldMeta{field("people", "jobTitle")}
		if hasGenericMorph(t, tw.c) {
			noTarget = append(noTarget, field("connectorTypeProbes", "pMorph"))
		}
		for _, f := range noTarget {
			_, err := tw.c.relationTarget(ctx, f.ID)
			require.ErrorContains(t, err, "didn't say which object", f.Name)
		}
		_, err = tw.c.relationTarget(ctx, uuid.NewString())
		require.ErrorContains(t, err, "Twenty's metadata API answered: ")
	})

	t.Run("a company's account owner links to a workspace member by email in any case, or by name", func(t *testing.T) {
		tw, key, _ := setup(t)
		members, err := tw.c.findWhere(ctx, "workspaceMembers", "deletedAt[is]:NULL")
		require.NoError(t, err)
		require.NotEmpty(t, members, "the workspace has a member")
		email, _ := members[0]["userEmail"].(string)
		require.NotEmpty(t, email)
		h := harnessFor(t, tw.c, testBinding{
			resource: `{"name":"acmeCo/companies","object":"companies","id_field":"externalId",` +
				`"relations":[{"field":"accountOwner","id_field":"userEmail","source_field":"owner"}]}`,
			collection: collection("acmeCo/companies", "string", proj("name", "/name", "string"), proj("owner", "/owner", "null", "string")),
		})
		doc := func(owner string) storeItem {
			raw, err := json.Marshal(map[string]any{"id": key("a"), "name": "Owned", "owner": owner})
			require.NoError(t, err)
			return h.doc(0, string(raw))
		}
		require.NoError(t, h.store(doc(email)))
		rows, err := tw.c.find(ctx, "companies", "externalId", []string{key("a")}, true)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, members[0]["id"], rows[0]["accountOwnerId"])
		other := strings.ToUpper(email)
		if other == email {
			other = strings.ToLower(email)
		}
		require.NotEqual(t, email, other)
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": rows[0]["id"], "accountOwnerId": nil}})
		require.NoError(t, err)
		require.NoError(t, h.store(doc(other)))
		require.Equal(t, stats{received: 1, updated: 1}, h.stats("companies"))
		rows, err = tw.c.find(ctx, "companies", "externalId", []string{key("a")}, true)
		require.NoError(t, err)
		require.Equal(t, members[0]["id"], rows[0]["accountOwnerId"])

		// By name: "First Last", case and spaces ignored. A name two members share is left out.
		name, _ := members[0]["name"].(map[string]any)
		first, _ := name["firstName"].(string)
		last, _ := name["lastName"].(string)
		full := strings.TrimSpace(first + " " + last)
		if full == "" {
			t.Skip("the workspace member has no name")
		}
		shared := 0
		for _, m := range members {
			n, _ := m["name"].(map[string]any)
			f, _ := n["firstName"].(string)
			l, _ := n["lastName"].(string)
			if strings.EqualFold(strings.Join(strings.Fields(f+" "+l), " "), strings.Join(strings.Fields(full), " ")) {
				shared++
			}
		}
		byName := harnessFor(t, tw.c, testBinding{
			resource: `{"name":"acmeCo/companies","object":"companies","id_field":"externalId",` +
				`"relations":[{"field":"accountOwner","id_field":"name","source_field":"owner"}]}`,
			collection: collection("acmeCo/companies", "string", proj("name", "/name", "string"), proj("owner", "/owner", "null", "string")),
		})
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": rows[0]["id"], "accountOwnerId": nil}})
		require.NoError(t, err)
		raw, err := json.Marshal(map[string]any{"id": key("a"), "name": "Owned", "owner": "  " + strings.ToUpper(first) + "   " + last + " "})
		require.NoError(t, err)
		require.NoError(t, byName.store(byName.doc(0, string(raw))))
		rows, err = tw.c.find(ctx, "companies", "externalId", []string{key("a")}, true)
		require.NoError(t, err)
		if shared == 1 {
			require.Equal(t, stats{received: 1, updated: 1}, byName.stats("companies"))
			require.Equal(t, members[0]["id"], rows[0]["accountOwnerId"])
		} else {
			require.Equal(t, stats{received: 1, updated: 1, skipped: map[string]int{"accountOwner_link_ambiguous": 1}}, byName.stats("companies"))
			require.Nil(t, rows[0]["accountOwnerId"])
		}
	})

	t.Run("a note links to a record through its target field, once per note and record, and a link without a target can repeat", func(t *testing.T) {
		tw, key, _ := setup(t)
		note := create(t, tw.c, "notes", record{"title": "Contract note", "externalId": key("n")})
		acme := create(t, tw.c, "companies", record{"name": "Contract acme", "externalId": key("c")})
		objects, err := tw.c.objects(ctx)
		require.NoError(t, err)
		obj, err := findObject(objects, "noteTargets")
		require.NoError(t, err)
		target, ok := obj.field("targetCompany")
		require.True(t, ok)
		got, err := tw.c.morphTarget(ctx, target)
		require.NoError(t, err)
		require.Equal(t, "companies", got)
		require.NotEmpty(t, target.MorphID)
		if generic, ok := obj.field("target"); ok {
			require.Empty(t, generic.Settings.JoinColumnName)
			require.Equal(t, target.MorphID, generic.MorphID)
		}

		h := harnessFor(t, tw.c, noteTargets(`[{"field":"note","id_field":"externalId","source_field":"noteKey"},`+
			`{"field":"targetCompany","id_field":"externalId","source_field":"companyKey"}]`))
		raw, err := json.Marshal(map[string]any{"id": key("l1"), "noteKey": key("n"), "companyKey": key("c")})
		require.NoError(t, err)
		require.NoError(t, h.store(h.doc(0, string(raw))))
		rows, err := tw.c.find(ctx, "noteTargets", "externalId", []string{key("l1")}, true)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, note["id"], rows[0]["noteId"])
		require.Equal(t, acme["id"], rows[0]["targetCompanyId"])

		_, err = tw.c.create(ctx, "noteTargets", []record{{"externalId": key("l2"), "noteId": note["id"], "targetCompanyId": acme["id"]}})
		if twentyAtLeast(t, tw.c, [3]int{2, 38, 0}) {
			require.Contains(t, strings.Join(rejected(t, err).messages, " "), "A duplicate entry was detected")
		} else {
			require.NoError(t, err, "Twenty before v2.38 takes a note's link to one record twice")
		}
		create(t, tw.c, "noteTargets", record{"externalId": key("l3"), "noteId": note["id"]})
		create(t, tw.c, "noteTargets", record{"externalId": key("l4"), "noteId": note["id"]})

		// Deleting the company for good deletes its note targets too.
		_, err = tw.c.remove(ctx, "companies", []string{acme["id"].(string)}, "", false)
		require.NoError(t, err)
		rows, err = tw.c.find(ctx, "noteTargets", "externalId", []string{key("l1")}, true)
		require.NoError(t, err)
		require.Empty(t, rows)
	})

	t.Run("the connector creates a missing field it lists, which takes records at once, and Twenty refuses the reserved names", func(t *testing.T) {
		tw, key, _ := setup(t)
		count, whenSigned := "zzCount"+tw.tag, "zzSigned"+tw.tag
		t.Cleanup(func() {
			tw.dropField(t, "connectorTypeProbes", count)
			tw.dropField(t, "connectorTypeProbes", whenSigned)
		})
		tb := testBinding{
			resource: `{"name":"acmeCo/probes","object":"connectorTypeProbes","id_field":"externalId","create_fields":["` + count + `","` + whenSigned + `"]}`,
			collection: collection("acmeCo/probes", "string", proj("name", "/name", "string"), proj(count, "/count", "integer"),
				dateTime(proj(whenSigned, "/signed", "null", "string"))),
		}
		spec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(tb.resource),
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{count, "name", whenSigned}, Document: "flow_document"},
		}}}
		objects, err := tw.c.objects(ctx)
		require.NoError(t, err)
		created, err := createFields(ctx, tw.c, objects, spec)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"connectorTypeProbes." + count + " (NUMBER)", "connectorTypeProbes." + whenSigned + " (DATE_TIME)"}, created)
		// Again: nothing to create.
		objects, err = tw.c.objects(ctx)
		require.NoError(t, err)
		created, err = createFields(ctx, tw.c, objects, spec)
		require.NoError(t, err)
		require.Empty(t, created)

		h := harnessFor(t, tw.c, tb)
		raw, err := json.Marshal(map[string]any{"id": key("p"), "name": "Created", "count": 42, "signed": "2026-11-03T10:00:00Z"})
		require.NoError(t, err)
		require.NoError(t, h.store(h.doc(0, string(raw))))
		rows, err := tw.c.find(ctx, "connectorTypeProbes", "externalId", []string{key("p")}, true)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.EqualValues(t, 42, rows[0][count])
		require.Equal(t, "2026-11-03T10:00:00.000Z", rows[0][whenSigned])

		if tw.live {
			// The connector's list of reserved names matches Twenty's.
			obj, err := findObject(objects, "connectorTypeProbes")
			require.NoError(t, err)
			for _, name := range []string{"type", "user", "links", "search", "relations", "index"} {
				require.Contains(t, reservedFieldNames, name)
				err := tw.c.createField(ctx, obj.ID, newField{name: name, typ: "TEXT"})
				require.Error(t, err, name)
				tw.dropField(t, "connectorTypeProbes", name) // in case Twenty took it
			}
		}
	})

	t.Run("the connector creates a binding's ID field as unique, and its delete fields", func(t *testing.T) {
		tw, key, _ := setup(t)
		idf, gone, goneAt := "zzKey"+tw.tag, "zzGone"+tw.tag, "zzGoneAt"+tw.tag
		t.Cleanup(func() {
			for _, name := range []string{idf, gone, goneAt} {
				tw.dropField(t, "connectorTypeProbes", name)
			}
		})
		tb := testBinding{
			resource: `{"name":"acmeCo/probes","object":"connectorTypeProbes","id_field":"` + idf + `","delete_mode":"flag","deleted_field":"` + gone +
				`","deleted_at_field":"` + goneAt + `","create_fields":["` + idf + `","` + gone + `","` + goneAt + `"]}`,
			collection: collection("acmeCo/probes", "string", proj("name", "/name", "string")),
		}
		spec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(tb.resource),
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}, Document: "flow_document"},
		}}}
		objects, err := tw.c.objects(ctx)
		require.NoError(t, err)
		created, err := createFields(ctx, tw.c, objects, spec)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"connectorTypeProbes." + idf + " (TEXT)", "connectorTypeProbes." + gone + " (BOOLEAN)",
			"connectorTypeProbes." + goneAt + " (DATE_TIME)"}, created)
		objects, err = tw.c.objects(ctx)
		require.NoError(t, err)
		obj, err := findObject(objects, "connectorTypeProbes")
		require.NoError(t, err)
		f, ok := obj.field(idf)
		require.True(t, ok)
		require.True(t, f.IsUnique)

		h := harnessFor(t, tw.c, tb)
		require.NoError(t, h.store(h.doc(0, `{"id":"`+key("o")+`","name":"Own"}`)))
		require.NoError(t, h.store(h.deletion(0, key("o"))))
		rows, err := tw.c.find(ctx, "connectorTypeProbes", idf, []string{key("o")}, true)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, true, rows[0][gone])
		require.NotEmpty(t, rows[0][goneAt])
	})

	t.Run("a self-hosted Twenty says its version, which the delete guard reads", func(t *testing.T) {
		tw, _, _ := setup(t)
		if tw.c.baseURL == defaultBaseURL {
			t.Skip("Twenty Cloud runs the latest version and isn't asked")
		}
		v, ok := parseVersion(tw.c.appVersion(ctx))
		require.True(t, ok, "Twenty at %s didn't say its version at /client-config", tw.c.baseURL)
		err := checkDeleteVersion(ctx, tw.c, []string{"acmeCo/companies"})
		if v.atLeast(minDeleteVersion) {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "skips the deletes the connector sends")
		}
	})

	t.Run("a rich text write replaces the whole value, and Twenty makes the editor's format from markdown", func(t *testing.T) {
		tw, key, _ := setup(t)
		n := create(t, tw.c, "notes", record{"externalId": key("n"), "title": "Contract note", "bodyV2": record{"markdown": "# Call\n\n- one"}})
		id := n["id"].(string)
		body := func() map[string]any { return read(t, tw.c, "notes", id)["bodyV2"].(map[string]any) }
		require.Equal(t, "# Call\n\n- one", body()["markdown"])
		made, other := editorFormat(t, tw.c)
		require.Empty(t, body()[other])
		require.NotEmpty(t, body()[made])
		_, err := tw.c.upsert(ctx, "notes", []record{{"id": id, "title": "Renamed"}})
		require.NoError(t, err)
		require.Equal(t, "# Call\n\n- one", body()["markdown"])
		// So the connector doesn't merge the subfields of two documents.
		_, err = tw.c.upsert(ctx, "notes", []record{{"id": id, "bodyV2": record{"blocknote": nil}}})
		require.NoError(t, err)
		require.Empty(t, body()["markdown"])
		_, err = tw.c.upsert(ctx, "notes", []record{{"id": id, "bodyV2": record{"markdown": "again"}}})
		require.NoError(t, err)
		_, err = tw.c.upsert(ctx, "notes", []record{{"id": id, "bodyV2": record{"markdown": nil}}})
		require.NoError(t, err)
		require.Empty(t, body()["markdown"])
		require.Empty(t, body()[made])
		// So the connector clears it with a null markdown.
		_, err = tw.c.upsert(ctx, "notes", []record{{"id": id, "bodyV2": nil}})
		rejected(t, err)
	})

	t.Run("select, rating, multi-select, array, rich text and UUID values must be what Twenty allows", func(t *testing.T) {
		tw, key, _ := setup(t)
		for _, tc := range []struct {
			field string
			value any
			want  string
		}{
			{"pSelect", "OPT_X", `Invalid value "OPT_X" for field "pSelect". Valid values are: OPT_A, OPT_B`},
			{"pSelect", "", `Invalid value "" for field "pSelect"`},
			{"pRating", "3", `Invalid value "3" for field "pRating"`},
			{"pRating", 3, `Invalid string value 3 for text field "pRating"`},
			{"pMultiSelect", []any{"OPT_X"}, `for multi select field "pMultiSelect"`},
			{"pArray", []any{1, 2}, "Array values need to be string"},
			{"pArray", []any{"a", nil}, `Invalid value [ 'a', null ] for field "pArray - Array values need to be string"`},
			{"pMultiSelect", []any{"OPT_A", nil}, `Invalid value [ 'OPT_A', null ] for field "pMultiSelect - Array values need to be string"`},
			{"pRichText", "text", `Invalid object value 'text' for field "pRichText"`},
			{"pRichText", record{"markdown": 5}, `Invalid string value 5 for text field "pRichText.markdown"`},
			{"pRichText", record{"other": "x"}, `Invalid subfield 'other' for rich text field "pRichText"`},
			{"pUuid", "abc", `Invalid UUID value 'abc' for field "pUuid"`},
		} {
			_, err := tw.c.create(ctx, "connectorTypeProbes", []record{{"externalId": key("x"), tc.field: tc.value}})
			require.Contains(t, strings.Join(rejected(t, err).messages, " "), tc.want, "%s = %v", tc.field, tc.value)
		}
		// A string is taken as a list of one, and a null list is empty.
		p := create(t, tw.c, "connectorTypeProbes", record{"externalId": key("a"), "pMultiSelect": "OPT_A", "pArray": "a"})
		id := p["id"].(string)
		require.Equal(t, []any{[]any{"OPT_A"}, []any{"a"}}, []any{read(t, tw.c, "connectorTypeProbes", id)["pMultiSelect"], read(t, tw.c, "connectorTypeProbes", id)["pArray"]})
		_, err := tw.c.upsert(ctx, "connectorTypeProbes", []record{{"id": id, "pMultiSelect": nil, "pArray": nil}})
		require.NoError(t, err)
		row := read(t, tw.c, "connectorTypeProbes", id)
		require.Equal(t, []any{[]any{}, []any{}}, []any{row["pMultiSelect"], row["pArray"]})
	})

	t.Run("each type the connector writes round-trips, and the rest are refused at Validate", func(t *testing.T) {
		tw, key, _ := setup(t)
		co := create(t, tw.c, "companies", record{"name": "Contract co", "externalId": key("co")})
		stringItems := func(p pf.Projection) pf.Projection {
			p.Inference.Array = &pf.Inference_Array{ItemTypes: []string{"string"}}
			return p
		}
		date := func(p pf.Projection) pf.Projection {
			p.Inference.String_ = &pf.Inference_String{Format: "date"}
			return p
		}
		morph, morphWant := "pMorph", "can point at more than one object"
		if !hasGenericMorph(t, tw.c) {
			morph, morphWant = "pMorphCompany", "is a relation. To link records through it"
		}
		h := harnessFor(t, tw.c, probes(`,"relations":[{"field":"pCompany","id_field":"externalId","source_field":"company"},`+
			`{"field":"pParent","id_field":"externalId","source_field":"parent"}]`,
			proj("pText", "/text", "null", "string"), proj("pNumber", "/number", "null", "number"),
			proj("pBoolean", "/flag", "null", "boolean"), date(proj("pDate", "/date", "null", "string")),
			dateTime(proj("pDateTime", "/at", "null", "string")), proj("pSelect", "/select", "null", "string"),
			stringItems(proj("pMultiSelect", "/multi", "null", "array")), proj("pRating", "/rating", "null", "string"),
			stringItems(proj("pArray", "/tags", "null", "array")), proj("pRawJson", "/raw", "null", "object"),
			proj("pRichText", "/body", "null", "string"), proj("pUuid", "/uuid", "null", "string"),
			proj("pEmails", "/email", "null", "string"), proj("pPhones", "/phones", "null", "object"),
			proj("pLinks", "/site", "null", "string"), proj("pAddress", "/address", "null", "object"),
			proj("pFullName/firstName", "/first", "null", "string"), proj("pFullName/lastName", "/last", "null", "string"),
			proj("pCurrency", "/price", "null", "object"), proj("company", "/company", "null", "string"),
			proj("parent", "/parent", "null", "string"),
			proj("pFiles", "/files", "array"), proj(morph, "/morph", "string"), proj("probeChildren", "/children", "array"),
			proj("position", "/position", "number"), proj("createdBy", "/createdBy", "object"), proj("searchVector", "/sv", "string"),
		))
		for field, want := range map[string]string{
			"pFiles": "holds files uploaded to Twenty", morph: morphWant,
			"probeChildren": "lists the records that point at this one", "position": "Twenty manages",
			"createdBy": "Twenty manages", "searchVector": "Twenty manages",
		} {
			c := h.constraints[0][field][0]
			require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, c.Type, field)
			require.Contains(t, c.Reason, want, field)
		}
		doc := func(fields map[string]any) storeItem {
			raw, err := json.Marshal(fields)
			require.NoError(t, err)
			return h.doc(0, string(raw))
		}
		get := func(k string) record {
			rows, err := tw.c.find(ctx, "connectorTypeProbes", "externalId", []string{k}, true)
			require.NoError(t, err)
			require.Len(t, rows, 1, k)
			return rows[0]
		}
		a, b, u := key("a"), key("b"), uuid.NewString()

		// The parent comes after its child, so its link is written later.
		require.NoError(t, h.store(doc(map[string]any{"id": a, "text": "hello", "number": 1.5, "flag": true, "date": "2026-10-29",
			"at": "2026-10-29T09:02:03+08:00", "select": "OPT_A", "multi": []any{"OPT_A", "OPT_B"}, "rating": "RATING_3",
			"tags": []any{"a", "b"}, "raw": map[string]any{"k": []any{1, "x"}}, "body": "**bold**", "uuid": u,
			"email": "Ada@Example.com", "phones": map[string]any{"primaryPhoneNumber": "4155550123", "primaryPhoneCountryCode": "US", "primaryPhoneCallingCode": "+1"},
			"site": "https://example.com/docs", "address": map[string]any{"addressCity": "Lisbon", "addressCountry": "Portugal"},
			"first": "Ada", "last": "Lovelace", "price": map[string]any{"amountMicros": 1500000, "currencyCode": "EUR"},
			"company": key("co"), "parent": b}), doc(map[string]any{"id": b, "text": "parent"})))
		require.Equal(t, stats{received: 2, created: 2, linkedLater: 1}, h.stats("connectorTypeProbes"))
		row := get(a)
		require.Equal(t, []any{"hello", 1.5, true, "2026-10-29", "2026-10-29T01:02:03.000Z", "OPT_A", []any{"OPT_A", "OPT_B"}, "RATING_3",
			[]any{"a", "b"}, map[string]any{"k": []any{1.0, "x"}}, u},
			[]any{row["pText"], number(t, row["pNumber"]), row["pBoolean"], row["pDate"], row["pDateTime"], row["pSelect"], row["pMultiSelect"],
				row["pRating"], row["pArray"], row["pRawJson"], row["pUuid"]})
		require.Equal(t, []any{"**bold**", "ada@example.com", "4155550123", "+1", "https://example.com/docs", "Lisbon", "Portugal", "Ada", "Lovelace", "EUR"},
			[]any{fakeGet(row, "pRichText.markdown"), fakeGet(row, "pEmails.primaryEmail"), fakeGet(row, "pPhones.primaryPhoneNumber"),
				fakeGet(row, "pPhones.primaryPhoneCallingCode"), fakeGet(row, "pLinks.primaryLinkUrl"), fakeGet(row, "pAddress.addressCity"),
				fakeGet(row, "pAddress.addressCountry"), fakeGet(row, "pFullName.firstName"), fakeGet(row, "pFullName.lastName"),
				fakeGet(row, "pCurrency.currencyCode")})
		made, _ := editorFormat(t, tw.c)
		require.NotEmpty(t, fakeGet(row, "pRichText."+made))
		require.Equal(t, 1500000.0, number(t, fakeGet(row, "pCurrency.amountMicros")))
		require.Equal(t, []any{co["id"], get(b)["id"]}, []any{row["pCompanyId"], row["pParentId"]})

		// A field the document leaves out keeps its value, and a composite merges its subfields.
		require.NoError(t, h.store(doc(map[string]any{"id": a, "text": "changed", "first": "Grace",
			"price": map[string]any{"currencyCode": "USD"}, "address": map[string]any{"addressCity": "Porto"}})))
		row = get(a)
		require.Equal(t, []any{"changed", 1.5, "**bold**", "Grace", "Lovelace", "USD", "Porto", "Portugal", co["id"], get(b)["id"]},
			[]any{row["pText"], number(t, row["pNumber"]), fakeGet(row, "pRichText.markdown"), fakeGet(row, "pFullName.firstName"),
				fakeGet(row, "pFullName.lastName"), fakeGet(row, "pCurrency.currencyCode"), fakeGet(row, "pAddress.addressCity"),
				fakeGet(row, "pAddress.addressCountry"), row["pCompanyId"], row["pParentId"]})
		require.Equal(t, 1500000.0, number(t, fakeGet(row, "pCurrency.amountMicros")))

		// A null clears every field.
		nulls := map[string]any{"id": a}
		for _, f := range []string{"text", "number", "flag", "date", "at", "select", "multi", "rating", "tags", "raw", "body", "uuid",
			"email", "phones", "site", "address", "first", "last", "price", "company", "parent"} {
			nulls[f] = nil
		}
		require.NoError(t, h.store(doc(nulls)))
		require.Equal(t, stats{received: 1, updated: 1}, h.stats("connectorTypeProbes"))
		row = get(a)
		for _, f := range []string{"pText", "pNumber", "pBoolean", "pDate", "pDateTime", "pSelect", "pMultiSelect", "pRating", "pArray",
			"pRawJson", "pUuid", "pCompanyId", "pParentId", "pRichText.markdown", "pRichText." + made, "pEmails.primaryEmail",
			"pPhones.primaryPhoneNumber", "pLinks.primaryLinkUrl", "pAddress.addressCity", "pAddress.addressCountry",
			"pFullName.firstName", "pFullName.lastName", "pCurrency.amountMicros", "pCurrency.currencyCode"} {
			require.Empty(t, fakeGet(row, f), f)
		}
	})

	t.Run("a currency amount in units is written as micros, rounded half away from zero", func(t *testing.T) {
		tw, key, _ := setup(t)
		h := harnessFor(t, tw.c, testBinding{
			resource: `{"name":"acmeCo/deals","object":"companies","id_field":"externalId"}`,
			collection: collection("acmeCo/deals", "string", proj("name", "/name", "string"),
				proj("annualRevenue/amount", "/amount", "null", "number"), proj("annualRevenue/currencyCode", "/currency", "string")),
		})
		k := key("amount")
		require.NoError(t, h.store(h.doc(0, `{"id":"`+k+`","name":"Amount co","amount":19.9999995,"currency":"EUR"}`)))
		rows, err := tw.c.find(ctx, "companies", "externalId", []string{k}, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, 20000000.0, number(t, fakeGet(rows[0], "annualRevenue.amountMicros")))
		require.Equal(t, "EUR", fakeGet(rows[0], "annualRevenue.currencyCode"))
	})

	t.Run("adopt_by takes a live record with an empty ID field, whether it was left out or empty", func(t *testing.T) {
		tw, key, _ := setup(t)
		d1, d2 := strings.ToLower(key("a"))+".example.com", strings.ToLower(key("b"))+".example.com"
		left := create(t, tw.c, "companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": d1}})
		empty := create(t, tw.c, "companies", record{"name": "By hand", "externalId": "", "domainName": map[string]any{"primaryLinkUrl": d2}})
		h := harnessFor(t, tw.c, testBinding{
			resource:   `{"name":"acmeCo/accounts","object":"companies","id_field":"externalId","adopt_by":"domainName"}`,
			collection: collection("acmeCo/accounts", "string", proj("name", "/name", "string"), proj("domainName", "/domain", "string")),
		})
		k1, k2 := key("k1"), key("k2")
		raw, err := json.Marshal([]map[string]any{{"id": k1, "name": "Adopted", "domain": "https://www." + strings.ToUpper(d1)}, {"id": k2, "name": "Adopted", "domain": d2}})
		require.NoError(t, err)
		var docs []json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &docs))
		require.NoError(t, h.store(h.doc(0, string(docs[0])), h.doc(0, string(docs[1]))))
		require.Equal(t, 2, h.stats("companies").adopted)
		for id, k := range map[any]string{left["id"]: k1, empty["id"]: k2} {
			row := read(t, tw.c, "companies", id.(string))
			require.Equal(t, []any{k, "Adopted"}, []any{row["externalId"], row["name"]})
		}
	})

	t.Run("a compound key's encoded text finds its record again, apart from a key that joins the same text", func(t *testing.T) {
		tw, key, _ := setup(t)
		coll := collection("acmeCo/accounts", "string", proj("name", "/name", "string"))
		coll.Key = []string{"/region", "/id"}
		coll.Projections = append(coll.Projections, pf.Projection{Field: "region", Ptr: "/region", IsPrimaryKey: true,
			Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}})
		h := harnessFor(t, tw.c, testBinding{resource: `{"name":"acmeCo/accounts","object":"companies","id_field":"externalId"}`, collection: coll})
		region := key("eu/west é%")
		doc := func(region, id, name string) storeItem {
			raw, err := json.Marshal(map[string]any{"region": region, "id": id, "name": name})
			require.NoError(t, err)
			return h.doc(0, string(raw))
		}
		require.NoError(t, h.store(doc(region, "1", "A"), doc(region+"/1", "", "B")))
		require.NoError(t, h.store(doc(region, "1", "A2")))
		require.Equal(t, stats{received: 1, updated: 1}, h.stats("companies"))
		a, b := escape(region)+"/1", escape(region+"/1")+"/"
		rows, err := tw.c.find(ctx, "companies", "externalId", []string{a, b}, false)
		require.NoError(t, err)
		names := map[string]any{}
		for _, r := range rows {
			names[r["externalId"].(string)] = r["name"]
		}
		require.Equal(t, map[string]any{a: "A2", b: "B"}, names)
	})

	t.Run("a relation finds its record by a unique domain, as Twenty stores it", func(t *testing.T) {
		tw, key, _ := setup(t)
		domain := strings.ToLower(key("d")) + ".example.com"
		co := create(t, tw.c, "companies", record{"name": "Domain co", "externalId": key("co"), "domainName": map[string]any{"primaryLinkUrl": domain}})
		h := harnessFor(t, tw.c, probes(`,"relations":[{"field":"pCompany","id_field":"domainName","source_field":"site"}]`,
			proj("site", "/site", "string")))
		k := key("p")
		raw, err := json.Marshal(map[string]any{"id": k, "site": "https://www." + strings.ToUpper(domain) + "/about"})
		require.NoError(t, err)
		require.NoError(t, h.store(h.doc(0, string(raw))))
		rows, err := tw.c.find(ctx, "connectorTypeProbes", "externalId", []string{k}, false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, co["id"], rows[0]["pCompanyId"])
	})

	t.Run("two companies swap domains, each keeping its label and secondary links", func(t *testing.T) {
		tw, key, domain := setup(t)
		blog := []any{map[string]any{"url": "https://blog-" + tw.tag + ".example.com", "label": "Blog"}}
		a := create(t, tw.c, "companies", record{"name": "a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("x"), "primaryLinkLabel": "A", "secondaryLinks": blog}})
		b := create(t, tw.c, "companies", record{"name": "b", "externalId": key("b"), "domainName": record{"primaryLinkUrl": domain("y")}})
		h := harnessFor(t, tw.c, companies(""))
		doc := func(k, d string) storeItem {
			raw, err := json.Marshal(map[string]any{"id": k, "domain": d})
			require.NoError(t, err)
			return h.doc(0, string(raw))
		}
		require.NoError(t, h.store(doc(key("a"), domain("y")), doc(key("b"), domain("x"))))
		require.Equal(t, stats{received: 2, updated: 2}, h.stats("companies"))
		got := read(t, tw.c, "companies", a["id"].(string))["domainName"].(map[string]any)
		require.Contains(t, got["primaryLinkUrl"], domain("y"))
		require.Equal(t, "A", got["primaryLinkLabel"])
		require.Len(t, got["secondaryLinks"], 1)
		require.Contains(t, read(t, tw.c, "companies", b["id"].(string))["domainName"].(map[string]any)["primaryLinkUrl"], domain("x"))
	})

	t.Run("a stand-in that ends in its record's id and .invalid is a valid domain, email and text, which a like filter finds", func(t *testing.T) {
		tw, key, domain := setup(t)
		blog := []any{map[string]any{"url": "https://blog-" + tw.tag + ".example.com", "label": "Blog"}}
		a := create(t, tw.c, "companies", record{"name": "a", "externalId": key("a"), "domainName": record{"primaryLinkUrl": domain("x"), "primaryLinkLabel": "A", "secondaryLinks": blog}})
		id := a["id"].(string)
		stand := domain("x") + "." + id + ".invalid"
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "domainName": record{"primaryLinkUrl": stand, "primaryLinkLabel": "A", "secondaryLinks": blog}}})
		require.NoError(t, err)
		got := read(t, tw.c, "companies", id)["domainName"].(map[string]any)
		require.Equal(t, stand, got["primaryLinkUrl"])
		require.Equal(t, "A", got["primaryLinkLabel"])
		require.Len(t, got["secondaryLinks"], 1)
		rows, err := tw.c.findWhere(ctx, "companies", `domainName.primaryLinkUrl[like]:"%-`+tw.tag+`.example.com.%.invalid"`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, id, rows[0]["id"])

		p := create(t, tw.c, "people", record{"externalId": key("p"), "emails": record{"primaryEmail": key("p") + "@example.com"}})
		pid := p["id"].(string)
		standEmail := key("p") + "@example.com." + pid + ".invalid"
		_, err = tw.c.upsert(ctx, "people", []record{{"id": pid, "emails": record{"primaryEmail": standEmail}}})
		require.NoError(t, err)
		require.Equal(t, standEmail, read(t, tw.c, "people", pid)["emails"].(map[string]any)["primaryEmail"])
		rows, err = tw.c.findWhere(ctx, "people", `emails.primaryEmail[like]:"`+key("p")+`@%.invalid"`)
		require.NoError(t, err)
		require.Len(t, rows, 1)

		// A unique text field takes an empty value on several records at once, so two can swap
		// through it, and a stand-in like the others.
		x := create(t, tw.c, "connectorTypeProbes", record{"name": "x", "externalId": key("x")})
		y := create(t, tw.c, "connectorTypeProbes", record{"name": "y", "externalId": key("y")})
		_, err = tw.c.upsert(ctx, "connectorTypeProbes", []record{{"id": x["id"], "externalId": ""}, {"id": y["id"], "externalId": ""}})
		require.NoError(t, err)
		_, err = tw.c.upsert(ctx, "connectorTypeProbes", []record{{"id": x["id"], "externalId": key("y")}, {"id": y["id"], "externalId": key("x")}})
		require.NoError(t, err)
		require.Equal(t, key("y"), read(t, tw.c, "connectorTypeProbes", x["id"].(string))["externalId"])
		standText := key("y") + "." + x["id"].(string) + ".invalid"
		_, err = tw.c.upsert(ctx, "connectorTypeProbes", []record{{"id": x["id"], "externalId": standText}})
		require.NoError(t, err)
		rows, err = tw.c.findWhere(ctx, "connectorTypeProbes", `externalId[like]:"`+key("")+`%.invalid"`)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, standText, rows[0]["externalId"])
	})

	t.Run("filters match exactly and hide soft-deleted records unless asked", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		create(t, tw.c, "companies", record{"name": "Contract aa", "externalId": key("a") + "a"})
		tw.softDelete(t, "companies", a["id"].(string))
		live, err := tw.c.find(ctx, "companies", "externalId", []string{key("a")}, false)
		require.NoError(t, err)
		require.Empty(t, live)
		all, err := tw.c.find(ctx, "companies", "externalId", []string{key("a")}, true)
		require.NoError(t, err)
		require.Len(t, all, 1)
		require.Equal(t, a["id"], all[0]["id"])
	})

	t.Run("a quoted filter value may hold commas, apostrophes, parentheses and brackets", func(t *testing.T) {
		// Twenty v2.52 skips what is between a value's quotes when it counts parentheses and
		// splits and(...) terms and [in] lists, so these values match like any other.
		tw, key, _ := setup(t)
		values := []string{"plain", "x,y", "O'Brien", "a(b", "a)b", "a[1]", "a],b", "'q,x'", "a, b", "a)("}
		ids := map[string]string{}
		var keys []string
		for _, v := range values {
			k := key(v)
			keys = append(keys, k)
			ids[k] = create(t, tw.c, "companies", record{"name": "Special " + v, "externalId": k})["id"].(string)
		}
		for _, k := range keys {
			for _, deleted := range []bool{false, true} {
				rows, err := tw.c.find(ctx, "companies", "externalId", []string{k}, deleted)
				require.NoError(t, err, "%q", k)
				require.Len(t, rows, 1, "%q", k)
				require.Equal(t, ids[k], rows[0]["id"], "%q", k)
			}
		}
		for _, deleted := range []bool{false, true} {
			rows, err := tw.c.find(ctx, "companies", "externalId", keys, deleted)
			require.NoError(t, err)
			got := map[string]any{}
			for _, r := range rows {
				got[r["externalId"].(string)] = r["id"]
			}
			require.Len(t, got, len(keys))
			for _, k := range keys {
				require.Equal(t, ids[k], got[k], "%q", k)
			}
		}

		// [eq] alone, inside and(...) and inside or(and(...)) as the transactor's writes use it.
		for _, k := range keys {
			rows, err := tw.c.findWhere(ctx, "companies", `externalId[eq]:"`+k+`"`)
			require.NoError(t, err, "%q", k)
			require.Len(t, rows, 1, "%q", k)
			where, err := filterEq("externalId", k)
			require.NoError(t, err)
			rows, err = tw.c.update(ctx, "companies", []string{ids[k]}, record{"name": "Updated " + k}, where)
			require.NoError(t, err, "%q", k)
			require.Len(t, rows, 1, "%q", k)
			where, err = pairFilter("externalId", []string{ids[k], ids[keys[0]]}, map[string]string{ids[k]: k, ids[keys[0]]: keys[0]})
			require.NoError(t, err)
			rows, err = tw.c.update(ctx, "companies", []string{ids[k]}, record{"name": "Paired " + k}, where)
			require.NoError(t, err, "%q", k)
			require.Len(t, rows, 1, "%q", k)
		}
		for _, k := range keys[1:] {
			where, err := filterEq("externalId", k)
			require.NoError(t, err)
			rows, err := tw.c.remove(ctx, "companies", []string{ids[k]}, where, true)
			require.NoError(t, err, "%q", k)
			require.Len(t, rows, 1, "%q", k)
		}

		// A person's email, a composite subfield.
		var emails []string
		for _, local := range []string{"plain", "o'brien", "a(b)", "x,y"} {
			emails = append(emails, strings.ToLower(local+"-"+tw.tag+"@example.com"))
		}
		for _, e := range emails[:2] {
			create(t, tw.c, "people", record{"externalId": key(e), "emails": record{"primaryEmail": e}})
		}
		rows, err := tw.c.find(ctx, "people", "emails.primaryEmail", emails, false)
		require.NoError(t, err)
		require.Len(t, rows, 2)
	})

	t.Run("filtered update is a compare-and-set that never creates and skips soft-deleted records", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		b := create(t, tw.c, "companies", record{"name": "Contract b", "externalId": key("b")})
		id, bid := a["id"].(string), b["id"].(string)
		where, err := filterEq("externalId", key("other"))
		require.NoError(t, err)
		rows, err := tw.c.update(ctx, "companies", []string{id}, record{"name": "x"}, where)
		require.NoError(t, err)
		require.Empty(t, rows)
		tw.softDelete(t, "companies", bid)
		rows, err = tw.c.update(ctx, "companies", []string{id, bid, uuid.NewString()}, record{"externalDeleted": true}, "")
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, id, rows[0]["id"])
		require.NotEqual(t, true, read(t, tw.c, "companies", bid)["externalDeleted"])
	})

	t.Run("filtered delete trashes or deletes only live matching records, and a permanent one clears links to it", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		b := create(t, tw.c, "companies", record{"name": "Contract b", "externalId": key("b")})
		c := create(t, tw.c, "companies", record{"name": "Contract c", "externalId": key("c")})
		p := create(t, tw.c, "people", record{"externalId": key("p"), "companyId": c["id"]})
		id, bid, cid := a["id"].(string), b["id"].(string), c["id"].(string)
		where, err := filterEq("externalId", key("other"))
		require.NoError(t, err)
		rows, err := tw.c.remove(ctx, "companies", []string{id}, where, true)
		require.NoError(t, err)
		require.Empty(t, rows)

		// Trash: a and b go to the trash; trashing b again matches nothing.
		rows, err = tw.c.remove(ctx, "companies", []string{id, bid, uuid.NewString()}, "", true)
		require.NoError(t, err)
		require.ElementsMatch(t, []any{id, bid}, []any{rows[0]["id"], rows[1]["id"]})
		all, err := tw.c.find(ctx, "companies", "externalId", []string{key("a"), key("b")}, true)
		require.NoError(t, err)
		require.Len(t, all, 2)
		for _, r := range all {
			require.NotNil(t, r["deletedAt"])
		}
		rows, err = tw.c.remove(ctx, "companies", []string{bid}, "", false)
		require.NoError(t, err)
		require.Empty(t, rows, "a permanent delete with deletedAt[is]:NULL leaves trashed records")

		// Each id with its own key: c with b's key matches nothing.
		where, err = pairFilter("externalId", []string{cid, bid}, map[string]string{cid: key("b"), bid: key("b")})
		require.NoError(t, err)
		rows, err = tw.c.remove(ctx, "companies", []string{cid}, where, true)
		require.NoError(t, err)
		require.Empty(t, rows)

		// Permanent: c is gone, and the person that linked to it no longer does.
		rows, err = tw.c.remove(ctx, "companies", []string{cid}, "", false)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		all, err = tw.c.find(ctx, "companies", "externalId", []string{key("c")}, true)
		require.NoError(t, err)
		require.Empty(t, all)
		require.Nil(t, read(t, tw.c, "people", p["id"].(string))["companyId"])
	})

	t.Run("date-times come back in UTC with milliseconds", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"), "externalDeletedAt": "2026-10-29T01:02:03.123Z"})
		require.Equal(t, "2026-10-29T01:02:03.123Z", read(t, tw.c, "companies", a["id"].(string))["externalDeletedAt"])
	})

	t.Run("a number starting with + refuses a conflicting country code", func(t *testing.T) {
		tw, key, _ := setup(t)
		_, err := tw.c.create(ctx, "people", []record{{"externalId": key("p"),
			"phones": record{"primaryPhoneNumber": "+14155550123", "primaryPhoneCountryCode": "SG", "primaryPhoneCallingCode": "+65"}}})
		require.Contains(t, strings.Join(rejected(t, err).messages, " "), "conflicting")
		p := create(t, tw.c, "people", record{"externalId": key("q"), "phones": record{"primaryPhoneNumber": "+14155550123"}})
		// Twenty splits the calling code off the number; the fake keeps it whole.
		row := read(t, tw.c, "people", p["id"].(string))
		calling, _ := fakeGet(row, "phones.primaryPhoneCallingCode").(string)
		require.Equal(t, "+14155550123", calling+fakeGet(row, "phones.primaryPhoneNumber").(string),
			"Twenty reads the codes from the number")
	})

	t.Run("an empty primary link promotes a secondary link; null subfields clear it whole", func(t *testing.T) {
		tw, key, domain := setup(t)
		links := record{"primaryLinkUrl": domain("a"), "primaryLinkLabel": "",
			"secondaryLinks": []any{record{"url": domain("b"), "label": ""}}}
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a"), "domainName": links})
		id := a["id"].(string)
		_, err := tw.c.upsert(ctx, "companies", []record{{"id": id, "domainName": record{"primaryLinkUrl": nil, "primaryLinkLabel": "",
			"secondaryLinks": []any{record{"url": domain("b"), "label": ""}}}}})
		require.NoError(t, err)
		require.Equal(t, domain("b"), read(t, tw.c, "companies", id)["domainName"].(map[string]any)["primaryLinkUrl"])
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": id, "domainName": links}})
		require.NoError(t, err)
		_, err = tw.c.upsert(ctx, "companies", []record{{"id": id, "domainName": record{"primaryLinkUrl": nil, "primaryLinkLabel": nil, "secondaryLinks": nil}}})
		require.NoError(t, err)
		got := read(t, tw.c, "companies", id)["domainName"].(map[string]any)
		require.Empty(t, got["primaryLinkUrl"])
		require.Empty(t, got["secondaryLinks"])
	})

	t.Run("writes answer with the stored record", func(t *testing.T) {
		tw, key, _ := setup(t)
		a := create(t, tw.c, "companies", record{"name": "Contract a", "externalId": key("a")})
		require.NotEmpty(t, a["id"])
		require.Equal(t, key("a"), a["externalId"])
		require.Equal(t, "Contract a", a["name"])
	})
}
