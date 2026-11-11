package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

// logs catches the connector's log lines, which the tests read the stored counts from.
var logs = logtest.NewGlobal()

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// --- collections ---------------------------------------------------------------------------

func proj(field, ptr string, types ...string) pf.Projection {
	return pf.Projection{Field: field, Ptr: ptr, Inference: pf.Inference{Types: types, Exists: pf.Inference_MAY}}
}

func dateTime(p pf.Projection) pf.Projection {
	p.Inference.String_ = &pf.Inference_String{Format: "date-time"}
	return p
}

// collection is a collection keyed by /id, with its root document and the given projections.
func collection(name, keyType string, projections ...pf.Projection) pf.CollectionSpec {
	key := pf.Projection{Field: "id", Ptr: "/id", IsPrimaryKey: true,
		Inference: pf.Inference{Types: []string{keyType}, Exists: pf.Inference_MUST}}
	doc := pf.Projection{Field: "flow_document", Ptr: "",
		Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST}}
	all := append([]pf.Projection{key, doc}, projections...)
	slices.SortFunc(all, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	return pf.CollectionSpec{Name: pf.Collection(name), Key: []string{"/id"}, Projections: all}
}

func companiesCollection() pf.CollectionSpec {
	return collection("acmeCo/companies", "string",
		proj("accountCode", "/code", "string"),
		proj("address/addressCity", "/city", "string"),
		proj("address/addressCountry", "/country", "string"),
		proj("annualRevenue", "/revenue", "object"),
		proj("domainName", "/domain", "string"),
		proj("employees", "/employees", "integer"),
		proj("externalId", "/externalId", "string"),
		dateTime(proj("flow_published_at", "/_meta/uuid", "string")),
		proj("idealCustomerProfile", "/icp", "boolean"),
		dateTime(proj("lastOrderAt", "/lastOrderAt", "string")),
		proj("linkedinLink", "/linkedin", "string"),
		proj("name", "/name", "null", "string"),
		proj("nope", "/nope", "string"),
		proj("position", "/position", "integer"),
		proj("score", "/score", "number"),
		proj("tags", "/tags", "array"),
	)
}

func peopleCollection() pf.CollectionSpec {
	return collection("acmeCo/people", "integer",
		proj("company", "/company", "string"),
		proj("companyKey", "/companyKey", "null", "integer"),
		proj("emails", "/email", "string"),
		proj("emails/additionalEmails", "/moreEmails", "array"),
		proj("jobTitle", "/title", "null", "string"),
		proj("name/firstName", "/first", "string"),
		proj("name/lastName", "/last", "string"),
		proj("phones", "/phones", "object"),
	)
}

type testBinding struct {
	resource   string
	collection pf.CollectionSpec
}

func companies(options string) testBinding {
	return testBinding{
		resource:   `{"name":"acmeCo/companies","object":"companies","id_field":"externalId"` + options + `}`,
		collection: companiesCollection(),
	}
}

func people(options string) testBinding {
	return testBinding{
		resource:   `{"name":"acmeCo/people","object":"people","id_field":"externalId"` + options + `}`,
		collection: peopleCollection(),
	}
}

func notes(options string, body pf.Projection) testBinding {
	return testBinding{
		resource:   `{"name":"acmeCo/notes","object":"notes","id_field":"externalId"` + options + `}`,
		collection: collection("acmeCo/notes", "string", proj("title", "/title", "string"), body),
	}
}

const (
	flagDeletes = `,"delete_mode":"flag","deleted_field":"externalDeleted","deleted_at_field":"externalDeletedAt"`
	// linkCompany links a person to the company whose externalId is the document's /company.
	linkCompany = `,"relations":[{"field":"company","id_field":"externalId"}]`
)

// --- harness ---------------------------------------------------------------------------------

// harness runs the transactor against a fake Twenty, or a real one in the contract tests. Each
// binding selects every field that validateBinding allows or requires.
type harness struct {
	t           *testing.T
	fake        *fakeTwenty // nil for a real Twenty
	spec        *pf.MaterializationSpec
	tx          *transactor
	constraints []map[string][]*pm.Response_Validated_Constraint // by binding
	now         time.Time                                        // the transactor's clock, after clock
}

// clock starts the transactor's clock at a fixed time, which tests then move with h.now.
func (h *harness) clock() {
	h.now = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return h.now }
}

// waitingJane is a harness whose person 1 waits for company k1, which is now in Twenty.
func waitingJane(t *testing.T) *harness {
	h := newHarness(t, people(linkCompany), companies(""))
	h.clock()
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`)))
	h.fake.seed("companies", record{"externalId": "k1"})
	return h
}

func newHarness(t *testing.T, bindings ...testBinding) *harness {
	t.Helper()
	fake, c := newFakeTwenty(t)
	h := harnessFor(t, c, bindings...)
	h.fake = fake
	return h
}

func harnessFor(t *testing.T, c *client, bindings ...testBinding) *harness {
	t.Helper()
	objects, err := c.objects(t.Context())
	require.NoError(t, err)

	h := &harness{t: t, spec: &pf.MaterializationSpec{Name: "acmeCo/twenty", ConfigJson: json.RawMessage(`{}`)}}
	var mapped []*binding
	for _, tb := range bindings {
		var res resource
		require.NoError(t, boilerplate.UnmarshalStrict(json.RawMessage(tb.resource), &res))
		obj, err := findObject(objects, res.Object)
		require.NoError(t, err)
		links, err := checkRelations(t.Context(), c, objects, res, obj)
		require.NoError(t, err)
		constraints, err := validateBinding(res, obj, links, &tb.collection)
		require.NoError(t, err)
		h.constraints = append(h.constraints, constraints)
		var values []string
		for field, cs := range constraints {
			if ty := cs[0].Type; ty == pm.Response_Validated_Constraint_FIELD_OPTIONAL || ty == pm.Response_Validated_Constraint_FIELD_REQUIRED {
				values = append(values, field)
			}
		}
		slices.Sort(values)
		b := &pf.MaterializationSpec_Binding{
			ResourceConfigJson: json.RawMessage(tb.resource),
			ResourcePath:       res.path(),
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: keyFields(tb.collection), Values: values, Document: "flow_document"},
		}
		h.spec.Bindings = append(h.spec.Bindings, b)
		m, err := newBinding(res, obj, links, h.spec, b)
		require.NoError(t, err)
		mapped = append(mapped, m)
	}
	now := time.Date(2026, 10, 29, 1, 2, 3, 0, time.UTC)
	h.tx = &transactor{client: c, bindings: mapped, be: materialize.NewBindingEvents(), now: func() time.Time { return now }}
	return h
}

type storeItem struct {
	binding int
	key     tuple.Tuple
	values  tuple.Tuple
	doc     json.RawMessage
	delete  bool
}

type fakeStream struct{ items []storeItem }

func (s *fakeStream) Send(*pm.Response) error { return nil }

func (s *fakeStream) RecvMsg(m *pm.Request) error {
	if len(s.items) == 0 {
		return nil // a request without a Store ends the iteration
	}
	item := s.items[0]
	s.items = s.items[1:]
	m.Store = &pm.Request_Store{
		Binding:      uint32(item.binding),
		KeyPacked:    item.key.Pack(),
		ValuesPacked: item.values.Pack(),
		DocJson:      item.doc,
		Delete:       item.delete,
	}
	return nil
}

// doc makes the store of a document, with its key and values read from it as the runtime does.
// keyFields are the collection's key projections, in the key's order, as Flow selects them.
func keyFields(c pf.CollectionSpec) []string {
	var out []string
	for _, ptr := range c.Key {
		if i := slices.IndexFunc(c.Projections, func(p pf.Projection) bool { return p.IsPrimaryKey && p.Ptr == ptr }); i >= 0 {
			out = append(out, c.Projections[i].Field)
		}
	}
	return out
}

func (h *harness) doc(binding int, doc string) storeItem {
	h.t.Helper()
	var parsed any
	require.NoError(h.t, decode([]byte(doc), &parsed))
	b := h.spec.Bindings[binding]
	extract := func(field string) any {
		p := b.Collection.GetProjection(field)
		v, ok := lookup(parsed, p.Ptr)
		if !ok && len(p.Inference.DefaultJson) > 0 {
			// As the runtime does.
			require.NoError(h.t, decode(p.Inference.DefaultJson, &v))
		} else if !ok {
			return nil
		}
		switch v := v.(type) {
		case json.Number:
			if i, err := v.Int64(); err == nil {
				return i
			}
			f, err := v.Float64()
			require.NoError(h.t, err)
			return f
		case map[string]any, []any:
			raw, err := json.Marshal(v)
			require.NoError(h.t, err)
			return raw
		}
		return v
	}
	item := storeItem{binding: binding, doc: json.RawMessage(doc)}
	for _, k := range b.FieldSelection.Keys {
		item.key = append(item.key, extract(k))
	}
	for _, v := range b.FieldSelection.Values {
		item.values = append(item.values, extract(v))
	}
	return item
}

// deletion makes the store of a deleted document, as the runtime sends it in delta updates:
// without the Delete flag, so only _meta/op tells it apart.
func (h *harness) deletion(binding int, key any) storeItem {
	h.t.Helper()
	k, err := json.Marshal(key)
	require.NoError(h.t, err)
	return h.doc(binding, fmt.Sprintf(`{"id":%s,"_meta":{"op":"d"}}`, k))
}

func lookup(doc any, ptr string) (any, bool) {
	if ptr == "" {
		return doc, true
	}
	for _, token := range strings.Split(strings.TrimPrefix(ptr, "/"), "/") {
		m, ok := doc.(map[string]any)
		if !ok {
			return nil, false
		}
		if doc, ok = m[token]; !ok {
			return nil, false
		}
	}
	return doc, true
}

func (h *harness) store(items ...storeItem) error {
	h.t.Helper()
	logs.Reset()
	it := materialize.NewStoreIterator(h.t.Context(), &fakeStream{items: items}, &pm.Request{Store: &pm.Request_Store{}})
	_, err := h.tx.Store(it)
	return err
}

// stats returns the counts the last Store logged for an object.
func (h *harness) stats(object string) stats {
	h.t.Helper()
	for _, e := range slices.Backward(logs.AllEntries()) {
		if e.Message != "stored documents" || e.Data["object"] != object {
			continue
		}
		return stats{
			received:    e.Data["received"].(int),
			superseded:  e.Data["superseded"].(int),
			created:     e.Data["created"].(int),
			updated:     e.Data["updated"].(int),
			flagged:     e.Data["flaggedDeleted"].(int),
			deleted:     e.Data["deleted"].(int),
			rejected:    e.Data["rejected"].(int),
			linkedLater: e.Data["linkedLater"].(int),
			adopted:     e.Data["adopted"].(int),
			skipped:     e.Data["skipped"].(map[string]int),
			invalid:     e.Data["invalidValuesLeftOut"].(map[string]int),
		}
	}
	h.t.Fatalf("no counts were logged for %s", object)
	return stats{}
}

func (h *harness) only(object, key string) record {
	h.t.Helper()
	rows := h.fake.byKey(object, key)
	require.Len(h.t, rows, 1, "records with key %q", key)
	return rows[0]
}

// --- spec and validation -----------------------------------------------------------------------

func TestSpec(t *testing.T) {
	resp, err := Driver{}.Spec(t.Context(), &pm.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestValidateBinding(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	companiesObj, err := findObject(objects, "companies")
	require.NoError(t, err)

	coll := companiesCollection()
	got, err := validateBinding(resource{Object: "companies", IDField: "externalId"}, companiesObj, nil, &coll)
	require.NoError(t, err)
	types := make(map[string]pm.Response_Validated_Constraint_Type)
	var snap strings.Builder
	for _, field := range slices.Sorted(maps.Keys(got)) {
		types[field] = got[field][0].Type
		fmt.Fprintf(&snap, "%s: %s: %s\n", field, got[field][0].Type, got[field][0].Reason)
	}
	const (
		required  = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		optional  = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		forbidden = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
	)
	require.Equal(t, map[string]pm.Response_Validated_Constraint_Type{
		"accountCode": optional, "address/addressCity": optional, "address/addressCountry": optional,
		"annualRevenue": optional, "domainName": optional, "employees": optional, "externalId": forbidden,
		"flow_document": required, "flow_published_at": forbidden, "id": required, "idealCustomerProfile": optional,
		"lastOrderAt": optional, "linkedinLink": optional, "name": optional, "nope": forbidden, "position": forbidden,
		"score": forbidden, "tags": optional,
	}, types)
	cupaloy.SnapshotT(t, snap.String())

	peopleObj, err := findObject(objects, "people")
	require.NoError(t, err)
	pc := peopleCollection()
	got, err = validateBinding(resource{Object: "people", IDField: "externalId"}, peopleObj, nil, &pc)
	require.NoError(t, err)
	require.Equal(t, forbidden, got["company"][0].Type)
	require.Contains(t, got["company"][0].Reason, `"company" is a relation. To link records through it, add it to the binding's relations`)
	require.Equal(t, optional, got["name/firstName"][0].Type)

	for _, tc := range []struct {
		name    string
		res     resource
		coll    func() pf.CollectionSpec
		wantErr string
	}{
		{"missing ID field", resource{Object: "companies", IDField: "missing"}, companiesCollection,
			`the Twenty object "companies" has no field "missing"`},
		{"ID field not text", resource{Object: "companies", IDField: "employees"}, companiesCollection,
			"companies.employees is a NUMBER field, and must be a TEXT field"},
		{"ID field not unique", resource{Object: "companies", IDField: "name"}, companiesCollection,
			"companies.name must be unique"},
		{"deleted field not boolean", resource{Object: "companies", IDField: "externalId", DeleteMode: "flag", DeletedField: "name"},
			companiesCollection, "companies.name is a TEXT field, and must be a BOOLEAN field"},
		{"system ID field", resource{Object: "companies", IDField: "position"}, companiesCollection,
			"is a POSITION field"},
		{"compound key with a number part", resource{Object: "companies", IDField: "externalId"}, func() pf.CollectionSpec {
			c := companiesCollection()
			c.Key = []string{"/id", "/score"}
			c.GetProjection("score").IsPrimaryKey = true
			c.GetProjection("score").Inference.Exists = pf.Inference_MUST
			return c
		}, `the key field "score" must be a string or an integer, not number`},
		{"nullable key", resource{Object: "companies", IDField: "externalId"}, func() pf.CollectionSpec {
			c := companiesCollection()
			c.GetProjection("id").Inference.Types = []string{"null", "string"}
			return c
		}, `the key field "id" may be missing or null`},
		{"number key", resource{Object: "companies", IDField: "externalId"}, func() pf.CollectionSpec {
			c := companiesCollection()
			c.GetProjection("id").Inference.Types = []string{"number"}
			return c
		}, `the key field "id" must be a string or an integer, not number`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := tc.coll()
			_, err := validateBinding(tc.res, companiesObj, nil, &coll)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestNewBinding(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	res := resource{Object: "companies", IDField: "externalId"}

	for _, tc := range []struct {
		name    string
		sel     pf.FieldSelection
		wantErr string
	}{
		{"no keys", pf.FieldSelection{Values: []string{"name"}, Document: "flow_document"},
			"selects no key fields"},
		{"no document", pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}},
			"must select the root document"},
		{"forbidden field", pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name", "position"}, Document: "flow_document"},
			`the field "position" can't be written: Twenty manages the field "position"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := &pf.MaterializationSpec{}
			b := &pf.MaterializationSpec_Binding{Collection: companiesCollection(), FieldSelection: tc.sel}
			_, err := newBinding(res, obj, nil, spec, b)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	// Twenty's schema changed since Validate: the ID field is gone.
	_, err = findObject(objects, "opportunities")
	require.ErrorContains(t, err, `Twenty has no object "opportunities"`)
	gone := *obj
	gone.Fields = slices.DeleteFunc(slices.Clone(obj.Fields), func(f fieldMeta) bool { return f.Name == "externalId" })
	spec := &pf.MaterializationSpec{}
	b := &pf.MaterializationSpec_Binding{Collection: companiesCollection(),
		FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}, Document: "flow_document"}}
	_, err = newBinding(res, &gone, nil, spec, b)
	require.ErrorContains(t, err, `has no field "externalId"`)
}

func TestConnect(t *testing.T) {
	_, c := newFakeTwenty(t)

	_, objects, err := connect(t.Context(), config{BaseURL: c.baseURL, APIKey: fakeAPIKey})
	require.NoError(t, err)
	require.Len(t, objects, 8)

	_, _, err = connect(t.Context(), config{BaseURL: c.baseURL, APIKey: "wrong"})
	var userErr *cerrors.UserError
	require.ErrorAs(t, err, &userErr)
	require.ErrorContains(t, err, "Twenty refused the API key")
	require.ErrorContains(t, err, "Token invalid.")
}

// --- stores --------------------------------------------------------------------------------------

func TestStoreCreatesThenUpdates(t *testing.T) {
	h := newHarness(t, companies(""))
	acme := h.doc(0, `{"id":"k1","name":"Acme","domain":"https://www.Acme.com/about","employees":250,"icp":true,
		"tags":["a","b"],"lastOrderAt":"2026-10-28T09:30:00+08:00","code":"A-1","city":"Singapore",
		"revenue":{"amountMicros":9007199254740991,"currencyCode":"SGD"},"linkedin":"https://linkedin.com/company/acme"}`)
	beta := h.doc(0, `{"id":"k2","name":"Beta"}`)

	require.NoError(t, h.store(acme, beta))
	require.Equal(t, stats{received: 2, created: 2}, h.stats("companies"))
	row := h.only("companies", "k1")
	require.Equal(t, "Acme", row["name"])
	require.Equal(t, "acme.com", fakeGet(row, "domainName.primaryLinkUrl"))
	require.Equal(t, json.Number("250"), row["employees"])
	require.Equal(t, true, row["idealCustomerProfile"])
	require.Equal(t, []any{"a", "b"}, row["tags"])
	require.Equal(t, "2026-10-28T01:30:00.000Z", row["lastOrderAt"])
	require.Equal(t, "A-1", row["accountCode"])
	require.Equal(t, "Singapore", fakeGet(row, "address.addressCity"))
	require.Equal(t, json.Number("9007199254740991"), fakeGet(row, "annualRevenue.amountMicros"))
	require.Equal(t, "https://linkedin.com/company/acme", fakeGet(row, "linkedinLink.primaryLinkUrl"))
	require.Equal(t, false, row["externalDeleted"], "delete_mode ignore leaves the flag alone")

	// Re-sent documents update the same records, by their Twenty ids.
	require.NoError(t, h.store(acme, beta))
	require.Equal(t, stats{received: 2, updated: 2}, h.stats("companies"))
	require.Equal(t, 2, h.fake.rowCount("companies"))
	require.Equal(t, []string{"create", "upsert"}, h.fake.writes("companies"))
}

func TestStoreMissingVersusNull(t *testing.T) {
	h := newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","employees":250,"city":"Singapore","country":"SG"}`)))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":null,"city":"Jurong"}`)))

	row := h.only("companies", "k1")
	require.Equal(t, "", row["name"], "a null clears the field, and Twenty keeps a null TEXT as \"\"")
	require.Equal(t, json.Number("250"), row["employees"], "a missing field keeps its value")
	require.Equal(t, "Jurong", fakeGet(row, "address.addressCity"))
	require.Equal(t, "SG", fakeGet(row, "address.addressCountry"), "subfields merge")
}

func TestStoreSchemaDefault(t *testing.T) {
	c := companiesCollection()
	for i, p := range c.Projections {
		if p.Field == "employees" {
			c.Projections[i].Inference.DefaultJson = json.RawMessage(`1`)
		}
	}
	h := newHarness(t, testBinding{resource: companies("").resource, collection: c})
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","employees":250}`)))
	// Flow sends the default for the missing location, but the document didn't set it.
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.Equal(t, json.Number("250"), h.only("companies", "k1")["employees"])
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","employees":1}`)))
	require.Equal(t, json.Number("1"), h.only("companies", "k1")["employees"])
}

func TestStoreComposites(t *testing.T) {
	h := newHarness(t, companies(""), people(""))
	require.NoError(t, h.store(
		h.doc(0, `{"id":"k1","name":"Acme","linkedin":"https://linkedin.com/company/acme","revenue":{"amountMicros":5,"currencyCode":"SGD"}}`),
		h.doc(1, `{"id":1,"first":"Jane","phones":{"primaryPhoneNumber":"91234567","primaryPhoneCountryCode":"SG"}}`),
	))
	acme, jane := h.only("companies", "k1")["id"].(string), h.only("people", "1")["id"].(string)
	// Someone adds values in Twenty that the documents don't set.
	h.fake.rows["companies"][acme]["linkedinLink"] = map[string]any{"primaryLinkUrl": "https://linkedin.com/company/acme",
		"primaryLinkLabel": "Acme", "secondaryLinks": []any{map[string]any{"url": "https://acme.com", "label": "Site"}}}
	extra := []any{map[string]any{"number": "98765432", "countryCode": "SG", "callingCode": "+65"}}
	h.fake.rows["people"][jane]["phones"].(map[string]any)["additionalPhones"] = extra

	// Twenty would clear them, so the connector sends them back.
	require.NoError(t, h.store(
		h.doc(0, `{"id":"k1","linkedin":"https://linkedin.com/company/acme-2","revenue":null}`),
		h.doc(1, `{"id":1,"phones":{"primaryPhoneNumber":"81234567","primaryPhoneCountryCode":"SG"}}`),
	))
	row := h.only("companies", "k1")
	require.Equal(t, map[string]any{"primaryLinkUrl": "https://linkedin.com/company/acme-2", "primaryLinkLabel": "Acme",
		"secondaryLinks": []any{map[string]any{"url": "https://acme.com", "label": "Site"}}}, row["linkedinLink"])
	require.Equal(t, map[string]any{"amountMicros": nil, "currencyCode": nil}, row["annualRevenue"], "a null clears a composite field")
	require.Equal(t, "81234567", fakeGet(h.only("people", "1"), "phones.primaryPhoneNumber"))
	require.Equal(t, extra, fakeGet(h.only("people", "1"), "phones.additionalPhones"))

	// A null phones field clears all of it.
	require.NoError(t, h.store(h.doc(1, `{"id":1,"phones":null}`)))
	require.Equal(t, map[string]any{"primaryPhoneNumber": "", "primaryPhoneCountryCode": "", "primaryPhoneCallingCode": "",
		"additionalPhones": []any{}}, h.only("people", "1")["phones"])
}

func TestStoreSupersededDocuments(t *testing.T) {
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Old"}`), h.doc(0, `{"id":"k1","name":"New"}`)))
	require.Equal(t, stats{received: 2, superseded: 1, created: 1}, h.stats("companies"))
	require.Equal(t, "New", h.only("companies", "k1")["name"])

	// A rejected newer document leaves the older one to be written, as writing each in turn would.
	require.NoError(t, h.store(h.doc(0, `{"id":"k2","name":"Old"}`), h.doc(0, `{"id":"k2","name":"New","lastOrderAt":"yesterday"}`)))
	require.Equal(t, stats{received: 2, created: 1, rejected: 1}, h.stats("companies"))
	require.Equal(t, "Old", h.only("companies", "k2")["name"])
}

func TestStoreDeletionAfterWrite(t *testing.T) {
	// Ignored, the deletion leaves the write before it in the chunk.
	h := newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"New"}`), h.deletion(0, "k1")))
	require.Equal(t, "New", h.only("companies", "k1")["name"])
	require.Equal(t, map[string]int{"deletion_ignored": 1}, h.stats("companies").skipped)

	// Flagged, the record is written, then flagged, as writing each in turn would.
	h = newHarness(t, companies(flagDeletes))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"New"}`), h.deletion(0, "k1")))
	row := h.only("companies", "k1")
	require.Equal(t, "New", row["name"])
	require.Equal(t, true, row["externalDeleted"])
}

func TestStoreIgnoresDeletions(t *testing.T) {
	h := newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.NoError(t, h.store(h.deletion(0, "k1")))
	require.Equal(t, stats{received: 1, skipped: map[string]int{"deletion_ignored": 1}}, h.stats("companies"))
	require.Equal(t, "Acme", h.only("companies", "k1")["name"])
	require.Zero(t, h.fake.count(http.MethodPatch, "/rest/companies"))
}

func TestStoreFlagsDeletions(t *testing.T) {
	h := newHarness(t, companies(flagDeletes))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`),
		h.doc(0, `{"id":"k3","name":"Gone"}`)))
	require.Equal(t, false, h.only("companies", "k1")["externalDeleted"])
	gone := h.only("companies", "k3")
	h.fake.softDelete("companies", gone["id"].(string))

	require.NoError(t, h.store(h.deletion(0, "k1"), h.deletion(0, "k3"), h.deletion(0, "k4")))
	require.Equal(t, stats{received: 3, flagged: 1,
		skipped: map[string]int{"deletion_deleted_in_twenty": 1, "deletion_not_in_twenty": 1}}, h.stats("companies"))
	row := h.only("companies", "k1")
	require.Equal(t, true, row["externalDeleted"])
	require.Equal(t, "2026-10-29T01:02:03.000Z", row["externalDeletedAt"])
	require.Nil(t, row["deletedAt"], "the connector never deletes a record")
	require.Equal(t, "Acme", row["name"])
	require.Equal(t, false, h.only("companies", "k2")["externalDeleted"])
	require.Equal(t, false, h.fake.row("companies", gone["id"].(string))["externalDeleted"])
	require.Empty(t, h.fake.byKey("companies", "k4"), "a deletion never creates a record")

	require.NoError(t, h.store(h.deletion(0, "k1")))
	require.Equal(t, stats{received: 1, skipped: map[string]int{"deletion_already_flagged": 1}}, h.stats("companies"))

	// The document comes back, and the flag clears.
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme 2"}`)))
	row = h.only("companies", "k1")
	require.Equal(t, false, row["externalDeleted"])
	require.Nil(t, row["externalDeletedAt"])
	require.Equal(t, "Acme 2", row["name"])
}

func TestStoreTrashesOrDeletes(t *testing.T) {
	for _, mode := range []string{deleteTrash, deleteDelete} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, companies(`,"delete_mode":"`+mode+`"`))
			var query url.Values
			h.fake.hook = func(r *http.Request) {
				if r.Method == http.MethodDelete {
					query = r.URL.Query()
				}
			}
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`),
				h.doc(0, `{"id":"k3","name":"Gone"}`)))
			id := h.only("companies", "k1")["id"].(string)
			gone := h.only("companies", "k3")["id"].(string)
			h.fake.softDelete("companies", gone)

			require.NoError(t, h.store(h.deletion(0, "k1"), h.deletion(0, "k3"), h.deletion(0, "k4")))
			require.Equal(t, stats{received: 3, deleted: 1,
				skipped: map[string]int{"deletion_deleted_in_twenty": 1, "deletion_not_in_twenty": 1}}, h.stats("companies"))
			require.Equal(t, strconv.FormatBool(mode == deleteTrash), query.Get("soft_delete"))
			require.Contains(t, query.Get("filter"), "deletedAt[is]:NULL")
			require.Equal(t, "Beta", h.only("companies", "k2")["name"])
			require.NotNil(t, h.fake.row("companies", gone), "a record trashed in Twenty stays in the trash")
			if mode == deleteTrash {
				require.NotNil(t, h.fake.row("companies", id)["deletedAt"])
			} else {
				require.Nil(t, h.fake.row("companies", id))
			}

			// The document comes back: a trashed record stays trashed unless restore_deleted is on;
			// a deleted one is created anew.
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme 2"}`)))
			if mode == deleteTrash {
				require.Equal(t, stats{received: 1, skipped: map[string]int{"deleted_in_twenty": 1}}, h.stats("companies"))
			} else {
				require.Equal(t, stats{received: 1, created: 1}, h.stats("companies"))
				require.Equal(t, "Acme 2", h.only("companies", "k1")["name"])
			}
		})
	}
}

func TestStoreDeleteThenWrite(t *testing.T) {
	// In one chunk, a write after a deletion goes after it, as writing each in turn would.
	h := newHarness(t, companies(`,"delete_mode":"delete"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","accountCode":"A-1"}`)))
	old := h.only("companies", "k1")["id"]
	require.NoError(t, h.store(h.deletion(0, "k1"), h.doc(0, `{"id":"k1","name":"Acme 2"}`)))
	require.Equal(t, stats{received: 2, deleted: 1, created: 1}, h.stats("companies"))
	row := h.only("companies", "k1")
	require.NotEqual(t, old, row["id"])
	require.Equal(t, "Acme 2", row["name"])
	require.Empty(t, row["accountCode"], "the new record has none of the deleted one's fields")

	h = newHarness(t, companies(`,"delete_mode":"trash","restore_deleted":true`))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.NoError(t, h.store(h.deletion(0, "k1"), h.doc(0, `{"id":"k1","name":"Acme 2"}`)))
	require.Equal(t, stats{received: 2, deleted: 1, updated: 1}, h.stats("companies"))
	row = h.only("companies", "k1")
	require.Nil(t, row["deletedAt"], "restore_deleted brings the trashed record back")
	require.Equal(t, "Acme 2", row["name"])

	// A deletion after a write: written, then deleted.
	h = newHarness(t, companies(`,"delete_mode":"delete"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"New"}`), h.deletion(0, "k1")))
	require.Equal(t, stats{received: 2, created: 1, deleted: 1}, h.stats("companies"))
	require.Empty(t, h.fake.byKey("companies", "k1"))
}

func TestResourceDeleteModes(t *testing.T) {
	for _, mode := range []string{deleteIgnore, deleteTrash, deleteDelete} {
		res := resource{Object: "companies", IDField: "externalId", DeleteMode: mode}
		require.NoError(t, res.Validate())
		res.DeletedField = "externalDeleted"
		require.ErrorContains(t, res.Validate(), `"deleted_field" and "deleted_at_field" apply only when "delete_mode" is "flag"`)
	}
	require.ErrorContains(t, resource{Object: "companies", IDField: "externalId", DeleteMode: "purge"}.Validate(),
		`"delete_mode" must be "ignore", "flag", "trash" or "delete"`)
}

func TestStoreDeleteRace(t *testing.T) {
	h := newHarness(t, companies(`,"delete_mode":"delete"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	id := h.only("companies", "k1")["id"].(string)
	// Someone changes the record's key between the connector's lookup and its delete.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodDelete {
			h.fake.rows["companies"][id]["externalId"] = "k1-renamed"
		}
	}
	require.NoError(t, h.store(h.deletion(0, "k1")))
	require.Equal(t, stats{received: 1, skipped: map[string]int{"deletion_changed_in_twenty": 1}}, h.stats("companies"))
	require.NotNil(t, h.fake.row("companies", id))
}

func TestStoreDeleteSkippedByTwenty(t *testing.T) {
	for _, mode := range []string{deleteTrash, deleteDelete} {
		t.Run(mode, func(t *testing.T) {
			// Twenty answers the delete without an error but deletes nothing, as v2.30 to v2.34 do.
			h := newHarness(t, companies(`,"delete_mode":"`+mode+`"`))
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`)))
			h.fake.noDelete = true
			err := h.store(h.deletion(0, "k1"))
			require.ErrorContains(t, err, `companies: can't write the document with key "k1": Twenty answered the delete without an error, but the record is still live with the same key`)

			// Under rejected_records skip, it is counted as rejected, not deleted.
			h = newHarness(t, companies(`,"delete_mode":"`+mode+`","rejected_records":"skip"`))
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`)))
			h.fake.noDelete = true
			require.NoError(t, h.store(h.deletion(0, "k1"), h.deletion(0, "k2")))
			got := h.stats("companies")
			require.Equal(t, 2, got.rejected)
			require.Zero(t, got.deleted)
			require.Empty(t, got.skipped)
		})
	}
}

func TestStoreDeleteGoneMeanwhile(t *testing.T) {
	// The record is trashed or deleted between the lookup and the delete, by someone in Twenty or by
	// an earlier try of the same delete whose answer was lost: skipped, not rejected.
	for _, mode := range []string{deleteTrash, deleteDelete} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, companies(`,"delete_mode":"`+mode+`"`))
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
			id := h.only("companies", "k1")["id"].(string)
			h.fake.hook = func(r *http.Request) {
				if r.Method != http.MethodDelete {
					return
				} else if mode == deleteTrash {
					h.fake.rows["companies"][id]["deletedAt"] = "2026-11-03T00:00:00.000Z"
				} else {
					delete(h.fake.rows["companies"], id)
				}
			}
			require.NoError(t, h.store(h.deletion(0, "k1")))
			require.Equal(t, stats{received: 1, skipped: map[string]int{"deletion_changed_in_twenty": 1}}, h.stats("companies"))
		})
	}
}

func TestStoreDeleteSkippedKeepsLinks(t *testing.T) {
	// One record of a batch is deleted and one is left live by Twenty: under skip, the first counts
	// as deleted and drops its waiting link, and the second is rejected and keeps its own.
	h := newHarness(t, people(linkCompany+`,"delete_mode":"trash","rejected_records":"skip"`), companies(""))
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(0, `{"id":2,"first":"Raj","company":"k1"}`)))
	raj := h.only("people", "2")["id"].(string)
	// Twenty skips Raj's delete: he is hidden from the delete only, and back for the next request.
	var hidden record
	h.fake.hook = func(r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			hidden = h.fake.rows["people"][raj]
			delete(h.fake.rows["people"], raj)
		case hidden != nil:
			h.fake.rows["people"][raj], hidden = hidden, nil
		}
	}
	h.storeState(h.deletion(0, "1"), h.deletion(0, "2"))
	got := h.stats("people")
	require.Equal(t, 1, got.deleted)
	require.Equal(t, 1, got.rejected)
	require.Equal(t, map[string]int{"link_dropped_record_deleted": 1}, got.skipped)
	require.Len(t, h.tx.bindings[0].waiting, 1)
	require.Contains(t, h.tx.bindings[0].waiting, linkSlot{"2", "companyId"})
}

func TestStoreDeleteKeySwap(t *testing.T) {
	h := newHarness(t, companies(`,"delete_mode":"delete"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"a","name":"A"}`), h.doc(0, `{"id":"b","name":"B"}`)))
	a, b := h.only("companies", "a")["id"].(string), h.only("companies", "b")["id"].(string)
	// Between the lookup and the delete, b's key changes to c, then a's to b.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodDelete {
			h.fake.rows["companies"][b]["externalId"] = "c"
			h.fake.rows["companies"][a]["externalId"] = "b"
		}
	}
	require.NoError(t, h.store(h.deletion(0, "a"), h.deletion(0, "b")))
	require.Equal(t, stats{received: 2, skipped: map[string]int{"deletion_changed_in_twenty": 2}}, h.stats("companies"))
	require.NotNil(t, h.fake.row("companies", a))
	require.NotNil(t, h.fake.row("companies", b))
}

func TestStoreDeleteDropsWaitingLinks(t *testing.T) {
	h := newHarness(t, people(linkCompany+`,"delete_mode":"delete"`), companies(""))
	h.clock()
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(0, `{"id":2,"first":"Raj","company":"k1"}`)))
	require.Len(t, h.tx.bindings[0].waiting, 2)
	st := h.storeState(h.deletion(0, "1"))
	require.Equal(t, map[string]int{"link_dropped_record_deleted": 1}, h.stats("people").skipped)
	require.JSONEq(t, `{"waitingLinks":{"people/externalId":[
		{"key":"2","column":"companyId","target":"k1","lookup":"companies.externalId","since":"2026-11-02T12:00:00Z","tried":"2026-11-02T12:00:00Z"}]}}`, string(st))

	// Trashed, with restore_deleted on: the dropped link can't bring the record back.
	h2 := newHarness(t, people(linkCompany+`,"delete_mode":"trash","restore_deleted":true`), companies(""))
	h2.tx.now = func() time.Time { return h.now }
	require.NotNil(t, h2.storeState(h2.doc(0, `{"id":1,"first":"Jane","company":"k1"}`)))
	require.JSONEq(t, `{}`, string(h2.storeState(h2.deletion(0, "1"))))
	h2.fake.seed("companies", record{"externalId": "k1"})
	h.now = h.now.Add(linkRetryEvery)
	h2.tx.retryWaiting(t.Context())
	require.NotNil(t, h2.only("people", "1")["deletedAt"])
	require.Nil(t, h2.only("people", "1")["companyId"])

	// In one transaction too: a link the deleted key's document set doesn't wait.
	st = h.storeState(h.doc(0, `{"id":3,"first":"Ana","company":"k2"}`), h.deletion(0, "3"))
	require.Nil(t, st)
	require.Len(t, h.tx.bindings[0].waiting, 1)
}

func TestStoreFlagsLongKeys(t *testing.T) {
	h := newHarness(t, companies(flagDeletes))
	var longest int
	h.fake.hook = func(r *http.Request) { longest = max(longest, len(r.RequestURI)) }
	var items []storeItem
	for i := range maxBatch {
		key := fmt.Sprintf("k%02d-%s", i, strings.Repeat("x", 500))
		h.fake.seed("companies", record{"externalId": key, "externalDeleted": false})
		items = append(items, h.deletion(0, key))
	}
	require.NoError(t, h.store(items...))
	require.Equal(t, stats{received: maxBatch, flagged: maxBatch}, h.stats("companies"))
	require.LessOrEqual(t, longest, 7000, "each request's URL fits the 8 KB that proxies like nginx allow")
}

func TestStoreDeleteURLs(t *testing.T) {
	for _, mode := range []string{deleteFlag, deleteTrash, deleteDelete} {
		for _, keyLen := range []int{1, 500} {
			res := `,"delete_mode":"` + mode + `"`
			if mode == deleteFlag {
				res = flagDeletes
			}
			h := newHarness(t, companies(res))
			var longest int
			h.fake.hook = func(r *http.Request) { longest = max(longest, len(r.RequestURI)) }
			var items []storeItem
			for i := range maxBatch {
				key := fmt.Sprintf("%02d%s", i, strings.Repeat("x", keyLen))
				h.fake.seed("companies", record{"externalId": key, "externalDeleted": false})
				items = append(items, h.deletion(0, key))
			}
			require.NoError(t, h.store(items...))
			got := h.stats("companies")
			require.Equal(t, maxBatch, got.flagged+got.deleted, "%s, %d-byte keys", mode, keyLen)
			require.LessOrEqual(t, longest, 7000, "%s, %d-byte keys: each URL fits the 8 KB that proxies like nginx allow", mode, keyLen)
		}
	}
}

func TestStoreDeletionRace(t *testing.T) {
	h := newHarness(t, companies(flagDeletes))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	id := h.only("companies", "k1")["id"].(string)

	// Someone changes the record's key between the connector's lookup and its update.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPatch {
			h.fake.rows["companies"][id]["externalId"] = "k1-renamed"
		}
	}
	require.NoError(t, h.store(h.deletion(0, "k1")))
	require.Equal(t, stats{received: 1, skipped: map[string]int{"deletion_changed_in_twenty": 1}}, h.stats("companies"))
	require.Equal(t, false, h.fake.row("companies", id)["externalDeleted"])
}

func TestStoreRecordsDeletedInTwenty(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(fmt.Sprintf("restore_deleted=%t", restore), func(t *testing.T) {
			h := newHarness(t, companies(fmt.Sprintf(`,"restore_deleted":%t`, restore)))
			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
			id := h.only("companies", "k1")["id"].(string)
			h.fake.softDelete("companies", id)

			require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme 2"}`)))
			row := h.fake.row("companies", id)
			if restore {
				require.Equal(t, stats{received: 1, updated: 1}, h.stats("companies"))
				require.Nil(t, row["deletedAt"])
				require.Equal(t, "Acme 2", row["name"])
			} else {
				require.Equal(t, stats{received: 1, skipped: map[string]int{"deleted_in_twenty": 1}}, h.stats("companies"))
				require.NotNil(t, row["deletedAt"])
				require.Equal(t, "Acme", row["name"])
			}
			require.Equal(t, 1, h.fake.rowCount("companies"))
		})
	}
}

func TestStoreUniqueConflicts(t *testing.T) {
	h := newHarness(t, companies(""))
	other := h.fake.seed("companies", record{"externalId": "other", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	manual := h.fake.seed("companies", record{"name": "Made in Twenty", "domainName": map[string]any{"primaryLinkUrl": "beta.com"}})
	old := h.fake.seed("companies", record{"externalId": "old", "accountCode": "A-9"})
	h.fake.softDelete("companies", old["id"].(string))

	require.NoError(t, h.store(
		h.doc(0, `{"id":"k1","name":"Acme","domain":"https://www.ACME.com/about"}`),
		h.doc(0, `{"id":"k2","name":"Beta","domain":"beta.com"}`),
		h.doc(0, `{"id":"k3","name":"Gamma","domain":"gamma.com","code":"A-9"}`),
		h.doc(0, `{"id":"k4","name":"Gamma 2","domain":"GAMMA.com"}`),
	))
	require.Equal(t, stats{received: 4, created: 4, skipped: map[string]int{
		"domainName_held_by_another_record": 3, "accountCode_held_by_another_record": 1}}, h.stats("companies"))

	require.Nil(t, fakeGet(h.only("companies", "k1"), "domainName.primaryLinkUrl"))
	require.Nil(t, fakeGet(h.only("companies", "k2"), "domainName.primaryLinkUrl"))
	require.Equal(t, "gamma.com", fakeGet(h.only("companies", "k3"), "domainName.primaryLinkUrl"))
	require.Equal(t, "", h.only("companies", "k3")["accountCode"])
	require.Nil(t, fakeGet(h.only("companies", "k4"), "domainName.primaryLinkUrl"))
	require.Equal(t, other, h.fake.row("companies", other["id"].(string)), "the other record is untouched")
	require.Equal(t, manual, h.fake.row("companies", manual["id"].(string)))

	// A record keeps a unique value it already holds.
	require.NoError(t, h.store(h.doc(0, `{"id":"k3","name":"Gamma","domain":"gamma.com"}`)))
	require.Equal(t, stats{received: 1, updated: 1}, h.stats("companies"))
	require.Equal(t, "gamma.com", fakeGet(h.only("companies", "k3"), "domainName.primaryLinkUrl"))
}

func TestStoreUniqueConflictsFail(t *testing.T) {
	h := newHarness(t, companies(`,"unique_conflict":"fail"`))
	h.fake.seed("companies", record{"externalId": "other", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	err := h.store(h.doc(0, `{"id":"k1","domain":"acme.com"}`))
	require.ErrorContains(t, err, `the domainName value of the document with key "k1" belongs to another record`)
	// The value waits for the end of the transaction, so the record is written without it first.
	require.Nil(t, fakeGet(h.only("companies", "k1"), "domainName.primaryLinkUrl"))
}

func TestStoreInvalidValues(t *testing.T) {
	h := newHarness(t, people(`,"rejected_records":"skip"`))
	require.NoError(t, h.store(
		h.doc(0, `{"id":1,"first":"Jane","email":"jane.doe@example","title":"CTO"}`),
		h.doc(0, `{"id":2,"first":"Raj","phones":{"primaryPhoneNumber":"call me","primaryPhoneCountryCode":"SG"}}`),
		h.doc(0, `{"id":3,"first":"Ok","last":"Person","email":" OK@Example.com"}`),
	))
	require.Equal(t, stats{received: 3, created: 3, invalid: map[string]int{"emails": 1, "phones": 1}}, h.stats("people"))

	jane := h.only("people", "1")
	require.Nil(t, fakeGet(jane, "emails.primaryEmail"))
	require.Equal(t, "Jane", fakeGet(jane, "name.firstName"))
	require.Equal(t, "CTO", jane["jobTitle"])
	require.Nil(t, fakeGet(h.only("people", "2"), "phones.primaryPhoneNumber"))
	ok := h.only("people", "3")
	require.Equal(t, "ok@example.com", fakeGet(ok, "emails.primaryEmail"))
	require.Equal(t, "Person", fakeGet(ok, "name.lastName"))
}

func TestStoreLeavesOutABadPhoneAcrossABatch(t *testing.T) {
	h := newHarness(t, people(`,"rejected_records":"skip"`))
	var items []storeItem
	for i := 1; i <= 30; i++ {
		phone := `"+65 9123 4567"`
		if i%3 == 0 {
			phone = `"555"` // no country, so the fake calls it invalid
		}
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":%d,"first":"P%d","phones":{"primaryPhoneNumber":%s}}`, i, i, phone)))
	}
	require.NoError(t, h.store(items...))
	require.Equal(t, stats{received: 30, created: 30, invalid: map[string]int{"phones": 10}}, h.stats("people"))
	require.Nil(t, fakeGet(h.only("people", "3"), "phones.primaryPhoneNumber"))
	require.Equal(t, "+65 9123 4567", fakeGet(h.only("people", "4"), "phones.primaryPhoneNumber"))
	// One refused batch and one that goes through, rather than a split down to each bad record.
	require.Equal(t, 2, h.fake.count("POST", "/rest/batch/people"))

	// Updates are split as before, as one may lean on country codes Twenty already holds.
	before := h.fake.count("POST", "/rest/batch/people")
	items = nil
	for i := 1; i <= 4; i++ {
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":%d,"first":"P%d","phones":{"primaryPhoneNumber":"555"}}`, i, i)))
	}
	require.NoError(t, h.store(items...))
	require.Equal(t, map[string]int{"phones": 4}, h.stats("people").invalid)
	require.Greater(t, h.fake.count("POST", "/rest/batch/people")-before, 2)

	// The same number with different codes may be valid in one record, so the batch is split.
	h = newHarness(t, people(`,"rejected_records":"skip"`))
	require.NoError(t, h.store(
		h.doc(0, `{"id":1,"first":"A","phones":{"primaryPhoneNumber":"9123 4567"}}`),
		h.doc(0, `{"id":2,"first":"B","phones":{"primaryPhoneNumber":"9123 4567","primaryPhoneCountryCode":"SG"}}`),
	))
	require.Equal(t, stats{received: 2, created: 2, invalid: map[string]int{"phones": 1}}, h.stats("people"))
	require.Nil(t, fakeGet(h.only("people", "1"), "phones.primaryPhoneNumber"))
	require.Equal(t, "9123 4567", fakeGet(h.only("people", "2"), "phones.primaryPhoneNumber"))

	// A record holding the number as an additional phone may be the one Twenty means.
	rej := &rejectedError{raw: []string{"Provided phone number is invalid 555"}}
	held := []pending{
		{key: "3", rec: record{"phones": map[string]any{"primaryPhoneNumber": "555", "primaryPhoneCountryCode": "SG"}}},
		{key: "4", rec: record{"phones": map[string]any{"primaryPhoneNumber": "+65 9123 4567", "additionalPhones": []any{map[string]any{"number": "555"}}}}},
	}
	var s stats
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	require.NotNil(t, held[0].rec["phones"])
	held[1].rec["phones"].(map[string]any)["additionalPhones"] = []any{}
	require.True(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	require.Nil(t, held[0].rec["phones"])
	require.Equal(t, map[string]int{"phones": 1}, s.invalid)

	// An additional phone written as a number counts too.
	held = []pending{
		{key: "6", rec: record{"phones": map[string]any{"primaryPhoneNumber": "555"}}},
		{key: "7", rec: record{"other": map[string]any{"additionalPhones": []any{map[string]any{"number": json.Number("555")}}}}},
	}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	held[1].rec["other"] = map[string]any{"additionalPhones": json.RawMessage(`[{"number":"555"}]`)}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	held[1].rec["other"] = map[string]any{"additionalPhones": json.RawMessage(`[{"number":"\u0035\u00355"}]`)}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	held[1].rec["other"] = map[string]any{"additionalPhones": json.RawMessage(`[{"number":"556"}]`)}
	require.True(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))

	// A number written as JSON may be the one Twenty means.
	held = []pending{
		{key: "8", rec: record{"phones": map[string]any{"primaryPhoneNumber": "555", "primaryPhoneCountryCode": "SG"}}},
		{key: "9", rec: record{"phones": map[string]any{"primaryPhoneNumber": json.Number("555"), "primaryPhoneCountryCode": "SG"}}},
	}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	require.NotNil(t, held[0].rec["phones"])
	held[1].rec["phones"].(map[string]any)["primaryPhoneNumber"] = json.Number("555.0")
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	held[1].rec["phones"] = map[string]any{"additionalPhones": json.RawMessage(`[{"number":5.55e2}]`)}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	require.NotNil(t, held[0].rec["phones"])

	// A cleared phone is no number, even when Twenty's message names "<nil>".
	rej = &rejectedError{raw: []string{"Provided phone number is invalid <nil>"}}
	held = []pending{{key: "5", rec: record{"phones": map[string]any{"primaryPhoneNumber": nil}}}}
	require.False(t, leaveOutPhone(&binding{object: "people"}, rej, held, &s))
	require.NotNil(t, held[0].rec["phones"])
}

func TestStoreRejectionsRedacted(t *testing.T) {
	h := newHarness(t, people(""))
	err := h.store(h.doc(0, `{"id":1,"first":"Jane","email":"jane.doe@example"}`))
	require.ErrorContains(t, err, `people: can't write the document with key "1"`)
	require.ErrorContains(t, err, "<email>")
	require.NotContains(t, err.Error(), "jane.doe")

	err = h.store(h.doc(0, `{"id":2,"first":"Raj","phones":{"primaryPhoneNumber":"9123 4567"}}`))
	require.ErrorContains(t, err, "Provided phone number is invalid <phone>")
	require.NotContains(t, err.Error(), "9123")
}

func TestStoreSplitsRejectedBatches(t *testing.T) {
	docs := func(h *harness) []storeItem {
		var items []storeItem
		for i := 1; i <= 5; i++ {
			items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%d","name":"Company %d"}`, i, i)))
		}
		return items
	}
	reject := func(_ string, rec record) string {
		if rec["externalId"] == "k3" {
			return "Something else is wrong"
		}
		return ""
	}

	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = reject
	require.NoError(t, h.store(docs(h)...))
	require.Equal(t, stats{received: 5, created: 4, rejected: 1}, h.stats("companies"))
	require.Equal(t, []int{5, 2, 3, 1, 2}, h.fake.batches)
	require.Empty(t, h.fake.byKey("companies", "k3"))
	require.Equal(t, 4, h.fake.rowCount("companies"))

	h = newHarness(t, companies(""))
	h.fake.reject = reject
	err := h.store(docs(h)...)
	require.ErrorContains(t, err, `companies: can't write the document with key "k3": Twenty rejected the records (HTTP 400): Something else is wrong`)
	require.Equal(t, []int{5, 2, 3, 1}, h.fake.batches)
}

func TestStoreSkipLimit(t *testing.T) {
	bad := func(_ string, rec record) string {
		if strings.HasSuffix(fmt.Sprint(rec["name"]), "!") {
			return "Bad"
		}
		return ""
	}

	// 11 of 30 is more than 10 and 10%.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = bad
	var items []storeItem
	for i := range 30 {
		name := fmt.Sprintf("Company %d", i)
		if i < 11 {
			name += "!"
		}
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%02d","name":%q}`, i, name)))
	}
	require.ErrorContains(t, h.store(items...), "companies: skipped 11 of 30 records, more than 10 and 10%")

	// Every 10th of 200 is 10% throughout, which doesn't stop the task.
	h = newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = bad
	items = nil
	for i := 1; i <= 200; i++ {
		name := fmt.Sprintf("Company %d", i)
		if i%10 == 0 {
			name += "!"
		}
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%03d","name":%q}`, i, name)))
	}
	require.NoError(t, h.store(items...))
	require.Equal(t, stats{received: 200, created: 180, rejected: 20}, h.stats("companies"))
}

func TestStoreChunks(t *testing.T) {
	h := newHarness(t, companies(""), people(""))
	var items []storeItem
	for i := range 70 {
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%02d","name":"Company %d"}`, i, i)))
	}
	items = append(items, h.doc(1, `{"id":7,"first":"Jane"}`))
	require.NoError(t, h.store(items...))
	require.Equal(t, stats{received: 70, created: 70}, h.stats("companies"))
	require.Equal(t, stats{received: 1, created: 1}, h.stats("people"))
	require.Equal(t, []int{60, 10, 1}, h.fake.batches)
	require.Equal(t, 70, h.fake.rowCount("companies"))
}

func TestStoreRetries(t *testing.T) {
	h := newHarness(t, companies(""))
	h.fake.failNext("/rest/batch/companies", 429, http.Header{"Retry-After": {"2"}}, messagesBody("Too many requests"))
	h.fake.failNext("/rest/batch/companies", 503, nil, messagesBody("Unavailable"))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.Equal(t, stats{received: 1, created: 1}, h.stats("companies"))
	require.Equal(t, 3, h.fake.count(http.MethodPost, "/rest/batch/companies"))
	require.Equal(t, 2*time.Second, h.fake.slept[0], "the client waits as long as Retry-After says")

	h = newHarness(t, companies(""))
	for range maxAttempts {
		h.fake.failNext("/rest/batch/companies", 500, nil, messagesBody("Internal error"))
	}
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)),
		"POST /rest/batch/companies failed 8 times; last: HTTP 500: Internal error")

	// A 429 without Retry-After waits out Twenty's per-minute window.
	h = newHarness(t, companies(""))
	h.fake.failNext("/rest/batch/companies", 429, nil, messagesBody("Too many requests"))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.GreaterOrEqual(t, h.fake.slept[0], rateWindow/2)

	// Postgres refusing a value is the record's fault: no retries, and skip skips it.
	h = newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.failNext("/rest/batch/companies", 500, nil, messagesBody("Data validation error."))
	h.fake.failNext("/rest/batch/companies", 500, nil, messagesBody("Data validation error."))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.Equal(t, 1, h.stats("companies").rejected)
	require.Empty(t, h.fake.slept)

	// A revoked key stops the task at once.
	h = newHarness(t, companies(""))
	h.fake.failNext("/rest/companies", 401, nil, messagesBody("Token invalid."))
	var auth *authError
	require.ErrorAs(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)), &auth)
	require.Equal(t, 1, h.fake.count(http.MethodGet, "/rest/companies"))
}

func TestStoreBadKeys(t *testing.T) {
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	require.NoError(t, h.store(h.doc(0, `{"id":"say \"hi\"","name":"Quoted"}`), h.doc(0, `{"id":"k1","name":"Acme"}`)))
	require.Equal(t, stats{received: 2, created: 1, rejected: 1}, h.stats("companies"))
	require.Equal(t, 1, h.fake.rowCount("companies"))

	// So is a key too long to look up.
	require.NoError(t, h.store(h.doc(0, fmt.Sprintf(`{"id":%q,"name":"Long"}`, strings.Repeat("k", maxValueBytes+1)))))
	require.Equal(t, stats{received: 1, rejected: 1}, h.stats("companies"))
	require.Equal(t, 1, h.fake.rowCount("companies"))
}

// --- helpers -------------------------------------------------------------------------------------

func TestFilters(t *testing.T) {
	got, err := filterIn("externalId", []string{"a b", "ü", "x,y"})
	require.NoError(t, err)
	require.Equal(t, `externalId[in]:["a b","ü","x,y"]`, got)
	n, err := parseFilter(got)
	require.NoError(t, err, "the fake parses what the client writes")
	require.Equal(t, []any{"a b", "ü", "x,y"}, n.value)

	for _, bad := range []string{"", `say "hi"`, `back\slash`, "tab\there"} {
		require.False(t, filterable(bad), bad)
		_, err := filterIn("externalId", []string{"ok", bad})
		require.Error(t, err, bad)
	}
	require.Equal(t, "a%20b%2Bc%5B%5D", escape("a b+c[]"))

	// A value too long for a URL can't go in either, counted as escaped.
	require.True(t, filterable(strings.Repeat("x", maxValueBytes)))
	require.False(t, filterable(strings.Repeat("x", maxValueBytes+1)))
	require.True(t, filterable(strings.Repeat("ü", maxValueBytes/6)))
	require.False(t, filterable(strings.Repeat("ü", maxValueBytes/6+1)))
	require.Equal(t, len(`%22a%20b%22%2C`), inFilterBytes("a b"))
}

func TestSizedBatches(t *testing.T) {
	sizes := map[string]int{"a": 4000, "b": 2000, "c": 1, "d": maxFilterBytes + 1}
	size := func(v string) int { return sizes[v] }
	require.Equal(t, [][]string{{"a", "b"}, {"c"}, {"d"}}, sizedBatches([]string{"a", "b", "c", "d"}, size),
		"a group fills up to maxFilterBytes, and a value over it goes alone")

	many := make([]string, maxBatch+1)
	for i := range many {
		many[i] = "c"
	}
	groups := sizedBatches(many, size)
	require.Len(t, groups, 2)
	require.Len(t, groups[0], maxBatch)
}

// brokenTransport fails every request, as a dropped connection does.
type brokenTransport struct{}

func (brokenTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, io.ErrUnexpectedEOF
}

func TestRequestErrorsMasked(t *testing.T) {
	// A network error names the URL, whose filter holds the values looked up.
	c := newClient("https://twenty.example.com", fakeAPIKey, 60000, 1000)
	c.sleep = func(context.Context, time.Duration) error { return nil }
	c.http.Transport = brokenTransport{}
	logs.Reset()
	_, err := c.find(t.Context(), "people", "emails.primaryEmail", []string{"jane.doe@example.com"}, true)
	require.ErrorContains(t, err, "GET /rest/people failed 8 times")
	require.NotContains(t, err.Error(), "jane.doe")
	require.Len(t, logs.AllEntries(), maxAttempts-1, "a retry is logged")
	for _, e := range logs.AllEntries() {
		require.NotContains(t, fmt.Sprint(e.Data), "jane.doe")
	}

	// Twenty's own messages can repeat an email.
	f, c := newFakeTwenty(t)
	for range maxAttempts {
		f.failNext("/rest/people", 503, nil, messagesBody("Looking up jane.doe@example.com timed out"))
	}
	_, err = c.find(t.Context(), "people", "emails.primaryEmail", []string{"jane.doe@example.com"}, true)
	require.ErrorContains(t, err, "HTTP 503: Looking up <email> timed out")
	f.failNext("/rest/people", 400, nil, messagesBody("Bad value jane.doe@example.com"))
	_, err = c.find(t.Context(), "people", "emails.primaryEmail", []string{"jane.doe@example.com"}, true)
	require.ErrorContains(t, err, "Bad value <email>")

	// rejectedFields still matches the value Twenty names, which the error masks.
	f.failNext("/rest/people", 400, nil, messagesBody("Provided phone number is invalid wrong@example.com"))
	_, err = c.find(t.Context(), "people", "emails.primaryEmail", []string{"jane.doe@example.com"}, true)
	var rej *rejectedError
	require.ErrorAs(t, err, &rej)
	require.NotContains(t, rej.Error(), "wrong@")
	rec := record{"workPhone": map[string]any{"primaryPhoneNumber": "wrong@example.com"},
		"mobilePhone": map[string]any{"primaryPhoneNumber": "+14155550123"}}
	require.Equal(t, []string{"workPhone"}, rejectedFields(rej, rec))

	// A redirect's address repeats the lookup's filter.
	f.failNext("/rest/people", 302, http.Header{"Location": {`https://other.example.com/rest/people?filter=emails.primaryEmail[in]:["jane.doe@example.com"]`}}, nil)
	_, err = c.find(t.Context(), "people", "emails.primaryEmail", []string{"jane.doe@example.com"}, true)
	require.ErrorContains(t, err, `redirect to "https://other.example.com/rest/people"`)
	require.NotContains(t, err.Error(), "jane.doe")
}

func TestRetryAfter(t *testing.T) {
	d, ok := retryAfter(http.Header{"Retry-After": {"1.5"}})
	require.True(t, ok)
	require.Equal(t, 1500*time.Millisecond, d)

	d, ok = retryAfter(http.Header{"Retry-After": {time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)}})
	require.True(t, ok)
	require.InDelta(t, 10*time.Second, d, float64(2*time.Second))

	d, ok = retryAfter(http.Header{"Retry-After": {"-3"}})
	require.True(t, ok)
	require.Zero(t, d)

	_, ok = retryAfter(http.Header{"Retry-After": {"soon"}})
	require.False(t, ok)
	_, ok = retryAfter(http.Header{})
	require.False(t, ok)
}

func TestMessages(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, messages([]byte(`{"messages":["a","b"]}`), false))
	require.Equal(t, []string{"one"}, messages([]byte(`{"statusCode":400,"message":"one","error":"Bad Request"}`), false))
	require.Equal(t, []string{"Bad Request"}, messages([]byte(`{"error":"Bad Request"}`), false))
	require.Equal(t, []string{"<html>oops</html>"}, messages([]byte(`<html>oops</html>`), false))
	require.Nil(t, messages(nil, false))
	// Nested "errors" are read only from the metadata API, so a record error keeps its one message.
	nested := []byte(`{"message":"top","errors":{"fieldMetadata":[{"errors":[{"message":"inner"}]}]}}`)
	require.Equal(t, []string{"top"}, messages(nested, false))
	require.Equal(t, []string{"top", "inner"}, messages(nested, true))
}

func TestMapKind(t *testing.T) {
	for _, tc := range []struct {
		types       []string
		format      string
		fieldType   string
		sub         string
		items       []string // an array's item types
		want        kind
		wantProblem string
	}{
		{types: []string{"string"}, fieldType: "TEXT", want: kindText},
		{types: []string{"null", "integer"}, fieldType: "TEXT", want: kindText},
		{types: []string{"boolean"}, fieldType: "TEXT", want: kindText},
		{types: []string{"object"}, fieldType: "TEXT", wantProblem: "can't hold object values"},
		{types: []string{"integer", "number"}, fieldType: "NUMBER", want: kindNumber},
		{types: []string{"string"}, format: "number", fieldType: "NUMBER", want: kindNumber},
		{types: []string{"string"}, fieldType: "NUMBER", wantProblem: "can't hold string values"},
		{types: []string{"string"}, format: "date-time", fieldType: "DATE_TIME", want: kindDateTime},
		{types: []string{"string"}, fieldType: "DATE_TIME", wantProblem: "can't hold string values"},
		{types: []string{"string"}, format: "date", fieldType: "DATE", want: kindString},
		{types: []string{"string"}, fieldType: "SELECT", want: kindString},
		{types: []string{"array"}, fieldType: "MULTI_SELECT", want: kindJSON},
		{types: []string{"string"}, fieldType: "LINKS", want: kindPrimary},
		{types: []string{"object"}, fieldType: "LINKS", want: kindJSON},
		{types: []string{"object"}, fieldType: "ADDRESS", want: kindJSON},
		{types: []string{"string"}, fieldType: "ADDRESS", wantProblem: "can't hold string values"},
		{types: []string{"string"}, fieldType: "FULL_NAME", sub: "firstName", want: kindString},
		{types: []string{"string"}, fieldType: "FULL_NAME", sub: "middleName", wantProblem: `has no subfield "middleName"`},
		{types: []string{"integer"}, fieldType: "CURRENCY", sub: "amountMicros", want: kindInteger},
		{types: []string{"string"}, format: "integer", fieldType: "CURRENCY", sub: "amountMicros", want: kindInteger},
		{types: []string{"number"}, fieldType: "CURRENCY", sub: "amountMicros", wantProblem: "can't hold number values"},
		{types: []string{"number"}, fieldType: "CURRENCY", sub: "amount", want: kindAmount},
		{types: []string{"integer"}, fieldType: "CURRENCY", sub: "amount", want: kindAmount},
		{types: []string{"string"}, format: "number", fieldType: "CURRENCY", sub: "amount", want: kindAmount},
		{types: []string{"string"}, fieldType: "CURRENCY", sub: "amount", wantProblem: "can't hold string values"},
		{types: []string{"number"}, fieldType: "NUMBER", sub: "amount", wantProblem: "has no subfields"},
		{types: []string{"number"}, fieldType: "LINKS", sub: "amount", wantProblem: `has no subfield "amount"`},
		{types: []string{"string"}, fieldType: "TEXT", sub: "x", wantProblem: "has no subfields"},
		{types: []string{"array"}, fieldType: "EMAILS", sub: "additionalEmails", want: kindJSON},
		{types: []string{"string"}, fieldType: "RATING", want: kindString},
		{types: []string{"integer"}, fieldType: "RATING", wantProblem: "takes the strings RATING_1 to RATING_5, not integer values"},
		{types: []string{"array"}, fieldType: "ARRAY", items: []string{"null", "string"}, want: kindJSON},
		{types: []string{"array"}, fieldType: "ARRAY", items: []string{"integer", "string"}, wantProblem: "holds strings, and this array can hold integer or string items"},
		{types: []string{"array"}, fieldType: "MULTI_SELECT", items: []string{"number"}, wantProblem: "holds strings, and this array can hold number items"},
		{types: []string{"string"}, fieldType: "MULTI_SELECT", wantProblem: "can't hold string values"},
		{types: []string{"null", "string"}, fieldType: "RICH_TEXT", want: kindPrimary},
		{types: []string{"object"}, fieldType: "RICH_TEXT", want: kindJSON},
		{types: []string{"integer"}, fieldType: "RICH_TEXT", wantProblem: "can't hold integer values"},
		{types: []string{"string"}, fieldType: "RICH_TEXT", sub: "markdown", want: kindString},
		{types: []string{"string"}, fieldType: "RICH_TEXT", sub: "blocknote", wantProblem: `has no subfield "blocknote"`},
	} {
		p := proj("f", "/f", tc.types...)
		if tc.format != "" {
			p.Inference.String_ = &pf.Inference_String{Format: tc.format}
		}
		if tc.items != nil {
			p.Inference.Array = &pf.Inference_Array{ItemTypes: tc.items}
		}
		got, problem := mapKind(&p, fieldMeta{Name: "f", Type: tc.fieldType}, tc.sub)
		name := fmt.Sprintf("%v %s %s.%s", tc.types, tc.format, tc.fieldType, tc.sub)
		if tc.wantProblem != "" {
			require.Contains(t, problem, tc.wantProblem, name)
		} else {
			require.Empty(t, problem, name)
			require.Equal(t, tc.want, got, name)
		}
	}
}

func TestConvert(t *testing.T) {
	for _, tc := range []struct {
		k       kind
		in      any
		want    any
		wantErr string
	}{
		{k: kindDateTime, in: "2026-10-28T09:30:00+08:00", want: "2026-10-28T01:30:00Z"},
		{k: kindDateTime, in: "2026-10-28T01:30:00.123456Z", want: "2026-10-28T01:30:00.123456Z"},
		{k: kindDateTime, in: "yesterday", wantErr: "is not an RFC 3339 date-time"},
		{k: kindDateTime, in: "0001-01-01T00:00:00Z", wantErr: "is before the year 100"},
		{k: kindDateTime, in: "0100-01-01T00:30:00+01:00", wantErr: "is before the year 100"},
		{k: kindDateTime, in: "0100-01-01T00:00:00Z", want: "0100-01-01T00:00:00Z"},
		{k: kindNumber, in: "12.50", want: json.Number("12.50")},
		{k: kindNumber, in: " 7 ", want: json.Number("7")},
		{k: kindNumber, in: "1e3", want: json.Number("1e3")},
		{k: kindNumber, in: "0x10", wantErr: "is not a number"},
		{k: kindNumber, in: "NaN", wantErr: "is not a number"},
		{k: kindNumber, in: math.Inf(1), wantErr: "unexpected value"},
		{k: kindNumber, in: 2.5, want: 2.5},
		{k: kindInteger, in: "12", want: json.Number("12")},
		{k: kindInteger, in: "12.5", wantErr: "is not a whole number"},
		{k: kindInteger, in: 2.5, wantErr: "unexpected value"},
		{k: kindText, in: 1.5, want: "1.5"},
		{k: kindText, in: 2500000.0, want: "2500000"},
		{k: kindText, in: 0.00001, want: "0.00001"},
		{k: kindText, in: int64(7), want: "7"},
		{k: kindText, in: uint64(math.MaxUint64), want: "18446744073709551615"},
		{k: kindText, in: true, want: "true"},
		{k: kindBool, in: "true", wantErr: "unexpected value"},
		{k: kindJSON, in: []byte(`{"a":1}`), want: json.RawMessage(`{"a":1}`)},
		{k: kindString, in: nil, want: nil},
		{k: kindNumber, in: int64(1 << 53), want: int64(1 << 53)},
		{k: kindNumber, in: int64(1<<53 + 1), wantErr: "too large"},
		{k: kindNumber, in: int64(-(1<<53 + 1)), wantErr: "too large"},
		{k: kindNumber, in: uint64(1<<53 + 1), wantErr: "too large"},
		{k: kindNumber, in: "9007199254740993", wantErr: "too large"},
		{k: kindNumber, in: "99999999999999999999", wantErr: "too large"},
		{k: kindNumber, in: "9007199254740993.0", want: json.Number("9007199254740993.0")},
		{k: kindInteger, in: int64(1<<53 + 1), want: int64(1<<53 + 1)},
		{k: kindInteger, in: "9007199254740993", want: json.Number("9007199254740993")},
		{k: kindInteger, in: uint64(math.MaxInt64 + 1), wantErr: "out of range for a Twenty currency amount"},
		{k: kindInteger, in: "9223372036854775808", wantErr: "out of range for a Twenty currency amount"},
		{k: kindText, in: "a\x00b", want: "ab"},
		{k: kindString, in: "a\x00b", want: "ab"},
		{k: kindPrimary, in: "a\x00b.com", want: "ab.com"},
		{k: kindJSON, in: json.RawMessage(`{"a\u0000":["x\u0000y",1]}`), want: json.RawMessage(`{"a":["xy",1]}`)},
		{k: kindLink, in: "k1", want: "k1"},
		{k: kindAmount, in: 19.99, want: int64(19990000)},
		{k: kindAmount, in: int64(-3), want: int64(-3000000)},
		{k: kindAmount, in: uint64(5), want: int64(5000000)},
		{k: kindAmount, in: " 0.1234565 ", want: int64(123457)},
		{k: kindAmount, in: "-0.0000005", want: int64(-1)},
		{k: kindAmount, in: "0.00000049", want: int64(0)},
		{k: kindAmount, in: "1e-999999999", want: int64(0)},
		{k: kindAmount, in: "2.5e3", want: int64(2500000000)},
		{k: kindAmount, in: "9007199254.740992", want: int64(1 << 53)},
		{k: kindAmount, in: "9007199254.7409925", want: int64(1<<53 + 1)},
		{k: kindAmount, in: "-123456789012.34", want: int64(-123456789012340000)},
		{k: kindAmount, in: "9223372036854.775807", want: int64(math.MaxInt64)},
		{k: kindAmount, in: "9223372036854.7758075", wantErr: "out of range for a Twenty currency amount"},
		{k: kindAmount, in: "-9223372036854.775808", wantErr: "out of range for a Twenty currency amount"},
		{k: kindAmount, in: 1e13, wantErr: "out of range for a Twenty currency amount"},
		{k: kindAmount, in: "1e999999999", wantErr: "out of range for a Twenty currency amount"},
		{k: kindAmount, in: "12,50", wantErr: "is not a number"},
		{k: kindAmount, in: 1e300, wantErr: "out of range for a Twenty currency amount"},
		{k: kindAmount, in: -0.0000005, want: int64(-1)},
		{k: kindAmount, in: "7", want: int64(7000000)},
		{k: kindAmount, in: "-0", want: int64(0)},
		{k: kindAmount, in: math.Copysign(0, -1), want: int64(0)},
		// A float is rounded from its shortest decimal form, the number the document most likely
		// held, not from its exact binary value just below the half.
		{k: kindAmount, in: 1073741824.000000476837158203125, want: int64(1073741824000001)},
		{k: kindAmount, in: "1073741824.000000476837158203125", want: int64(1073741824000000)},
		{k: kindAmount, in: "1" + strings.Repeat("0", 100) + "e-100", wantErr: "too long for a currency amount"},
		{k: kindAmount, in: math.NaN(), wantErr: "unexpected value"},
		{k: kindAmount, in: true, wantErr: "unexpected value"},
		{k: kindLink, in: int64(-7), want: "-7"},
		{k: kindLink, in: uint64(math.MaxUint64), want: "18446744073709551615"},
		{k: kindLink, in: 1.5, wantErr: "unexpected value"},
		{k: kindLink, in: nil, want: nil},
	} {
		got, err := convert(tc.k, tc.in)
		name := fmt.Sprintf("%d %v", tc.k, tc.in)
		if tc.wantErr != "" {
			require.ErrorContains(t, err, tc.wantErr, name)
		} else {
			require.NoError(t, err, name)
			require.Equal(t, tc.want, got, name)
		}
	}
}

func TestNormalizeDomain(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"acme.com", "acme.com"},
		{"https://www.Acme.com/about?x=1#top", "acme.com"},
		{"http://user:pw@acme.com:8080/x", "acme.com"},
		{"  www.www.ACME.com  ", "acme.com"},
		{"München.de", "xn--mnchen-3ya.de"},
		{"sub.acme.co.uk", "sub.acme.co.uk"},
		{"ftp://acme.com", "ftp://acme.com"},
		{"not a domain", "not a domain"},
		{"acme.com.", "acme.com."},
		{"", ""},
	} {
		got := normalizeDomain(tc.in)
		require.Equal(t, tc.want, got, tc.in)
		// The unique check looks up what Twenty will store: what it makes of the raw value, and
		// of the value the connector sends.
		if stored, ok := twentyDomain(tc.in); ok {
			require.Equal(t, stored, got, tc.in)
		}
		if stored, ok := twentyDomain(got); ok {
			require.Equal(t, got, stored, tc.in)
		}
	}
	require.Equal(t, "ok@example.com", normalizeEmail(" OK@Example.COM "))
}

func TestRejectedFields(t *testing.T) {
	rec := record{
		"emails":       map[string]any{"primaryEmail": "jane.doe@example"},
		"phones":       map[string]any{"primaryPhoneNumber": "+65 9123 4567"},
		"workPhone":    map[string]any{"primaryPhoneNumber": "123"},
		"domainName":   map[string]any{"primaryLinkUrl": "x"},
		"linkedinLink": map[string]any{"primaryLinkUrl": "y"},
		"name":         "Jane",
	}
	for _, tc := range []struct {
		messages []string
		want     []string
	}{
		{[]string{`Invalid string value 'jane.doe@example' for email field "emails.primaryEmail"`}, []string{"emails"}},
		{[]string{"Provided phone number is invalid +65 9123 4567"}, []string{"phones"}},
		{[]string{"Provided phone number is invalid 999"}, []string{"phones", "workPhone"}},
		{[]string{"The URL of the link is not valid"}, []string{"domainName", "linkedinLink"}},
		{[]string{`"x" is not a domain name, for domain-typed links field "domainName.primaryLinkUrl"`}, []string{"domainName"}},
		{[]string{`Invalid value for field "name.firstName"`}, nil},
		{[]string{`field "emails.primaryEmail" is bad`}, nil},
		{[]string{"Provided phone number is invalid 999", "another"}, nil},
	} {
		require.Equal(t, tc.want, rejectedFields(&rejectedError{status: 400, messages: tc.messages, raw: tc.messages}, rec), tc.messages)
	}
}

func TestRedact(t *testing.T) {
	rec := record{
		"phones": map[string]any{"primaryPhoneNumber": "+65 9123 4567"},
		"fax":    map[string]any{"primaryPhoneNumber": "12"},
	}
	require.Equal(t, "Provided phone number is invalid <phone>; email <email> and '<email>'; 12 left",
		redact("Provided phone number is invalid +65 9123 4567; email jane@x.io and 'bob@y'; 12 left", rec))
	require.Len(t, redact(strings.Repeat("x", 400), nil), 300)
	require.Equal(t, strings.Repeat("x", 299), redact(strings.Repeat("x", 299)+"ü", nil), "never half a character")
}

func TestStoreCompoundKey(t *testing.T) {
	coll := collection("acmeCo/accounts", "string", proj("name", "/name", "string"))
	coll.Key = []string{"/region", "/id"}
	coll.Projections = append(coll.Projections, pf.Projection{Field: "region", Ptr: "/region", IsPrimaryKey: true,
		Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}})
	h := newHarness(t, testBinding{resource: `{"name":"acmeCo/accounts","object":"companies","id_field":"externalId"}`, collection: coll})
	require.Equal(t, []string{"region", "id"}, h.spec.Bindings[0].FieldSelection.Keys)
	// "eu/west" and "1" must not meet "eu" and "west/1".
	require.NoError(t, h.store(h.doc(0, `{"region":"eu/west","id":"1","name":"A"}`), h.doc(0, `{"region":"eu","id":"west/1","name":"B"}`)))
	require.Equal(t, "A", h.only("companies", "eu%2Fwest/1")["name"])
	require.Equal(t, "B", h.only("companies", "eu/west%2F1")["name"])
	require.NoError(t, h.store(h.doc(0, `{"region":"eu/west","id":"1","name":"A2"}`)))
	require.Equal(t, "A2", h.only("companies", "eu%2Fwest/1")["name"])

	// A deletion finds the record by the same text, and a relation links to it by that text.
	h = newHarness(t, testBinding{resource: `{"name":"acmeCo/accounts","object":"companies","id_field":"externalId","delete_mode":"delete"}`, collection: coll},
		people(linkCompany))
	require.NoError(t, h.store(h.doc(0, `{"region":"eu/west","id":"1","name":"A"}`), h.doc(0, `{"region":"eu","id":"2","name":"B"}`)))
	require.NoError(t, h.store(h.doc(1, `{"id":7,"first":"Jane","company":"eu%2Fwest/1"}`)))
	require.Equal(t, h.only("companies", "eu%2Fwest/1")["id"], h.only("people", "7")["companyId"])
	require.NoError(t, h.store(h.doc(0, `{"region":"eu","id":"2","_meta":{"op":"d"}}`)))
	require.Equal(t, stats{received: 1, deleted: 1}, h.stats("companies"))
	require.Len(t, h.fake.rows["companies"], 1)

	// The key's text must fit a Twenty filter: 3,000 bytes escaped in the URL.
	fits, _ := keyString(tuple.Tuple{strings.Repeat("é", 299), "aaaaaaa"})
	tooLong, _ := keyString(tuple.Tuple{strings.Repeat("é", 299), "aaaaaaaa"})
	require.True(t, filterable(fits))
	require.False(t, filterable(tooLong))
}

func TestStoreLinksByDomain(t *testing.T) {
	h := newHarness(t, people(`,"relations":[{"field":"company","id_field":"domainName"}]`), companies(""))
	h.clock()
	require.NoError(t, h.store(h.doc(1, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	acme := h.only("companies", "k1")["id"]
	// The key is matched as Twenty stores a domain: host only, lowercased, without "www.".
	// Two spellings of one domain in one transaction both link.
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"https://www.ACME.com/about"}`),
		h.doc(0, `{"id":5,"first":"Lee","company":"acme.com"}`)))
	require.Equal(t, []any{acme, acme}, []any{h.only("people", "1")["companyId"], h.only("people", "5")["companyId"]})
	// A domain no company holds yet waits under its own spelling, and links once one does.
	require.NoError(t, h.store(h.doc(0, `{"id":2,"first":"Raj","company":"https://www.BETA.io/x"}`)))
	require.Nil(t, h.only("people", "2")["companyId"])
	require.NoError(t, h.store(h.doc(1, `{"id":"k2","name":"Beta","domain":"beta.io"}`)))
	h.now = h.now.Add(linkRetryEvery)
	require.NoError(t, h.store())
	require.Equal(t, h.only("companies", "k2")["id"], h.only("people", "2")["companyId"])
	// A key that can't be a filter value is skipped rather than waiting.
	require.NoError(t, h.store(h.doc(0, `{"id":3,"first":"Ann","company":"a\"b.com"}`)))
	require.Equal(t, map[string]int{"company_target_key_unusable": 1}, h.stats("people").skipped)
	// So is a key that holds no domain.
	require.NoError(t, h.store(h.doc(0, `{"id":4,"first":"Bo","company":"Acme.com."}`)))
	require.Equal(t, map[string]int{"company_target_key_unusable": 1}, h.stats("people").skipped)
}

func TestLinkTargetValue(t *testing.T) {
	email := link{match: matchUnique, idMeta: fieldMeta{Type: "EMAILS"}}
	domain := link{match: matchUnique, idMeta: fieldMeta{Type: "LINKS"}}
	domain.idMeta.Settings.Type = "domain"
	url := link{match: matchUnique, idMeta: fieldMeta{Type: "LINKS"}}
	value := func(l link, key string) []any {
		v, ok := l.targetValue(key)
		return []any{v, ok}
	}
	require.Equal(t, []any{"jane@acme.com", true}, value(email, " Jane@ACME.com "))
	require.Equal(t, []any{"acme.com", true}, value(domain, "https://www.Acme.com/x"))
	require.Equal(t, []any{"https://Acme.com/X", true}, value(url, "https://Acme.com/X"))
	require.Equal(t, []any{" K1 ", true}, value(link{}, " K1 "))
	// A key that holds no domain is never looked up.
	for _, junk := range []string{"Acme.com.", "ftp://acme.com", "acme .com", "", "www."} {
		require.Equal(t, []any{"", false}, value(domain, junk), junk)
	}
	require.Equal(t, "<emails>", logKey(link{match: matchUnique, idField: "emails", idMeta: email.idMeta}, "jane@acme.com"))
	require.Equal(t, "acme.com", logKey(link{match: matchUnique, idField: "domainName", idMeta: domain.idMeta}, "acme.com"))
}

func TestKeyString(t *testing.T) {
	for _, tc := range []struct {
		key     tuple.Tuple
		want    string
		wantErr string
	}{
		{tuple.Tuple{"k1"}, "k1", ""},
		{tuple.Tuple{int64(-7)}, "-7", ""},
		{tuple.Tuple{uint64(math.MaxUint64)}, "18446744073709551615", ""},
		{tuple.Tuple{1.5}, "", "unexpected key of type float64"},
		{tuple.Tuple{"a/b c", int64(-7)}, "a%2Fb%20c/-7", ""},
		{tuple.Tuple{"a", "b/c"}, "a/b%2Fc", ""},
		{tuple.Tuple{"a/b", "c"}, "a%2Fb/c", ""},
		{tuple.Tuple{"Zé~_.-", uint64(9)}, "Z%C3%A9~_.-/9", ""},
		{tuple.Tuple{"", "%"}, "/%25", ""},
		{tuple.Tuple{"a/b"}, "a/b", ""},
		{tuple.Tuple{"a", 1.5}, "", "unexpected key of type float64"},
		{tuple.Tuple{}, "", "expected a key, got none"},
	} {
		got, err := keyString(tc.key)
		if tc.wantErr != "" {
			require.ErrorContains(t, err, tc.wantErr)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		}
	}
}

func TestPresent(t *testing.T) {
	var doc any
	require.NoError(t, decode([]byte(`{"a":{"b":null},"x/y":1,"t~":2,"arr":[0,1]}`), &doc))
	for ptr, want := range map[string]bool{
		"": true, "/a": true, "/a/b": true, "/a/c": false, "/x~1y": true, "/t~0": true,
		"/arr/1": true, "/arr/2": false, "/arr/-1": false, "/a/b/c": false,
	} {
		require.Equal(t, want, present(doc, ptr), ptr)
	}
}

func TestIsDeletion(t *testing.T) {
	for _, c := range []struct {
		flag bool
		doc  string
		want bool
	}{
		{false, `{"id":1,"_meta":{"op":"d"}}`, true},
		{false, `{"id":1,"_meta":{"op":"u"}}`, false},
		{false, `{"id":1}`, false},
		{true, `{"id":1}`, true},
	} {
		got, err := isDeletion(c.flag, json.RawMessage(c.doc))
		require.NoError(t, err)
		require.Equal(t, c.want, got, "%v %s", c.flag, c.doc)
	}
	_, err := isDeletion(false, json.RawMessage(`{`))
	require.Error(t, err)
}

func TestStoreTwentyQuirks(t *testing.T) {
	h := newHarness(t, companies(""), people(""))
	require.NoError(t, h.store(
		h.doc(0, `{"id":"k1","name":"Ac\u0000me","linkedin":"https://linkedin.com/company/acme"}`),
		h.doc(1, `{"id":1,"first":"Jane","phones":{"primaryPhoneNumber":"+14155550123","primaryPhoneCountryCode":"SG","primaryPhoneCallingCode":"+65"}}`),
	))
	require.Equal(t, "Acme", h.only("companies", "k1")["name"], "NUL characters are left out")
	require.Equal(t, "+14155550123", fakeGet(h.only("people", "1"), "phones.primaryPhoneNumber"), "a + number goes without the mapped codes")

	acme := h.only("companies", "k1")["id"].(string)
	h.fake.rows["companies"][acme]["linkedinLink"] = map[string]any{"primaryLinkUrl": "https://linkedin.com/company/acme",
		"primaryLinkLabel": "Acme", "secondaryLinks": []any{map[string]any{"url": "https://acme.com", "label": "Site"}}}
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","linkedin":null}`)))
	require.Equal(t, map[string]any{"primaryLinkUrl": "", "primaryLinkLabel": "", "secondaryLinks": []any{}},
		h.only("companies", "k1")["linkedinLink"], "a cleared link is cleared whole, not replaced by its secondary link")

	// An update to additional emails alone keeps the primary email, which Twenty would clear.
	require.NoError(t, h.store(h.doc(1, `{"id":1,"email":"jane@example.com"}`)))
	require.NoError(t, h.store(h.doc(1, `{"id":1,"moreEmails":["j@example.com"]}`)))
	require.Equal(t, "jane@example.com", fakeGet(h.only("people", "1"), "emails.primaryEmail"))
}

func TestStoreDefaultPhoneCountry(t *testing.T) {
	h := newHarness(t, people(""))
	h.tx.bindings[0].phoneCountry = "US"
	require.NoError(t, h.store(
		h.doc(0, `{"id":1,"phones":{"primaryPhoneNumber":"(415) 555-0101"}}`),
		h.doc(0, `{"id":2,"phones":{"primaryPhoneNumber":"0044 20 7946 0958"}}`),
		h.doc(0, `{"id":3,"phones":{"primaryPhoneNumber":"9123 4567","primaryPhoneCountryCode":"SG"}}`),
		h.doc(0, `{"id":4,"phones":{"primaryPhoneNumber":"+65 9123 4567"}}`),
	))
	require.Equal(t, "US", fakeGet(h.only("people", "1"), "phones.primaryPhoneCountryCode"), "a number without a country gets the default")
	require.Equal(t, "+44 20 7946 0958", fakeGet(h.only("people", "2"), "phones.primaryPhoneNumber"), "00 is read as +")
	require.Nil(t, fakeGet(h.only("people", "2"), "phones.primaryPhoneCountryCode"))
	require.Equal(t, "SG", fakeGet(h.only("people", "3"), "phones.primaryPhoneCountryCode"), "the document's own country wins")
	require.Nil(t, fakeGet(h.only("people", "4"), "phones.primaryPhoneCountryCode"), "a + number needs no country")

	// Without a default, such a number is still refused, as Twenty does.
	h.tx.bindings[0].phoneCountry = ""
	require.ErrorContains(t, h.store(h.doc(0, `{"id":5,"phones":{"primaryPhoneNumber":"(415) 555-0102"}}`)),
		"Provided phone number is invalid")
}

func TestConfigPhoneCountry(t *testing.T) {
	cfg := config{APIKey: "k", DefaultPhoneCountry: "us"}
	require.NoError(t, cfg.Validate())
	require.Equal(t, "US", cfg.defaultPhoneCountry())
	for _, bad := range []string{"USA", "U", "1"} {
		cfg.DefaultPhoneCountry = bad
		require.ErrorContains(t, cfg.Validate(), "default_phone_country", bad)
	}
}

func TestStoreWorkspaceLimits(t *testing.T) {
	// A workspace at its plan's record limit stops the task, even when bad records are skipped.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.failNext("/rest/batch/companies", 409, nil, messagesBody("Workspace record limit reached"))
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`)),
		"Workspace record limit reached")
	require.Equal(t, 1, h.fake.count(http.MethodPost, "/rest/batch/companies"), "the batch isn't split")

	// A redirect stops the task rather than turning the write into a GET.
	h = newHarness(t, companies(""))
	h.fake.failNext("/rest/batch/companies", 301, http.Header{"Location": {"https://twenty.example.com/rest/batch/companies"}}, nil)
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`)), `redirect to "https://twenty.example.com/rest/batch/companies"`)
}

func TestFindSplitsLongFilters(t *testing.T) {
	f, c := newFakeTwenty(t)
	values := make([]string, 30)
	for i := range values {
		values[i] = fmt.Sprintf("%03d-%s", i, strings.Repeat("x", 500))
	}
	_, err := c.find(context.Background(), "companies", "externalId", values, true)
	require.NoError(t, err)
	require.Equal(t, 3, f.count(http.MethodGet, "/rest/companies"), "30 values of 500 bytes take 3 lookups of at most 6,000 bytes")
}

func TestStoreSupersededMerges(t *testing.T) {
	h := newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Old","code":"A-1","city":"Paris"}`),
		h.doc(0, `{"id":"k1","name":"New","country":"France"}`)))
	row := h.only("companies", "k1")
	require.Equal(t, "New", row["name"])
	require.Equal(t, "A-1", row["accountCode"], "a field only the earlier document set is kept")
	require.Equal(t, "Paris", fakeGet(row, "address.addressCity"))
	require.Equal(t, "France", fakeGet(row, "address.addressCountry"))

	// A number starting with "+" drops the country code an earlier document set for its own
	// number, which Twenty would call conflicting.
	h = newHarness(t, people(""))
	require.NoError(t, h.store(
		h.doc(0, `{"id":1,"first":"Jane","phones":{"primaryPhoneNumber":"81234567","primaryPhoneCountryCode":"SG"}}`),
		h.doc(0, `{"id":1,"phones":{"primaryPhoneNumber":"+14155550123"}}`)))
	jane := h.only("people", "1")
	require.Equal(t, "+14155550123", fakeGet(jane, "phones.primaryPhoneNumber"))
	require.Empty(t, fakeGet(jane, "phones.primaryPhoneCountryCode"))
	require.Equal(t, "Jane", fakeGet(jane, "name.firstName"))
}

func TestStoreUniqueValueMoves(t *testing.T) {
	for _, mode := range []string{"", `,"unique_conflict":"fail"`} {
		h := newHarness(t, companies(mode))
		h.fake.seed("companies", record{"externalId": "a", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})

		// a gives acme.com up in the same chunk that c takes it.
		require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"beta.com"}`), h.doc(0, `{"id":"c","name":"Gamma","domain":"acme.com"}`)))
		require.Equal(t, stats{received: 2, created: 1, updated: 1}, h.stats("companies"), "a moved value isn't another record")
		require.Equal(t, "beta.com", fakeGet(h.only("companies", "a"), "domainName.primaryLinkUrl"))
		c := h.only("companies", "c")
		require.Equal(t, "acme.com", fakeGet(c, "domainName.primaryLinkUrl"))
		require.Equal(t, "Gamma", c["name"])
	}

	// Two values moving to one record go in one write.
	h := newHarness(t, companies(""))
	h.fake.seed("companies", record{"externalId": "a", "accountCode": "A-1", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"beta.com","code":"A-2"}`), h.doc(0, `{"id":"c","domain":"acme.com","code":"A-1"}`)))
	require.Equal(t, stats{received: 2, created: 1, updated: 1}, h.stats("companies"))
	c := h.only("companies", "c")
	require.Equal(t, "acme.com", fakeGet(c, "domainName.primaryLinkUrl"))
	require.Equal(t, "A-1", c["accountCode"])
	require.Equal(t, []int{1, 1, 1}, h.fake.batches, "create c, update a, then c's values")

	// A record whose own update is rejected doesn't take the value either, and a moved value
	// Twenty rejects, which no record holds then, is left out as rejected_records skip says,
	// without skipping the record it was moving to.
	h = newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = func(_ string, rec record) string {
		if rec["name"] == "Bad!" || (rec["externalId"] == "d" && fakeGet(rec, "domainName.primaryLinkUrl") == "delta.com") {
			return "Bad"
		}
		return ""
	}
	h.fake.seed("companies", record{"externalId": "a", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	h.fake.seed("companies", record{"externalId": "b"})
	h.fake.seed("companies", record{"externalId": "c", "domainName": map[string]any{"primaryLinkUrl": "delta.com"}})
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"beta.com"}`), h.doc(0, `{"id":"b","name":"Bad!","domain":"acme.com"}`),
		h.doc(0, `{"id":"c","domain":"gamma.com"}`), h.doc(0, `{"id":"d","name":"Delta","domain":"delta.com"}`)))
	require.Equal(t, stats{received: 4, created: 1, updated: 2, rejected: 1, skipped: map[string]int{"moved_value_rejected": 1}},
		h.stats("companies"))
	require.Empty(t, fakeGet(h.only("companies", "b"), "domainName.primaryLinkUrl"))
	require.Empty(t, fakeGet(h.only("companies", "d"), "domainName.primaryLinkUrl"))
	require.Equal(t, "Delta", h.only("companies", "d")["name"])

	// A swap: the records are written without the domains, write them as they are, give them up,
	// then take the other's.
	h = newHarness(t, companies(""))
	h.fake.seed("companies", record{"externalId": "a", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	h.fake.seed("companies", record{"externalId": "b", "domainName": map[string]any{"primaryLinkUrl": "beta.com"}})
	require.NoError(t, h.store(h.doc(0, `{"id":"a","domain":"beta.com"}`), h.doc(0, `{"id":"b","domain":"acme.com"}`)))
	require.Equal(t, stats{received: 2, updated: 2}, h.stats("companies"))
	require.Equal(t, "beta.com", fakeGet(h.only("companies", "a"), "domainName.primaryLinkUrl"))
	require.Equal(t, "acme.com", fakeGet(h.only("companies", "b"), "domainName.primaryLinkUrl"))
	require.Equal(t, []int{2, 2, 2, 2}, h.fake.batches)
}

func TestStoreCreateRefusedByExistingKey(t *testing.T) {
	h := newHarness(t, companies(""))
	// The record appears between the lookup and the create, as when a retried create's first
	// attempt was committed but its answer lost.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && h.fake.byKeyLocked("companies", "k1") == nil {
			id := h.fake.newID()
			row := h.fake.blank("companies")
			merge(row, record{"id": id, "externalId": "k1", "name": "First try"})
			h.fake.rows["companies"][id] = row
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`)))
	require.Equal(t, stats{received: 2, created: 1, updated: 1}, h.stats("companies"))
	require.Equal(t, "Acme", h.only("companies", "k1")["name"])
	require.Equal(t, "Beta", h.only("companies", "k2")["name"])
}

func TestStoreRetriedCreateRefusedByExistingKey(t *testing.T) {
	h := newHarness(t, people(`,"rejected_records":"skip"`))
	// Twenty rejects the bad email; the create without it is committed, but its answer lost.
	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/people") {
			if posts++; posts == 2 {
				id := h.fake.newID()
				row := h.fake.blank("people")
				merge(row, record{"id": id, "externalId": "1"})
				h.fake.rows["people"][id] = row
			}
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","email":"not-an-email"}`)))
	require.Equal(t, 0, h.stats("people").rejected)
	require.Equal(t, "Jane", fakeGet(h.only("people", "1"), "name.firstName"))
}

func TestStoreSkipLimitAtEnd(t *testing.T) {
	// 11 bad records first, then 200 good ones: 11 of 211 is under 10%, so the task goes on.
	h := newHarness(t, companies(`,"rejected_records":"skip"`))
	h.fake.reject = func(_ string, rec record) string {
		if strings.HasSuffix(fmt.Sprint(rec["name"]), "!") {
			return "Bad"
		}
		return ""
	}
	var items []storeItem
	for i := range 211 {
		name := fmt.Sprintf("Company %d", i)
		if i < 11 {
			name += "!"
		}
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":"k%03d","name":%q}`, i, name)))
	}
	require.NoError(t, h.store(items...))
	require.Equal(t, 11, h.stats("companies").rejected)
}

func TestStoreRedactsAllPhones(t *testing.T) {
	h := newHarness(t, people(""))
	h.fake.reject = func(_ string, rec record) string {
		return "Bad numbers 9123 4567 and 8765 4321"
	}
	err := h.store(h.doc(0, `{"id":1,"phones":{"primaryPhoneNumber":"9123 4567","additionalPhones":[{"number":"8765 4321"}]}}`))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "9123")
	require.NotContains(t, err.Error(), "8765")
}

func TestNewBindingOverlaps(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "people")
	require.NoError(t, err)
	res := resource{Object: "people", IDField: "externalId"}
	coll := collection("acmeCo/people", "integer",
		proj("emails", "/email", "string"), proj("emails/primaryEmail", "/email2", "string"),
		proj("phones", "/phones", "object"), proj("phones/primaryPhoneNumber", "/phone", "string"),
		proj("emails/additionalEmails", "/more", "array"))
	for _, tc := range []struct {
		values  []string
		wantErr string
	}{
		{[]string{"emails", "emails/primaryEmail"}, `the fields "emails" and "emails/primaryEmail" both write to the Twenty field emails.primaryEmail`},
		{[]string{"phones", "phones/primaryPhoneNumber"}, `both write to the Twenty field phones.primaryPhoneNumber`},
		{[]string{"emails", "emails/additionalEmails"}, ""},
	} {
		b := &pf.MaterializationSpec_Binding{Collection: coll,
			FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: tc.values, Document: "flow_document"}}
		_, err := newBinding(res, obj, nil, &pf.MaterializationSpec{}, b)
		if tc.wantErr == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.wantErr)
		}
	}
}

func TestStoreCurrencyAmount(t *testing.T) {
	h := newHarness(t, testBinding{
		resource: `{"name":"acmeCo/deals","object":"companies","id_field":"externalId"}`,
		collection: collection("acmeCo/deals", "string",
			proj("annualRevenue/amount", "/amount", "null", "number"),
			proj("annualRevenue/currencyCode", "/currency", "string")),
	})
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","amount":19.99,"currency":"EUR"}`)))
	require.Equal(t, map[string]any{"amountMicros": json.Number("19990000"), "currencyCode": "EUR"}, h.only("companies", "k1")["annualRevenue"])
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","amount":null}`)))
	require.Equal(t, map[string]any{"amountMicros": nil, "currencyCode": "EUR"}, h.only("companies", "k1")["annualRevenue"])
	require.NoError(t, h.store(h.doc(0, `{"id":"k2","amount":123456789012.34}`)))
	require.Equal(t, json.Number("123456789012340000"), fakeGet(h.only("companies", "k2"), "annualRevenue.amountMicros"))
	err := h.store(h.doc(0, `{"id":"k3","amount":1e13}`))
	require.ErrorContains(t, err, "out of range for a Twenty currency amount")
	require.Contains(t, h.constraints[0]["annualRevenue/amount"][0].Reason, "annualRevenue.amountMicros, converted from units to micros")

	// With null_values keep, a null amount leaves Twenty's.
	h = newHarness(t, testBinding{
		resource:   `{"name":"acmeCo/deals","object":"companies","id_field":"externalId","null_values":"keep"}`,
		collection: collection("acmeCo/deals", "string", proj("annualRevenue/amount", "/amount", "null", "number")),
	})
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","amount":2}`)))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","amount":null}`)))
	require.Equal(t, json.Number("2000000"), fakeGet(h.only("companies", "k1"), "annualRevenue.amountMicros"))
}

func TestNewBindingAmountOverlaps(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	coll := collection("acmeCo/companies", "string",
		proj("annualRevenue", "/revenue", "object"), proj("annualRevenue/amount", "/amount", "number"),
		proj("annualRevenue/amountMicros", "/micros", "integer"), proj("annualRevenue/currencyCode", "/code", "string"))
	for _, tc := range []struct {
		values  []string
		wantErr string
	}{
		{[]string{"annualRevenue", "annualRevenue/amount"}, `the fields "annualRevenue" and "annualRevenue/amount" both write to the Twenty field annualRevenue.amountMicros`},
		{[]string{"annualRevenue/amount", "annualRevenue/amountMicros"}, `both write to the Twenty field annualRevenue.amountMicros`},
		{[]string{"annualRevenue/amount", "annualRevenue/currencyCode"}, ""},
	} {
		b := &pf.MaterializationSpec_Binding{Collection: coll,
			FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: tc.values, Document: "flow_document"}}
		_, err := newBinding(resource{Object: "companies", IDField: "externalId"}, obj, nil, &pf.MaterializationSpec{}, b)
		if tc.wantErr == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.wantErr)
		}
	}
}

func TestStoreWholeCurrencyTooLarge(t *testing.T) {
	h := newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","revenue":{"amountMicros":9007199254740993,"currencyCode":"USD"}}`)))
	err := h.store(h.doc(0, `{"id":"k2","revenue":{"amountMicros":9223372036854775808,"currencyCode":"USD"}}`))
	require.ErrorContains(t, err, "amountMicros")
	require.ErrorContains(t, err, "out of range for a Twenty currency amount")
}

func TestCheckIDFieldKept(t *testing.T) {
	last := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
		ResourcePath:       []string{"companies", "externalId"},
		ResourceConfigJson: json.RawMessage(`{"name":"x","object":"companies","id_field":"externalId"}`),
		Collection:         pf.CollectionSpec{Name: "acme/companies"},
	}}}
	require.NoError(t, checkIDFieldKept(nil, "acme/companies", resource{Object: "companies", IDField: "other"}, nil))
	require.NoError(t, checkIDFieldKept(last, "acme/companies", resource{Object: "companies", IDField: "externalId"}, nil))
	require.NoError(t, checkIDFieldKept(last, "acme/companies", resource{Object: "people", IDField: "other"}, nil))
	// Another collection may write the same object by its own ID field.
	require.NoError(t, checkIDFieldKept(last, "acme/accounts", resource{Object: "companies", IDField: "other"}, nil))
	require.ErrorContains(t, checkIDFieldKept(last, "acme/companies", resource{Object: "companies", IDField: "other"}, nil),
		`finds records by "externalId", and can't change to "other"`)
	// A collection that writes one object by two ID fields can publish again.
	last.Bindings = append(last.Bindings, &pf.MaterializationSpec_Binding{
		ResourcePath:       []string{"companies", "other"},
		ResourceConfigJson: json.RawMessage(`{"name":"y","object":"companies","id_field":"other"}`),
		Collection:         pf.CollectionSpec{Name: "acme/companies"},
	})
	require.NoError(t, checkIDFieldKept(last, "acme/companies", resource{Object: "companies", IDField: "externalId"}, nil))
	require.NoError(t, checkIDFieldKept(last, "acme/companies", resource{Object: "companies", IDField: "other"}, nil))

	// Nor can its key's locations, or their order, change: the ID field would hold other text.
	keyed := pf.CollectionSpec{Name: "acme/companies", Key: []string{"/id"}, Projections: []pf.Projection{
		{Field: "id", Ptr: "/id"}, {Field: "ident", Ptr: "/id"}, {Field: "region", Ptr: "/region"}}}
	last = &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
		ResourceConfigJson: json.RawMessage(`{"name":"x","object":"companies","id_field":"externalId"}`),
		Collection:         keyed,
		FieldSelection:     pf.FieldSelection{Keys: []string{"id"}},
	}}}
	res := resource{Object: "companies", IDField: "externalId"}
	require.NoError(t, checkIDFieldKept(last, "acme/companies", res, []string{"/id"}))
	require.NoError(t, checkIDFieldKept(last, "acme/companies", res, nil))
	require.ErrorContains(t, checkIDFieldKept(last, "acme/companies", res, []string{"/region", "/id"}),
		`writes the key /id to "externalId", and can't change it to /region, /id`)
	last.Bindings[0].FieldSelection.Keys = []string{"region", "ident"} // a renamed projection of one location is the same key
	require.NoError(t, checkIDFieldKept(last, "acme/companies", res, []string{"/region", "/id"}))
	require.ErrorContains(t, checkIDFieldKept(last, "acme/companies", res, []string{"/id", "/region"}), "can't change it to /id, /region")
}

func TestWritable(t *testing.T) {
	for _, tc := range []struct {
		f       fieldMeta
		wantWhy string
	}{
		{fieldMeta{Name: "name", Type: "TEXT", IsActive: true, Writability: "OPEN"}, ""},
		{fieldMeta{Name: "bodyV2", Type: "RICH_TEXT", IsActive: true}, ""},
		{fieldMeta{Name: "old", Type: "TEXT"}, `The Twenty field "old" is inactive`},
		{fieldMeta{Name: "createdAt", Type: "DATE_TIME", IsActive: true, IsSystem: true}, `Twenty manages the field "createdAt"`},
		{fieldMeta{Name: "thread", Type: "TEXT", IsActive: true, Writability: "SYSTEM"}, `Twenty manages the field "thread"`},
		{fieldMeta{Name: "createdBy", Type: "ACTOR", IsActive: true}, `Twenty manages ACTOR fields like "createdBy"`},
		{rel("company", "MANY_TO_ONE"), `"company" is a relation. To link records through it, add it to the binding's relations`},
		{rel("people", "ONE_TO_MANY"), `"people" lists the records that point at this one. Link them from their own object's binding instead`},
		{fieldMeta{Name: "target", Type: "MORPH_RELATION", IsActive: true}, "can point at more than one object"},
		{morphField("targetCompany"), `"targetCompany" is a relation. To link records through it, add it to the binding's relations`},
		{fieldMeta{Name: "avatarFile", Type: "FILES", IsActive: true}, "holds files uploaded to Twenty"},
		{fieldMeta{Name: "bodyV2", Type: "RICH_TEXT_V2", IsActive: true}, "older than v2.0"},
	} {
		ok, why := writable(tc.f)
		require.Equal(t, tc.wantWhy == "", ok, tc.f.Name)
		require.Contains(t, why, tc.wantWhy, tc.f.Name)
	}
}

func rel(name, relationType string) fieldMeta {
	f := fieldMeta{Name: name, Type: "RELATION", IsActive: true}
	f.Settings.RelationType = relationType
	return f
}

func TestResourceRelations(t *testing.T) {
	base := resource{Name: "acmeCo/people", Object: "people", IDField: "externalId"}
	for _, tc := range []struct {
		rels    []relation
		wantErr string
	}{
		{[]relation{{Field: "company", IDField: "externalId"}, {Field: "owner", IDField: "email", SourceField: "ownerKey"}}, ""},
		{[]relation{{IDField: "externalId"}}, `each needs "field"`},
		{[]relation{{Field: "company"}}, `"company" needs "id_field"`},
		{[]relation{{Field: "company.id", IDField: "externalId"}}, `each needs "field"`},
		{[]relation{{Field: "company", IDField: "externalId"}, {Field: "company", IDField: "name"}}, `"company" is listed twice`},
	} {
		res := base
		res.Relations = tc.rels
		err := res.Validate()
		if tc.wantErr == "" {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, tc.wantErr)
		}
	}
}

// --- relations -----------------------------------------------------------------------------------

func probes(options string, projections ...pf.Projection) testBinding {
	return testBinding{
		resource:   `{"name":"acmeCo/probes","object":"connectorTypeProbes","id_field":"externalId"` + options + `}`,
		collection: collection("acmeCo/probes", "string", projections...),
	}
}

// logged returns the fields of the last log line with the message.
func (h *harness) logged(message string) log.Fields {
	h.t.Helper()
	for _, e := range slices.Backward(logs.AllEntries()) {
		if e.Message == message {
			return e.Data
		}
	}
	h.t.Fatalf("nothing logged %q", message)
	return nil
}

func morphField(name string) fieldMeta {
	f := fieldMeta{Name: name, Type: "MORPH_RELATION", IsActive: true, MorphID: "m-note"}
	f.Settings.RelationType, f.Settings.JoinColumnName = "MANY_TO_ONE", name+"Id"
	return f
}

func TestCheckRelations(t *testing.T) {
	f, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	check := func(object string, rels ...relation) ([]link, error) {
		obj, err := findObject(objects, object)
		require.NoError(t, err)
		return checkRelations(t.Context(), c, objects, resource{Object: object, IDField: "externalId", Relations: rels}, obj)
	}

	links, err := check("people", relation{Field: "company", IDField: "externalId"})
	require.NoError(t, err)
	require.Equal(t, []link{{field: "company", joinColumn: "companyId", target: "companies", idField: "externalId", source: "company"}}, links)
	links, err = check("connectorTypeProbes", relation{Field: "pParent", IDField: "externalId", SourceField: "parentKey"})
	require.NoError(t, err)
	require.Equal(t, []link{{field: "pParent", joinColumn: "pParentId", target: "connectorTypeProbes", idField: "externalId", source: "parentKey"}}, links)

	// A morph relation links through its field for one object, found by Twenty's naming.
	links, err = check("noteTargets", relation{Field: "note", IDField: "externalId", SourceField: "noteKey"},
		relation{Field: "targetPerson", IDField: "externalId", SourceField: "personKey"})
	require.NoError(t, err)
	require.Equal(t, []link{
		{field: "note", joinColumn: "noteId", target: "notes", idField: "externalId", source: "noteKey"},
		{field: "targetPerson", joinColumn: "targetPersonId", target: "people", idField: "externalId", source: "personKey"},
	}, links)

	var userErr *cerrors.UserError
	for _, tc := range []struct {
		object  string
		rel     relation
		wantErr string
	}{
		{"noteTargets", relation{Field: "target", IDField: "externalId"}, `noteTargets.target can point at more than one object. Link through its field for one object instead, like "targetCompany"`},
		{"people", relation{Field: "nope", IDField: "externalId"}, `the Twenty object "people" has no field "nope"`},
		{"people", relation{Field: "jobTitle", IDField: "externalId"}, "people.jobTitle is a TEXT field, not a relation"},
		{"companies", relation{Field: "people", IDField: "externalId"}, "companies.people lists the records that point at this one"},
		{"connectorTypeProbes", relation{Field: "pMorph", IDField: "externalId"}, "can point at more than one object"},
		{"people", relation{Field: "company", IDField: "nope"}, `people.company points at companies, which has no field "nope"`},
		{"people", relation{Field: "company", IDField: "employees"}, "companies.employees is a NUMBER field, and must be a unique Text, Links or Emails field"},
		{"people", relation{Field: "company", IDField: "name"}, "companies.name must be unique"},
	} {
		_, err := check(tc.object, tc.rel)
		require.ErrorContains(t, err, tc.wantErr)
		require.ErrorAs(t, relationError(err), &userErr, "a relation that doesn't match Twenty is the user's to fix")
	}

	// A failed metadata request isn't the binding's fault, unless Twenty refused the key.
	f.failNext("/metadata", 200, nil, map[string]any{"data": nil, "errors": []any{map[string]any{"message": "Internal error"}}})
	_, err = check("people", relation{Field: "company", IDField: "externalId"})
	require.ErrorContains(t, err, "Twenty's metadata API answered: Internal error")
	require.False(t, errors.As(relationError(err), &userErr))
	f.failNext("/metadata", 401, nil, messagesBody("Token invalid."))
	_, err = check("people", relation{Field: "company", IDField: "externalId"})
	require.ErrorAs(t, relationError(err), &userErr)

	f.failNext("/metadata", 200, nil, map[string]any{"data": map[string]any{"field": map[string]any{"relation": map[string]any{
		"type": "MANY_TO_ONE", "targetObjectMetadata": map[string]any{"namePlural": "opportunities"}}}}})
	_, err = check("people", relation{Field: "company", IDField: "externalId"})
	require.ErrorContains(t, err, `people.company points at "opportunities": Twenty has no object "opportunities"`)
	f.failNext("/metadata", 200, nil, map[string]any{"data": map[string]any{"field": map[string]any{"relation": nil}}})
	_, err = check("people", relation{Field: "company", IDField: "externalId"})
	require.ErrorContains(t, err, "didn't say which object the relation points at")

	// Two targets of one morph relation can't both be set on a record.
	_, err = check("noteTargets", relation{Field: "targetCompany", IDField: "externalId", SourceField: "companyKey"},
		relation{Field: "targetPerson", IDField: "externalId", SourceField: "personKey"})
	require.ErrorContains(t, err, "noteTargets.targetCompany and noteTargets.targetPerson are two targets of one relation")
	require.ErrorAs(t, relationError(err), &userErr)
	// Task targets work the same way.
	links, err = check("taskTargets", relation{Field: "task", IDField: "externalId"}, relation{Field: "targetCompany", IDField: "externalId", SourceField: "companyKey"})
	require.NoError(t, err)
	require.Equal(t, link{field: "targetCompany", joinColumn: "targetCompanyId", target: "companies", idField: "externalId", source: "companyKey"}, links[1])

	// A target Twenty didn't name after its object, or named after two, isn't guessed, and that's
	// the user's to fix.
	for _, morph := range [][]string{{"people/person"}, {"companies/company", "firms/company"}} {
		var list []any
		for _, m := range morph {
			plural, singular, _ := strings.Cut(m, "/")
			list = append(list, map[string]any{"targetObjectMetadata": map[string]any{"namePlural": plural, "nameSingular": singular}})
		}
		f.failNext("/metadata", 200, nil, map[string]any{"data": map[string]any{"field": map[string]any{"morphRelations": list}}})
		_, err = check("noteTargets", relation{Field: "targetCompany", IDField: "externalId"})
		require.ErrorContains(t, err, `noteTargets.targetCompany is a field of a relation that can point at more than one object, and the connector can't tell which one it is for`)
		require.ErrorAs(t, relationError(err), &userErr)
	}
	// So is one whose join column isn't named after the field.
	obj, err := findObject(objects, "noteTargets")
	require.NoError(t, err)
	tf, ok := obj.field("targetCompany")
	require.True(t, ok)
	tf.Settings.JoinColumnName = "companyId"
	got, err := c.morphTarget(t.Context(), tf)
	require.NoError(t, err)
	require.Empty(t, got)
}

func noteTargets(rels string) testBinding {
	return testBinding{
		resource: `{"name":"acmeCo/noteLinks","object":"noteTargets","id_field":"externalId","relations":` + rels + `}`,
		collection: collection("acmeCo/noteLinks", "string", proj("noteKey", "/noteKey", "string"),
			proj("companyKey", "/companyKey", "null", "string")),
	}
}

func TestStoreNoteTargets(t *testing.T) {
	h := newHarness(t, noteTargets(`[{"field":"note","id_field":"externalId","source_field":"noteKey"},`+
		`{"field":"targetCompany","id_field":"externalId","source_field":"companyKey"}]`))
	note := h.fake.seed("notes", record{"externalId": "n1", "title": "Call"})["id"].(string)
	acme := h.fake.seed("companies", record{"externalId": "c1"})["id"].(string)

	// A link row points the note at the company; one whose company isn't there yet waits.
	require.NoError(t, h.store(h.doc(0, `{"id":"l1","noteKey":"n1","companyKey":"c1"}`), h.doc(0, `{"id":"l2","noteKey":"n1","companyKey":"c2"}`)))
	require.Equal(t, note, h.only("noteTargets", "l1")["noteId"])
	require.Equal(t, acme, h.only("noteTargets", "l1")["targetCompanyId"])
	require.Nil(t, h.only("noteTargets", "l2")["targetCompanyId"])
	require.Len(t, h.tx.bindings[0].waiting, 1)
}

func TestRetryNoteTargetDuplicates(t *testing.T) {
	// Two link rows wait for one company; when it arrives, Twenty takes only one of them.
	for _, policy := range []string{"skip", "fail"} {
		tb := noteTargets(`[{"field":"note","id_field":"externalId","source_field":"noteKey"},` +
			`{"field":"targetCompany","id_field":"externalId","source_field":"companyKey"}]`)
		tb.resource = strings.Replace(tb.resource, `"relations"`, `"rejected_records":"`+policy+`","relations"`, 1)
		h := newHarness(t, tb)
		now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
		h.tx.now = func() time.Time { return now }
		h.fake.seed("notes", record{"externalId": "n1"})
		require.NotNil(t, h.storeState(h.doc(0, `{"id":"l1","noteKey":"n1","companyKey":"c1"}`), h.doc(0, `{"id":"l2","noteKey":"n1","companyKey":"c1"}`)))
		require.Len(t, h.tx.bindings[0].waiting, 2)
		acme := h.fake.seed("companies", record{"externalId": "c1"})["id"]

		now = now.Add(linkRetryEvery)
		h.tx.retryWaiting(t.Context())
		linked := 0
		for _, k := range []string{"l1", "l2"} {
			if h.only("noteTargets", k)["targetCompanyId"] == acme {
				linked++
			}
		}
		require.Equal(t, 1, linked, policy)

		// The rejected link is tried once more with the next transaction.
		err := h.store(h.doc(0, `{"id":"l3","noteKey":"n1","companyKey":null}`))
		if policy == "fail" {
			require.ErrorContains(t, err, "A duplicate entry was detected")
			continue
		}
		require.NoError(t, err)
		require.Empty(t, h.tx.bindings[0].waiting)
	}
}

func TestStoreNoteTargetDuplicates(t *testing.T) {
	rels := `,"relations":[{"field":"note","id_field":"externalId","source_field":"noteKey"},` +
		`{"field":"targetCompany","id_field":"externalId","source_field":"companyKey"}]`
	for _, policy := range []string{"skip", "fail"} {
		tb := noteTargets(rels[len(`,"relations":`):])
		tb.resource = strings.Replace(tb.resource, `"relations"`, `"rejected_records":"`+policy+`","relations"`, 1)
		h := newHarness(t, tb)
		h.fake.seed("notes", record{"externalId": "n1"})
		h.fake.seed("companies", record{"externalId": "c1"})
		require.NoError(t, h.store(h.doc(0, `{"id":"l1","noteKey":"n1","companyKey":"c1"}`)))

		// Twenty keeps one live link of a note to a company, so a second row for the pair is rejected.
		err := h.store(h.doc(0, `{"id":"l2","noteKey":"n1","companyKey":"c1"}`))
		if policy == "fail" {
			require.ErrorContains(t, err, "A duplicate entry was detected")
			continue
		}
		require.NoError(t, err)
		require.Equal(t, 1, h.stats("noteTargets").rejected)
		require.Len(t, h.fake.rows["noteTargets"], 1)
	}
}

func TestValidateBindingLinks(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "people")
	require.NoError(t, err)
	company := link{field: "company", joinColumn: "companyId", target: "companies", idField: "externalId", source: "company"}

	pc := peopleCollection()
	got, err := validateBinding(resource{Object: "people", IDField: "externalId", Relations: []relation{{Field: "company", IDField: "externalId"}}},
		obj, []link{company}, &pc)
	require.NoError(t, err)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, got["company"][0].Type)
	require.Equal(t, `The connector sets the relation "company" to the companies record whose externalId is this value`, got["company"][0].Reason)

	for _, tc := range []struct {
		source  string
		types   []string
		wantErr string
	}{
		{"nope", nil, `the collection "acmeCo/people" has no field "nope" to hold the keys of the records "company" points at`},
		{"id", nil, `the collection key or document "id" can't hold the keys of linked records`},
		{"flow_document", nil, `the collection key or document "flow_document" can't hold the keys of linked records`},
		{"company", []string{"number"}, "which must be strings or integers, not number"},
		{"company", []string{"null", "object", "string"}, "which must be strings or integers, not object or string"},
		{"company", []string{"null"}, "which must be strings or integers, not null"},
	} {
		pc := peopleCollection()
		if tc.types != nil {
			pc.GetProjection("company").Inference.Types = tc.types
		}
		l := company
		l.source = tc.source
		_, err := validateBinding(resource{Object: "people", IDField: "externalId"}, obj, []link{l}, &pc)
		require.ErrorContains(t, err, tc.wantErr, tc.source)
	}

	// The source must be selected.
	spec := &pf.MaterializationSpec{}
	b := &pf.MaterializationSpec_Binding{Collection: peopleCollection(),
		FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: []string{"jobTitle"}, Document: "flow_document"}}
	_, err = newBinding(resource{Object: "people", IDField: "externalId"}, obj, []link{company}, spec, b)
	require.ErrorContains(t, err, `the binding links "company" from the field "company", which isn't selected`)
}

func TestStoreLinks(t *testing.T) {
	h := newHarness(t, companies(""), people(linkCompany))
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme"}`), h.doc(0, `{"id":"k2","name":"Beta"}`)))
	acme, beta := h.only("companies", "k1")["id"], h.only("companies", "k2")["id"]

	before := h.fake.count(http.MethodGet, "/rest/companies")
	require.NoError(t, h.store(h.doc(1, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(1, `{"id":2,"first":"Raj","company":"k2"}`),
		h.doc(1, `{"id":3,"first":"Ana","company":"k1"}`), h.doc(1, `{"id":4,"first":"Bo"}`)))
	require.Equal(t, stats{received: 4, created: 4}, h.stats("people"))
	require.Equal(t, acme, h.only("people", "1")["companyId"])
	require.Equal(t, beta, h.only("people", "2")["companyId"])
	require.Nil(t, h.only("people", "4")["companyId"])
	require.Equal(t, 1, h.fake.count(http.MethodGet, "/rest/companies")-before, "one lookup finds the chunk's companies")

	// A link moves, a null or empty key clears it, and a document without the field keeps it.
	require.NoError(t, h.store(h.doc(1, `{"id":1,"company":"k2"}`), h.doc(1, `{"id":2,"company":null}`),
		h.doc(1, `{"id":3,"company":""}`), h.doc(1, `{"id":4,"first":"Bo 2"}`)))
	require.Equal(t, stats{received: 4, updated: 4}, h.stats("people"))
	require.Equal(t, beta, h.only("people", "1")["companyId"])
	require.Nil(t, h.only("people", "2")["companyId"])
	require.Nil(t, h.only("people", "3")["companyId"])
	require.NoError(t, h.store(h.doc(1, `{"id":1,"first":"Jane 2"}`)))
	require.Equal(t, beta, h.only("people", "1")["companyId"])

	// A key that isn't in Twenty leaves the link as it is: a record keeps its link, and a new
	// one has none.
	require.NoError(t, h.store(h.doc(1, `{"id":1,"company":"k9"}`), h.doc(1, `{"id":5,"first":"New","company":"k9"}`)))
	require.Equal(t, stats{received: 2, created: 1, updated: 1, skipped: map[string]int{"company_link_waiting": 2}}, h.stats("people"))
	require.Equal(t, beta, h.only("people", "1")["companyId"])
	require.Nil(t, h.only("people", "5")["companyId"])
	missing := h.logged("links to records that aren't in Twenty yet wait for them")
	require.Equal(t, 2, missing["count"])
	require.Equal(t, []string{"1 -> k9", "5 -> k9"}, missing["examples"])

	// A soft-deleted company can still be linked, as in Twenty.
	h.fake.softDelete("companies", acme.(string))
	require.NoError(t, h.store(h.doc(1, `{"id":5,"company":"k1"}`)))
	require.Equal(t, acme, h.only("people", "5")["companyId"])

	// A key that can't go in a filter can't be looked up.
	require.NoError(t, h.store(h.doc(1, `{"id":6,"first":"Quote","company":"say \"hi\""}`)))
	require.Equal(t, stats{received: 1, created: 1, skipped: map[string]int{"company_target_key_unusable": 1}}, h.stats("people"))
	require.Nil(t, h.only("people", "6")["companyId"])
}

func TestStoreLinksLater(t *testing.T) {
	// People come first, so a company the transaction creates comes after the person.
	h := newHarness(t, people(linkCompany), companies(""))
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(0, `{"id":2,"first":"Raj","company":"k9"}`),
		h.doc(1, `{"id":"k1","name":"Acme"}`)))
	require.Equal(t, stats{received: 2, created: 2, linkedLater: 1, skipped: map[string]int{"company_link_waiting": 1}}, h.stats("people"))
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.Nil(t, h.only("people", "2")["companyId"])

	// Records that point at records of the same chunk, or at themselves.
	h = newHarness(t, probes(`,"relations":[{"field":"pParent","id_field":"externalId"}]`, proj("pParent", "/parent", "string")))
	require.NoError(t, h.store(h.doc(0, `{"id":"a","parent":"b"}`), h.doc(0, `{"id":"b","parent":"b"}`)))
	require.Equal(t, stats{received: 2, created: 2, linkedLater: 2}, h.stats("connectorTypeProbes"))
	b := h.only("connectorTypeProbes", "b")["id"]
	require.Equal(t, b, h.only("connectorTypeProbes", "a")["pParentId"])
	require.Equal(t, b, h.only("connectorTypeProbes", "b")["pParentId"])

	// A key's last document decides its link, even in a later chunk.
	h = newHarness(t, people(linkCompany), companies(""))
	h.fake.seed("companies", record{"externalId": "k1"})
	var items []storeItem
	for i := range maxBatch - 1 {
		items = append(items, h.doc(0, fmt.Sprintf(`{"id":%d,"first":"P%d"}`, 100+i, i)))
	}
	items = append(items, h.doc(0, `{"id":999,"company":"k9"}`), h.doc(0, `{"id":999,"company":"k1"}`), h.doc(1, `{"id":"k9"}`))
	require.NoError(t, h.store(items...))
	require.Equal(t, stats{received: maxBatch + 1, created: maxBatch, updated: 1}, h.stats("people"))
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "999")["companyId"])

	// A record Twenty rejects gets no link later.
	h = newHarness(t, people(linkCompany+`,"rejected_records":"skip"`), companies(""))
	h.fake.reject = func(_ string, rec record) string {
		if fakeGet(rec, "name.firstName") == "Bad" {
			return "Bad"
		}
		return ""
	}
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Bad","company":"k1"}`), h.doc(1, `{"id":"k1"}`)))
	require.Equal(t, stats{received: 1, rejected: 1}, h.stats("people"))
	require.Empty(t, h.fake.byKey("people", "1"))

	// Nor does a record deleted in Twenty while it waited.
	h = newHarness(t, people(linkCompany), companies(""))
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/rest/batch/companies" {
			for id, row := range h.fake.rows["people"] {
				if row["externalId"] == "1" {
					h.fake.rows["people"][id]["deletedAt"] = "2026-10-28T00:00:00.000Z"
				}
			}
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(1, `{"id":"k1"}`)))
	require.Equal(t, stats{received: 1, created: 1, skipped: map[string]int{"link_record_not_in_twenty": 1}}, h.stats("people"))
	require.Nil(t, h.only("people", "1")["companyId"])

	// A later link that Twenty rejects is left out, and the record stays as written. Or, with
	// rejected_records "fail", it stops the task.
	rejectLink := func(object string, rec record) string {
		if _, named := rec["name"]; object == "people" && !named && rec["companyId"] != nil {
			return `insert or update on table "person" violates foreign key constraint "FK_0123456789abcdef012345678"`
		}
		return ""
	}
	h = newHarness(t, people(linkCompany+`,"rejected_records":"skip"`), companies(""))
	h.fake.reject = rejectLink
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(1, `{"id":"k1"}`)))
	require.Equal(t, stats{received: 1, created: 1, skipped: map[string]int{"link_rejected": 1}}, h.stats("people"))
	require.Equal(t, "Jane", fakeGet(h.only("people", "1"), "name.firstName"))
	require.Nil(t, h.only("people", "1")["companyId"])
	require.Equal(t, "1", h.logged("left out the links of a record")["key"])
	h = newHarness(t, people(linkCompany), companies(""))
	h.fake.reject = rejectLink
	require.ErrorContains(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(1, `{"id":"k1"}`)),
		`people: can't link the record with key "1": Twenty rejected the records (HTTP 400): insert or update`)
}

func TestStoreLinkFromIntegerKey(t *testing.T) {
	h := newHarness(t, companies(""), people(`,"relations":[{"field":"company","id_field":"externalId","source_field":"companyKey"}]`))
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, h.constraints[1]["companyKey"][0].Type)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, h.constraints[1]["company"][0].Type)
	require.Equal(t, `The binding links "company" from the collection field "companyKey"`, h.constraints[1]["company"][0].Reason)
	require.NoError(t, h.store(h.doc(0, `{"id":"42"}`), h.doc(1, `{"id":1,"companyKey":42}`)))
	require.Equal(t, h.only("companies", "42")["id"], h.only("people", "1")["companyId"])
}

func TestLinkNeverSentUnresolved(t *testing.T) {
	_, err := json.Marshal(record{"companyId": linkRef{key: "k1"}})
	require.ErrorContains(t, err, "wasn't looked up before the write")
}

func TestStoreRichText(t *testing.T) {
	h := newHarness(t, notes("", proj("bodyV2", "/body", "null", "string")))
	require.NoError(t, h.store(h.doc(0, `{"id":"n1","title":"Call","body":"# Call\n\n- one"}`)))
	row := h.only("notes", "n1")
	require.Equal(t, "# Call\n\n- one", fakeGet(row, "bodyV2.markdown"))
	require.NotEmpty(t, fakeGet(row, "bodyV2.tiptap"), "Twenty makes the editor's format from the markdown")

	require.NoError(t, h.store(h.doc(0, `{"id":"n1","title":"Call 2"}`)))
	require.Equal(t, "# Call\n\n- one", fakeGet(h.only("notes", "n1"), "bodyV2.markdown"), "a missing body keeps it")
	require.NoError(t, h.store(h.doc(0, `{"id":"n1","body":null}`)))
	require.Equal(t, map[string]any{"blocknote": nil, "markdown": "", "tiptap": nil}, h.only("notes", "n1")["bodyV2"], "a null clears it")

	// An object is written as is, and doesn't merge with an earlier document's: Twenty
	// replaces the whole value.
	h = newHarness(t, notes("", proj("bodyV2", "/body", "object")))
	require.NoError(t, h.store(h.doc(0, `{"id":"n1","body":{"markdown":"first"}}`), h.doc(0, `{"id":"n1","body":{"blocknote":null}}`)))
	require.Equal(t, "", fakeGet(h.only("notes", "n1"), "bodyV2.markdown"))

	h = newHarness(t, notes("", proj("bodyV2/markdown", "/md", "string")))
	require.NoError(t, h.store(h.doc(0, `{"id":"n1","md":"plain"}`)))
	require.Equal(t, "plain", fakeGet(h.only("notes", "n1"), "bodyV2.markdown"))
}

func TestCheckOwnKeys(t *testing.T) {
	companies := &objectMeta{NamePlural: "companies"}
	seen := make(map[[2]string]string)
	// Each source finds its records by its own field.
	require.NoError(t, checkOwnKeys(seen, "acmeCo/hubspot", companies, resource{Object: "companies", IDField: "externalId"}))
	require.NoError(t, checkOwnKeys(seen, "acmeCo/salesforce", companies, resource{Object: "companies", IDField: "accountCode"}))
	// The same field, even with the object named the other way, would share one key space.
	require.ErrorContains(t, checkOwnKeys(seen, "acmeCo/other", companies, resource{Object: "company", IDField: "externalId"}),
		`the collections "acmeCo/hubspot" and "acmeCo/other" both write companies found by "externalId"`)
	require.Equal(t, []string{"companies", "accountCode"}, resource{Object: "companies", IDField: "accountCode"}.path())
}

// storeState runs a transaction and returns the connector state it saves, or nil.
func (h *harness) storeState(items ...storeItem) json.RawMessage {
	h.t.Helper()
	logs.Reset()
	it := materialize.NewStoreIterator(h.t.Context(), &fakeStream{items: items}, &pm.Request{Store: &pm.Request_Store{}})
	commit, err := h.tx.Store(it)
	require.NoError(h.t, err)
	if commit == nil {
		return nil
	}
	st, _ := commit(h.t.Context(), nil)
	return st.UpdatedJson
}

func TestStoreLinksWaitAcrossTransactions(t *testing.T) {
	h := newHarness(t, people(linkCompany), companies(""))
	h.clock()

	// The company isn't in Twenty: the link waits in the saved state.
	st := h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(0, `{"id":2,"first":"Raj","company":"k2"}`))
	require.JSONEq(t, `{"waitingLinks":{"people/externalId":[
		{"key":"1","column":"companyId","target":"k1","lookup":"companies.externalId","since":"2026-11-02T12:00:00Z","tried":"2026-11-02T12:00:00Z"},
		{"key":"2","column":"companyId","target":"k2","lookup":"companies.externalId","since":"2026-11-02T12:00:00Z","tried":"2026-11-02T12:00:00Z"}]}}`, string(st))
	require.Equal(t, map[string]int{"company_link_waiting": 2}, h.stats("people").skipped)

	// It arrives in a later transaction, but waiting links are tried again only every few minutes.
	require.Nil(t, h.storeState(h.doc(1, `{"id":"k1"}`)))
	require.Nil(t, h.only("people", "1")["companyId"])
	h.now = h.now.Add(linkRetryEvery)
	st = h.storeState()
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.JSONEq(t, `{"waitingLinks":{"people/externalId":[
		{"key":"2","column":"companyId","target":"k2","lookup":"companies.externalId","since":"2026-11-02T12:00:00Z","tried":"2026-11-02T12:05:00Z"}]}}`, string(st))

	// After a restart, the saved links still wait.
	restarted := newHarness(t, people(linkCompany), companies(""))
	restarted.fake = h.fake
	restarted.tx.client = h.tx.client
	restarted.clock()
	restarted.now = h.now
	require.NoError(t, restarted.tx.UnmarshalState(st))
	h = restarted
	// The saved time of its last try keeps it from being tried again at once.
	require.Nil(t, h.storeState(h.doc(1, `{"id":"k2"}`)))
	require.Nil(t, h.only("people", "2")["companyId"])
	h.now = h.now.Add(linkRetryEvery)
	require.JSONEq(t, `{}`, string(h.storeState()))
	require.Equal(t, h.only("companies", "k2")["id"], h.only("people", "2")["companyId"])
	require.Equal(t, 1, h.stats("people").linkedLater)

	// A newer document of the record decides its link, so a waiting one is dropped.
	st = h.storeState(h.doc(0, `{"id":3,"company":"k3"}`))
	require.Contains(t, string(st), `"target":"k3"`)
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(0, `{"id":3,"company":null}`))))

	// A link whose record never arrives is dropped after linkWaitFor.
	require.NotNil(t, h.storeState(h.doc(0, `{"id":4,"company":"k4"}`)))
	h.now = h.now.Add(linkWaitFor + time.Minute)
	require.JSONEq(t, `{}`, string(h.storeState()))
	require.Equal(t, map[string]int{"link_expired": 1}, h.stats("people").skipped)

	// A state that names a binding or relation the task no longer has is ignored.
	require.NoError(t, h.tx.UnmarshalState(json.RawMessage(`{"waitingLinks":{"gone/externalId":[{"key":"1","column":"companyId","target":"x"}],
		"people/externalId":[{"key":"1","column":"oldId","target":"x"}]}}`)))
	for _, b := range h.tx.bindings {
		require.Empty(t, b.waiting)
	}
	// So is a link of a relation that now finds its records by another field, and the next state
	// leaves them all out.
	require.NoError(t, h.tx.UnmarshalState(json.RawMessage(`{"waitingLinks":{"people/externalId":[
		{"key":"1","column":"companyId","target":"x","lookup":"companies.accountCode"}]}}`)))
	require.Empty(t, h.tx.bindings[0].waiting)
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(1, `{"id":"k9"}`))))
}

func TestStoreRejectedDocumentKeepsWaitingLink(t *testing.T) {
	h := newHarness(t, people(linkCompany+`,"rejected_records":"skip"`), companies(""))
	h.fake.reject = func(object string, rec record) string {
		if rec["jobTitle"] == "bad" {
			return "bad job title"
		}
		return ""
	}
	st := h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`))
	require.Contains(t, string(st), `"target":"k1"`)

	// A newer document that Twenty rejects doesn't replace the link waiting for k1.
	h.storeState(h.doc(0, `{"id":1,"company":"k2","title":"bad"}`))
	require.Equal(t, 1, h.stats("people").rejected)
	require.Equal(t, "k1", h.tx.bindings[0].waiting[linkSlot{"1", "companyId"}].target)

	// Nor does a rejected document of the key in a later chunk drop the link an earlier one left waiting.
	docs := []storeItem{h.doc(0, `{"id":2,"company":"k3"}`)}
	for i := range maxBatch - 1 {
		docs = append(docs, h.doc(0, fmt.Sprintf(`{"id":%d}`, 100+i)))
	}
	docs = append(docs, h.doc(0, `{"id":2,"title":"bad"}`))
	h.storeState(docs...)
	require.Contains(t, h.tx.bindings[0].waiting, linkSlot{"2", "companyId"})
}

func TestRetryWaitingWithoutDocuments(t *testing.T) {
	h := waitingJane(t)

	// No document arrives, and the retry links the record once the link is due.
	h.tx.retryWaiting(t.Context())
	require.Nil(t, h.only("people", "1")["companyId"])
	h.now = h.now.Add(linkRetryEvery)
	h.tx.retryWaiting(t.Context())
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.Empty(t, h.tx.bindings[0].waiting)
	// The next transaction saves that.
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(1, `{"id":"k2"}`))))
}

func TestRetryWaitingFailureKeepsLinks(t *testing.T) {
	h := waitingJane(t)

	// The record is found, but writing the link fails: the link waits for the next round.
	h.now = h.now.Add(linkRetryEvery)
	for range maxAttempts {
		h.fake.failNext("/rest/batch/people", 503, nil, messagesBody("Unavailable"))
	}
	h.tx.retryWaiting(t.Context())
	require.Nil(t, h.only("people", "1")["companyId"])
	require.Contains(t, h.tx.bindings[0].waiting, linkSlot{"1", "companyId"})
	h.tx.retryWaiting(t.Context()) // not due yet
	require.Nil(t, h.only("people", "1")["companyId"])

	h.now = h.now.Add(linkRetryEvery)
	h.tx.retryWaiting(t.Context())
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.Empty(t, h.tx.bindings[0].waiting)
}

func TestRetryWaitingLookupFailureKeepsLinks(t *testing.T) {
	h := newHarness(t, people(linkCompany), companies(""))
	start := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	now := start
	h.tx.now = func() time.Time { return now }
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`)))
	require.NotNil(t, h.storeState(h.doc(0, `{"id":2,"first":"Joe","company":"k9"}`)))
	h.fake.seed("companies", record{"externalId": "k1"})

	// Past linkWaitFor, k1 is found but reading the people fails: nothing is written, left out
	// or expired in that round.
	now = start.Add(linkWaitFor + time.Minute)
	for range maxAttempts {
		h.fake.failNext("/rest/people", 503, nil, messagesBody("Unavailable"))
	}
	h.tx.retryWaiting(t.Context())
	require.Len(t, h.tx.bindings[0].waiting, 2)
	require.Empty(t, h.tx.rejected)

	now = now.Add(linkRetryEvery)
	h.tx.retryWaiting(t.Context())
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.Empty(t, h.tx.bindings[0].waiting, "1 linked, 2 expired")
}

func TestRetryWaitingRejectionFailsTask(t *testing.T) {
	h := waitingJane(t)

	// With rejected_records fail, a link Twenty rejects in a background round stops the task
	// with the next transaction, though the link isn't due then, if Twenty rejects it again.
	h.now = h.now.Add(linkRetryEvery)
	h.fake.failNext("/rest/batch/people", 400, nil, messagesBody("Bad link"))
	h.tx.retryWaiting(t.Context())
	require.Contains(t, h.tx.bindings[0].waiting, linkSlot{"1", "companyId"})
	// Until then the background stops retrying, also once the link is due again.
	h.now = h.now.Add(linkRetryEvery)
	writes := h.fake.count("POST", "/rest/batch/people")
	h.tx.retryWaiting(t.Context())
	require.Equal(t, writes, h.fake.count("POST", "/rest/batch/people"))
	// The next transaction tries it though it isn't due: it was tried just now.
	h.tx.bindings[0].waiting[linkSlot{"1", "companyId"}] = waitingLink{target: "k1", since: h.now, tried: h.now}
	h.now = h.now.Add(time.Minute)
	h.fake.failNext("/rest/batch/people", 400, nil, messagesBody("Bad link"))
	err := h.store(h.doc(1, `{"id":"k2"}`))
	require.ErrorContains(t, err, "can't link the record")
}

func TestRetryWaitingRejectionAccepted(t *testing.T) {
	h := waitingJane(t)
	h.now = h.now.Add(linkRetryEvery)
	h.fake.failNext("/rest/batch/people", 400, nil, messagesBody("Bad link"))
	h.tx.retryWaiting(t.Context())

	// Twenty takes the link the next time, so the task carries on.
	h.now = h.now.Add(time.Minute)
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(1, `{"id":"k2"}`))))
	require.Equal(t, h.only("companies", "k1")["id"], h.only("people", "1")["companyId"])
	require.Empty(t, h.tx.rejected)
}

func TestRetryWaitingRejectionNotFound(t *testing.T) {
	h := newHarness(t, people(linkCompany), companies(""))
	start := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	now := start
	h.tx.now = func() time.Time { return now }
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`)))
	h.fake.seed("companies", record{"externalId": "k1"})
	now = start.Add(linkRetryEvery)
	h.fake.failNext("/rest/batch/people", 400, nil, messagesBody("Bad link"))
	h.tx.retryWaiting(t.Context())

	// The company is gone by the next transaction: the link waits, tried then, so not due a
	// minute later.
	for id := range h.fake.rows["companies"] {
		delete(h.fake.rows["companies"], id)
	}
	now = now.Add(4 * time.Minute)
	h.storeState(h.doc(1, `{"id":"k2"}`))
	w := h.tx.bindings[0].waiting[linkSlot{"1", "companyId"}]
	require.Equal(t, now, w.tried)
	require.False(t, w.found)
}

func TestRetryWaitingRejectionReplaced(t *testing.T) {
	h := waitingJane(t)
	h.fake.seed("companies", record{"externalId": "k2"})
	h.now = h.now.Add(linkRetryEvery)
	h.fake.failNext("/rest/batch/people", 400, nil, messagesBody("Bad link"))
	h.tx.retryWaiting(t.Context())

	// A newer document sets the link anew, so the rejected one isn't tried again.
	h.now = h.now.Add(time.Minute)
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k2"}`))))
	require.Equal(t, h.only("companies", "k2")["id"], h.only("people", "1")["companyId"])
}

func TestRetryWaitingSkipsRejectedLink(t *testing.T) {
	h := newHarness(t, people(linkCompany+`,"rejected_records":"skip"`), companies(""))
	h.clock()
	require.NotNil(t, h.storeState(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`), h.doc(0, `{"id":2,"first":"Joe","company":"k1"}`)))
	h.fake.seed("companies", record{"externalId": "k1"})
	h.fake.reject = func(object string, rec record) string {
		if object == "people" && rec["companyId"] != nil {
			if row := h.fake.rows["people"][rec["id"].(string)]; row != nil && row["externalId"] == "1" {
				return "Bad link"
			}
		}
		return ""
	}

	// A saved link Twenty rejects under skip is left out for good, and the other is written.
	h.now = h.now.Add(linkRetryEvery)
	h.tx.retryWaiting(t.Context())
	require.Empty(t, h.tx.rejected)
	require.Empty(t, h.tx.bindings[0].waiting)
	require.Equal(t, map[string]int{"link_rejected": 1}, h.logged("retried waiting links")["skipped"])
	require.Equal(t, 1, h.logged("retried waiting links")["linkedLater"])
	require.JSONEq(t, `{}`, string(h.storeState(h.doc(1, `{"id":"k2"}`))))
}

func TestLinkRetriesPerRound(t *testing.T) {
	h := newHarness(t, people(linkCompany))
	b, now := h.tx.bindings[0], time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return now }
	for i := range maxLinkRetries + 100 {
		b.wait(linkSlot{fmt.Sprint(i), "companyId"}, "x", now)
	}
	triedNow := func() map[linkSlot]bool {
		out := make(map[linkSlot]bool)
		for slot, w := range b.waiting {
			if w.tried.Equal(now) {
				out[slot] = true
			}
		}
		return out
	}
	now = now.Add(linkRetryEvery)
	require.NoError(t, h.tx.linkLater(t.Context(), b, &stats{}))
	first := triedNow()
	require.Len(t, first, maxLinkRetries)
	// Running again within linkRetryEvery, as each transaction and the timer do, tries no more.
	now = now.Add(time.Minute)
	require.NoError(t, h.tx.linkLater(t.Context(), b, &stats{}))
	require.Empty(t, triedNow())

	// The next round starts with the ones left out.
	now = now.Add(linkRetryEvery)
	require.NoError(t, h.tx.linkLater(t.Context(), b, &stats{}))
	second := triedNow()
	require.Len(t, second, maxLinkRetries)
	for slot := range b.waiting {
		if !first[slot] {
			require.True(t, second[slot], "%v left out twice", slot)
		}
	}
}

func TestLinkRetriesSlidingWindow(t *testing.T) {
	var r retryBudget
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	require.Equal(t, 0, r.take(0, now))
	require.Equal(t, 100, r.take(100, now))
	// At the end of the window, the rest of the budget.
	now = now.Add(linkRetryEvery - time.Second)
	require.Equal(t, maxLinkRetries-100, r.take(maxLinkRetries, now))
	// Then only what the first take gives back, not a whole new budget.
	now = now.Add(time.Second)
	require.Equal(t, 100, r.take(maxLinkRetries, now))
	now = now.Add(linkRetryEvery - time.Second)
	require.Equal(t, maxLinkRetries-100, r.take(maxLinkRetries, now))
}

func TestLinkRetriesAfterRestartSoon(t *testing.T) {
	h := newHarness(t, people(linkCompany), companies(""))
	h.clock()
	var docs []storeItem
	for i := range maxLinkRetries + 1 {
		docs = append(docs, h.doc(0, fmt.Sprintf(`{"id":%d,"company":"k%d"}`, i, i)))
	}
	st := h.storeState(docs...)

	// A restart a minute later: the first tries of new links don't use the budget.
	h2 := newHarness(t, people(linkCompany), companies(""))
	h2.fake, h2.tx.client = h.fake, h.tx.client
	h.now = h.now.Add(time.Minute)
	h2.tx.now = func() time.Time { return h.now }
	require.NoError(t, h2.tx.UnmarshalState(st))
	require.Empty(t, h2.tx.bindings[0].retries)
	h2.tx.retryWaiting(t.Context())
	h2.storeState(h2.doc(1, `{"id":"k0"}`))
	h.now = h.now.Add(linkRetryEvery)
	h2.tx.retryWaiting(t.Context())
	require.Equal(t, h2.only("companies", "k0")["id"], h2.only("people", "0")["companyId"])
}

func TestLinkRetriesOverBudget(t *testing.T) {
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	r := retryBudget{{now, maxLinkRetries + 5}}
	require.Equal(t, 0, r.take(10, now))
	require.Equal(t, 0, r.take(0, now))
}

func TestLinkRestoredFutureTry(t *testing.T) {
	h := newHarness(t, people(linkCompany))
	h.clock()
	st := `{"waitingLinks":{"people/externalId":[{"key":"1","column":"companyId","target":"k1",
		"lookup":"companies.externalId","since":"2026-11-02T11:00:00Z","tried":"2026-11-02T13:00:00Z","found":true}]}}`
	require.NoError(t, h.tx.UnmarshalState(json.RawMessage(st)))
	w := h.tx.bindings[0].waiting[linkSlot{"1", "companyId"}]
	require.Equal(t, h.now, w.tried)
	require.True(t, w.found)
	require.Len(t, h.tx.bindings[0].retries, 1)
	st2, err := h.tx.state()
	require.NoError(t, err)
	require.Contains(t, string(st2), `"found":true`)
}

func TestLinkTriedSurvivesRestart(t *testing.T) {
	h := newHarness(t, people(linkCompany))
	b, now := h.tx.bindings[0], time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return now }
	for i := range maxLinkRetries + 100 {
		b.wait(linkSlot{fmt.Sprint(i), "companyId"}, "x", now)
	}
	now = now.Add(linkRetryEvery)
	require.NoError(t, h.tx.linkLater(t.Context(), b, &stats{}))
	round1 := now
	st, err := h.tx.state()
	require.NoError(t, err)

	// After a restart, the links tried a minute ago aren't due, and their tries use up the budget.
	h2 := newHarness(t, people(linkCompany))
	b2 := h2.tx.bindings[0]
	now = now.Add(time.Minute)
	h2.tx.now = func() time.Time { return now }
	require.NoError(t, h2.tx.UnmarshalState(st))
	require.Equal(t, 0, b2.retries.take(maxLinkRetries, now))
	now = now.Add(linkRetryEvery)
	require.NoError(t, h2.tx.linkLater(t.Context(), b2, &stats{}))
	// The ones left out before the restart go first.
	for slot, w := range b2.waiting {
		if !b.waiting[slot].tried.Equal(round1) {
			require.True(t, w.tried.Equal(now), "%v left out twice", slot)
		}
	}
}

func TestExpireLinksAfterLastTry(t *testing.T) {
	h := newHarness(t, people(linkCompany))
	b, now := h.tx.bindings[0], time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return now }
	for i := range maxLinkRetries + 100 {
		b.wait(linkSlot{fmt.Sprint(i), "companyId"}, "x", now)
	}
	// Past linkWaitFor, only the links looked for this round expire; the rest get their try first.
	now = now.Add(linkWaitFor + time.Minute)
	s := stats{}
	require.NoError(t, h.tx.linkLater(t.Context(), b, &s))
	require.Len(t, b.waiting, 100)
	require.Equal(t, maxLinkRetries, s.skipped["link_expired"])
	now = now.Add(linkRetryEvery)
	require.NoError(t, h.tx.linkLater(t.Context(), b, &stats{}))
	require.Empty(t, b.waiting)
}

func TestExpireLinksCap(t *testing.T) {
	h := newHarness(t, people(linkCompany))
	b, now := h.tx.bindings[0], time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	for i := range maxWaitingLinks + 2 {
		b.wait(linkSlot{fmt.Sprint(i), "companyId"}, "x", now.Add(time.Duration(i)*time.Second))
	}
	var s stats
	h.tx.expireLinks(b, &s, now)
	require.Len(t, b.waiting, maxWaitingLinks)
	require.NotContains(t, b.waiting, linkSlot{"0", "companyId"})
	require.NotContains(t, b.waiting, linkSlot{"1", "companyId"})
	require.Equal(t, map[string]int{"link_waiting_dropped": 2}, s.skipped)
	require.True(t, h.tx.changed)

	// A link whose record was found, but not written yet, is kept over newer ones.
	w := b.waiting[linkSlot{"2", "companyId"}]
	w.found = true
	b.waiting[linkSlot{"2", "companyId"}] = w
	b.wait(linkSlot{"new", "companyId"}, "x", now.Add(time.Hour))
	h.tx.expireLinks(b, &s, now)
	require.Contains(t, b.waiting, linkSlot{"2", "companyId"})
	require.NotContains(t, b.waiting, linkSlot{"3", "companyId"})
}

func TestStoreLinksByID(t *testing.T) {
	h := newHarness(t, people(`,"relations":[{"field":"company","id_field":"id"}]`))
	acme := h.fake.seed("companies", record{"externalId": "k1"})["id"].(string)
	trashed := h.fake.seed("companies", record{"externalId": "k2"})["id"].(string)
	h.fake.softDelete("companies", trashed)

	// Any case of the id links, and so does a trashed record, as with other keys.
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"`+strings.ToUpper(acme)+`"}`),
		h.doc(0, `{"id":2,"first":"Raj","company":"`+trashed+`"}`)))
	require.Equal(t, stats{received: 2, created: 2}, h.stats("people"))
	require.Equal(t, acme, h.only("people", "1")["companyId"])
	require.Equal(t, trashed, h.only("people", "2")["companyId"])

	// An id that isn't one Twenty takes, or isn't in Twenty, is left out and doesn't wait: Twenty
	// makes the ids, so one rarely arrives later.
	st := h.storeState(h.doc(0, `{"id":3,"first":"Ana","company":"k1"}`), h.doc(0, `{"id":4,"first":"Bo","company":"`+uuid.NewString()+`"}`),
		h.doc(0, `{"id":1,"company":"not an id"}`), h.doc(0, `{"id":6,"first":"Nil","company":"00000000-0000-0000-0000-000000000000"}`),
		h.doc(0, `{"id":7,"first":"Seven","company":"018f3e2a-7b4c-7d00-8000-0123456789ab"}`))
	require.Nil(t, st)
	require.Equal(t, stats{received: 5, created: 4, updated: 1,
		skipped: map[string]int{"company_link_not_a_twenty_id": 4, "company_link_id_not_in_twenty": 1}}, h.stats("people"))
	require.Nil(t, h.only("people", "3")["companyId"])
	require.Equal(t, acme, h.only("people", "1")["companyId"], "a link left out keeps the record's link")
	require.Empty(t, h.tx.bindings[0].waiting)
}

func ownedCompanies(idField string) testBinding {
	return testBinding{
		resource: `{"name":"acmeCo/companies","object":"companies","id_field":"externalId",` +
			`"relations":[{"field":"accountOwner","id_field":"` + idField + `","source_field":"owner"}]}`,
		collection: collection("acmeCo/companies", "string", proj("name", "/name", "string"), proj("owner", "/owner", "null", "string")),
	}
}

func seedMember(f *fakeTwenty, email, first, last string) string {
	return f.seed("workspaceMembers", record{"userEmail": email, "name": record{"firstName": first, "lastName": last}})["id"].(string)
}

func TestStoreMemberLinks(t *testing.T) {
	h := newHarness(t, ownedCompanies("userEmail"))
	ada := seedMember(h.fake, "Ada@Example.com", "Ada", "Lovelace")
	seedMember(h.fake, "sam@example.com", "Sam", "Lee")
	seedMember(h.fake, "SAM@example.com", "Sam", "Lee")
	gone := seedMember(h.fake, "old@example.com", "Old", "Hand")
	seedMember(h.fake, "strasse@example.de", "Max", "Strasse")
	h.fake.softDelete("workspaceMembers", gone)

	// Emails match in any case. Two members whose emails differ only in case are left out, and
	// a removed member is not in the workspace.
	before := h.fake.count(http.MethodGet, "/rest/workspaceMembers")
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","owner":" ada@EXAMPLE.com "}`), h.doc(0, `{"id":"k2","owner":"Sam@Example.com"}`),
		h.doc(0, `{"id":"k3","owner":"old@example.com"}`)))
	require.Equal(t, 1, h.fake.count(http.MethodGet, "/rest/workspaceMembers")-before, "members are read once")
	require.Equal(t, stats{received: 3, created: 3, skipped: map[string]int{"accountOwner_link_ambiguous": 1, "accountOwner_link_waiting": 1}},
		h.stats("companies"))
	require.Equal(t, ada, h.only("companies", "k1")["accountOwnerId"])
	require.Nil(t, h.only("companies", "k2")["accountOwnerId"])
	require.Nil(t, h.only("companies", "k3")["accountOwnerId"])
	// Logs don't name people.
	require.Equal(t, []string{"k3 -> <userEmail>"}, h.logged("links to records that aren't in Twenty yet wait for them")["examples"])
	require.NotContains(t, fmt.Sprint(h.logged("left out a link whose key can't name one record")), "example.com")
	// Emails are lowercased, not folded, so "ß" doesn't match "ss".
	require.NoError(t, h.store(h.doc(0, `{"id":"k4","owner":"STRAßE@example.de"}`)))
	require.Equal(t, stats{received: 1, created: 1, skipped: map[string]int{"accountOwner_link_waiting": 1}}, h.stats("companies"))

	// By name: "First Last", case and spacing ignored, accents kept.
	h = newHarness(t, ownedCompanies("name"))
	ada = seedMember(h.fake, "ada@example.com", "Ada", "Lovelace")
	seedMember(h.fake, "sam@example.com", "Sam", "Lee")
	seedMember(h.fake, "sam2@example.com", "sam", " LEE")
	zoe := seedMember(h.fake, "zoe@example.com", "Zoë", "Ng")
	maia := seedMember(h.fake, "maia@example.com", "Μαΐα", "Παππά")
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","owner":"  ADA   lovelace "}`), h.doc(0, `{"id":"k2","owner":"Sam Lee"}`),
		h.doc(0, `{"id":"k3","owner":"ZOË NG"}`), h.doc(0, `{"id":"k4","owner":"Zoe Ng"}`), h.doc(0, `{"id":"k5","owner":"   "}`),
		h.doc(0, `{"id":"k6","owner":"ΜΑΪ́Α ΠΑΠΠΆ"}`)))
	require.Equal(t, stats{received: 6, created: 6,
		skipped: map[string]int{"accountOwner_link_ambiguous": 1, "accountOwner_link_waiting": 1, "accountOwner_target_key_unusable": 1}},
		h.stats("companies"))
	require.Equal(t, ada, h.only("companies", "k1")["accountOwnerId"])
	require.Nil(t, h.only("companies", "k2")["accountOwnerId"])
	require.Equal(t, zoe, h.only("companies", "k3")["accountOwnerId"])
	require.Nil(t, h.only("companies", "k4")["accountOwnerId"])
	require.Equal(t, maia, h.only("companies", "k6")["accountOwnerId"], "case folding keeps names comparable")
}

func TestStoreMemberLinksLater(t *testing.T) {
	h := newHarness(t, ownedCompanies("name"))
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return now }

	// A member who isn't in the workspace yet: the link waits.
	st := h.storeState(h.doc(0, `{"id":"k1","owner":"Pat Doe"}`), h.doc(0, `{"id":"k2","owner":"Kim Roe"}`))
	require.Contains(t, string(st), `"target":"Pat Doe"`)

	// One member joins, and two members with the other name: the first links, the second is
	// dropped, not left to wait.
	pat := seedMember(h.fake, "pat@example.com", "Pat", "Doe")
	seedMember(h.fake, "kim@example.com", "Kim", "Roe")
	seedMember(h.fake, "kim2@example.com", "Kim", "Roe")
	now = now.Add(linkRetryEvery)
	require.JSONEq(t, `{}`, string(h.storeState()))
	require.Equal(t, pat, h.only("companies", "k1")["accountOwnerId"])
	require.Nil(t, h.only("companies", "k2")["accountOwnerId"])
	s := h.stats("companies")
	require.Equal(t, 1, s.linkedLater)
	require.Equal(t, map[string]int{"accountOwner_link_ambiguous": 1}, s.skipped)
}

func TestRetryWaitingRereadsMembers(t *testing.T) {
	h := newHarness(t, ownedCompanies("userEmail"))
	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	h.tx.now = func() time.Time { return now }
	require.NotNil(t, h.storeState(h.doc(0, `{"id":"k1","owner":"pat@example.com"}`)))

	// The member joins with no document arriving, and the retry round finds them.
	pat := seedMember(h.fake, "Pat@Example.com", "Pat", "Doe")
	now = now.Add(linkRetryEvery)
	h.tx.retryWaiting(t.Context())
	require.Equal(t, pat, h.only("companies", "k1")["accountOwnerId"])
	require.Empty(t, h.tx.bindings[0].waiting)
}

func TestPagesNeedNewCursor(t *testing.T) {
	for _, cursor := range []string{"", "c1"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `{"data":{"workspaceMembers":[{"id":"a"}]},"pageInfo":{"hasNextPage":true,"endCursor":%q}}`, cursor)
		}))
		c := newClient(srv.URL, fakeAPIKey, 60000, 1000)
		n := 0
		err := c.pages(t.Context(), "workspaceMembers", "deletedAt[is]:NULL", func(rs []record) error { n += len(rs); return nil })
		srv.Close()
		require.ErrorContains(t, err, "Twenty says there are more pages but gives no new cursor")
		require.LessOrEqual(t, n, 2, "it stops instead of looping")
	}
}

func TestResourceCreateFields(t *testing.T) {
	base := `{"name":"acmeCo/companies","object":"companies","id_field":"externalId","relations":[{"field":"company","id_field":"externalId","source_field":"companyKey"}],"create_fields":`
	for list, want := range map[string]string{
		`["region","seats"]`:                  "",
		`["zz1abc","ownerId"]`:                "",
		`["` + strings.Repeat("a", 63) + `"]`: "",
		`["` + strings.Repeat("a", 64) + `"]`: `"create_fields": "aaaa`,
		`["Region"]`:                          `isn't a name Twenty takes`,
		`["re_gion"]`:                         `isn't a name Twenty takes`,
		`["type"]`:                            `Twenty reserves the name "type"`,
		`["createdAt"]`:                       `Twenty reserves the name "createdAt"`,
		`["region","region"]`:                 `"region" is listed twice`,
		`["externalId"]`:                      "",
		`["company"]`:                         `"company" is a relation of the binding`,
		`["companyKey"]`:                      `"companyKey" is a relation of the binding`,
	} {
		var res resource
		require.NoError(t, json.Unmarshal([]byte(base+list+`}`), &res))
		if err := res.Validate(); want == "" {
			require.NoError(t, err, list)
		} else {
			require.ErrorContains(t, err, want, list)
		}
	}
}

func TestNewFieldType(t *testing.T) {
	items := func(p pf.Projection, types ...string) pf.Projection {
		p.Inference.Array = &pf.Inference_Array{ItemTypes: types}
		return p
	}
	date := proj("d", "/d", "string")
	date.Inference.String_ = &pf.Inference_String{Format: "date"}
	email := proj("e", "/e", "null", "string")
	email.Inference.String_ = &pf.Inference_String{Format: "email"}
	for _, tc := range []struct {
		p    pf.Projection
		want string
	}{
		{proj("a", "/a", "string"), "TEXT"},
		{email, "TEXT"},
		{dateTime(proj("a", "/a", "null", "string")), "DATE_TIME"},
		{date, "DATE"},
		{proj("a", "/a", "integer"), "NUMBER"},
		{proj("a", "/a", "number", "null"), "NUMBER"},
		{proj("a", "/a", "integer", "number"), "NUMBER"},
		{proj("a", "/a", "boolean"), "BOOLEAN"},
		{items(proj("a", "/a", "array"), "string"), "ARRAY"},
		{items(proj("a", "/a", "array"), "null", "string"), "RAW_JSON"},
		{items(proj("a", "/a", "array"), "integer"), "RAW_JSON"},
		{proj("a", "/a", "array"), "RAW_JSON"},
		{proj("a", "/a", "object"), "RAW_JSON"},
		{proj("a", "/a", "integer", "string"), ""},
		{proj("a", "/a", "null"), ""},
	} {
		f, why := newFieldType(&tc.p)
		require.Equal(t, tc.want, f.typ, "%v", tc.p.Inference)
		require.Equal(t, tc.want == "", why != "", why)
	}
	f, _ := newFieldType(&[]pf.Projection{proj("a", "/a", "integer")}[0])
	require.Equal(t, map[string]any{"type": "number", "decimals": 0}, f.settings)
}

func createdCompanies(list string) testBinding {
	seats := proj("seats", "/seats", "integer")
	return testBinding{
		resource: `{"name":"acmeCo/companies","object":"companies","id_field":"externalId","create_fields":` + list + `}`,
		collection: collection("acmeCo/companies", "string", proj("name", "/name", "string"), proj("region", "/region", "string"),
			seats, dateTime(proj("signedAt", "/signedAt", "string")), proj("extra", "/extra", "object"),
			proj("mixed", "/mixed", "integer", "string"), proj("unlisted", "/unlisted", "string")),
	}
}

func TestValidateCreateFields(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	validate := func(tb testBinding) (map[string][]*pm.Response_Validated_Constraint, error) {
		var res resource
		require.NoError(t, json.Unmarshal([]byte(tb.resource), &res))
		require.NoError(t, res.Validate())
		return validateBinding(res, obj, nil, &tb.collection)
	}

	cs, err := validate(createdCompanies(`["region","seats","signedAt","extra","mixed","name"]`))
	require.NoError(t, err)
	for field, want := range map[string]string{
		"region":   `REQUIRED The connector creates the Twenty field "region" as TEXT when it applies`,
		"seats":    `REQUIRED The connector creates the Twenty field "seats" as NUMBER when it applies`,
		"signedAt": `REQUIRED The connector creates the Twenty field "signedAt" as DATE_TIME when it applies`,
		"extra":    `REQUIRED The connector creates the Twenty field "extra" as RAW_JSON when it applies`,
		"mixed":    `FORBIDDEN The connector can't pick a Twenty field type for integer or string values; create the field "mixed" in Twenty`,
		// A field Twenty has is written as usual.
		"name":     `OPTIONAL This field can be written to the Twenty field name`,
		"unlisted": `FORBIDDEN The Twenty object "companies" has no field "unlisted"`,
	} {
		c := cs[field][0]
		require.Equal(t, want, strings.TrimPrefix(c.Type.String(), "FIELD_")+" "+c.Reason, field)
	}

	_, err = validate(createdCompanies(`["nowhere"]`))
	require.ErrorContains(t, err, `"create_fields": the collection "acmeCo/companies" has no field "nowhere"`)

	// The key and the document aren't written as fields, so listing them is refused.
	aliased := func(list string) testBinding {
		tb := createdCompanies(list)
		tb.collection.Projections = append(tb.collection.Projections,
			pf.Projection{Field: "customerKey", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
			pf.Projection{Field: "doc", Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST}})
		slices.SortFunc(tb.collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
		return tb
	}
	_, err = validate(aliased(`["customerKey"]`))
	require.ErrorContains(t, err, `"create_fields": "customerKey" is the key of the collection "acmeCo/companies", which the connector writes to the ID Field "externalId"`)
	_, err = validate(aliased(`["doc"]`))
	require.ErrorContains(t, err, `"create_fields": "doc" is the whole document of the collection "acmeCo/companies"`)
}

func TestApplyCreatesFields(t *testing.T) {
	f, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	tb := createdCompanies(`["region","seats","signedAt","extra"]`)
	spec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
		ResourceConfigJson: json.RawMessage(tb.resource),
		Collection:         tb.collection,
		// extra isn't selected, so it isn't created.
		FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name", "region", "seats", "signedAt"}, Document: "flow_document"},
	}}}
	// A second binding wanting the same field as the same type creates it once.
	other := createdCompanies(`["region"]`)
	other.collection.Name = "acmeCo/more"
	spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
		ResourceConfigJson: json.RawMessage(strings.Replace(other.resource, "acmeCo/companies", "acmeCo/more", 1)),
		Collection:         other.collection,
		FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"region"}, Document: "flow_document"},
	})

	created, err := createFields(t.Context(), c, objects, spec)
	require.NoError(t, err)
	require.Equal(t, []string{"companies.region (TEXT)", "companies.seats (NUMBER)", "companies.signedAt (DATE_TIME)"}, created)
	require.Equal(t, []string{"companies.region:TEXT", "companies.seats:NUMBER", "companies.signedAt:DATE_TIME"}, f.created)
	require.Equal(t, map[string]any{"type": "number", "decimals": json.Number("0")}, f.objects["companies"].fields["seats"].settings)
	require.Equal(t, "created Twenty fields: companies.region (TEXT), companies.seats (NUMBER), companies.signedAt (DATE_TIME)", appliedDescription(created))

	// Applying again creates nothing, and the new fields are written like any other.
	objects, err = c.objects(t.Context())
	require.NoError(t, err)
	created, err = createFields(t.Context(), c, objects, spec)
	require.NoError(t, err)
	require.Empty(t, created)
	require.Empty(t, appliedDescription(created))
	h := harnessFor(t, c, createdCompanies(`["region","seats","signedAt"]`))
	h.fake = f
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","region":"EMEA","seats":12,"signedAt":"2026-11-03T10:00:00Z"}`)))
	rec := h.only("companies", "k1")
	require.Equal(t, "EMEA", rec["region"])
	require.Equal(t, json.Number("12"), rec["seats"])

	// A spec that refers to its collections by index creates the same fields.
	f, c = newFakeTwenty(t)
	objects, err = c.objects(t.Context())
	require.NoError(t, err)
	linked := &pf.MaterializationSpec{
		LinkedCollections: []pf.CollectionSpec{tb.collection},
		Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(tb.resource),
			CollectionIndex:    0,
			FieldSelection:     spec.Bindings[0].FieldSelection,
		}},
	}
	created, err = createFields(t.Context(), c, objects, linked)
	require.NoError(t, err)
	require.Equal(t, []string{"companies.region (TEXT)", "companies.seats (NUMBER)", "companies.signedAt (DATE_TIME)"}, created)

	// Two bindings that would create one field as different types fail before creating anything.
	f, c = newFakeTwenty(t)
	objects, err = c.objects(t.Context())
	require.NoError(t, err)
	clash := createdCompanies(`["region"]`)
	clash.collection.Projections = slices.DeleteFunc(clash.collection.Projections, func(p pf.Projection) bool { return p.Field == "region" })
	clash.collection.Projections = append(clash.collection.Projections, proj("region", "/region", "integer"))
	slices.SortFunc(clash.collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	spec.Bindings[1].Collection = clash.collection
	_, err = createFields(t.Context(), c, objects, spec)
	require.ErrorContains(t, err, `would create the Twenty field companies.region as TEXT and as NUMBER`)
	require.Empty(t, f.created)
	// Validate finds the clash first.
	seen := make(map[[2]string][2]string)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	for _, b := range spec.Bindings {
		var res resource
		require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &res))
		err = checkCreates(seen, &b.Collection, res, obj)
	}
	require.ErrorContains(t, err, `the collections "acmeCo/companies" and "acmeCo/companies" would create the Twenty field companies.region as TEXT and as NUMBER`)

	// An integer and a number for one field make a plain NUMBER, whichever binding comes first.
	for _, order := range [][2]string{{"integer", "number"}, {"number", "integer"}} {
		f, c = newFakeTwenty(t)
		objects, err = c.objects(t.Context())
		require.NoError(t, err)
		mixed := &pf.MaterializationSpec{}
		for i, typ := range order {
			tb := createdCompanies(`["count"]`)
			tb.collection.Name = pf.Collection(fmt.Sprintf("acmeCo/c%d", i))
			tb.collection.Projections = append(tb.collection.Projections, proj("count", "/count", typ))
			slices.SortFunc(tb.collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
			mixed.Bindings = append(mixed.Bindings, &pf.MaterializationSpec_Binding{
				ResourceConfigJson: json.RawMessage(tb.resource),
				Collection:         tb.collection,
				FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"count"}, Document: "flow_document"},
			})
		}
		_, err = createFields(t.Context(), c, objects, mixed)
		require.NoError(t, err)
		require.Equal(t, []string{"companies.count:NUMBER"}, f.created)
		require.Nil(t, f.objects["companies"].fields["count"].settings, order)
	}
}

func TestApplyCreateFieldFailures(t *testing.T) {
	tb := createdCompanies(`["region","seats"]`)
	spec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
		ResourceConfigJson: json.RawMessage(tb.resource),
		Collection:         tb.collection,
		FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"region", "seats"}, Document: "flow_document"},
	}}}

	// Twenty refuses the second field: the first stays, and the error says so with Twenty's reason.
	f, c := newFakeTwenty(t)
	c.sleep = func(context.Context, time.Duration) error { return nil }
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	f.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/rest/metadata/fields" && len(f.created) == 1 {
			f.objects["companies"].fields["seats"] = fakeField{typ: "TEXT"} // made in Twenty meanwhile, as another type
		}
	}
	created, err := createFields(t.Context(), c, objects, spec)
	require.ErrorContains(t, err, `creating the Twenty field companies.seats (NUMBER): Twenty refused the field (HTTP 400)`)
	require.ErrorContains(t, err, `Name "seats" is not available as it is already used by another field`)
	require.ErrorContains(t, err, `(fields created before it: companies.region (TEXT))`)
	require.Equal(t, []string{"companies.region (TEXT)"}, created)

	// A create Twenty applied whose answer was lost: the retry finds the name taken, and the
	// connector checks that the field is there as planned.
	f, c = newFakeTwenty(t)
	c.sleep = func(context.Context, time.Duration) error { return nil }
	objects, err = c.objects(t.Context())
	require.NoError(t, err)
	lost := false
	f.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/rest/metadata/fields" && !lost {
			lost = true
			f.objects["companies"].fields["region"] = fakeField{typ: "TEXT"}
			f.failures = append(f.failures, fakeFailure{path: "/rest/metadata/fields", status: 502})
		}
	}
	created, err = createFields(t.Context(), c, objects, spec)
	require.NoError(t, err)
	require.Equal(t, []string{"companies.region (TEXT)", "companies.seats (NUMBER)"}, created)
}

func TestParseVersion(t *testing.T) {
	for in, want := range map[string]version{"v2.52.0": {2, 52, 0, 1}, "2.35.1": {2, 35, 1, 1}, " v2.34.0-canary ": {2, 34, 0, 0}, "v3.0.0+build.1": {3, 0, 0, 1}} {
		got, ok := parseVersion(in)
		require.True(t, ok, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "dev", "v2.52", "v2.x.0", "v2.-1.0", "<html>"} {
		_, ok := parseVersion(in)
		require.False(t, ok, in)
	}
}

func TestCheckDeleteVersion(t *testing.T) {
	f, c := newFakeTwenty(t)
	asks := func() int {
		n := 0
		for _, call := range f.calls {
			if call.path == "/client-config" {
				n++
			}
		}
		return n
	}

	// Bindings that don't delete send no request, whatever the version.
	f.version = "v2.30.0"
	require.NoError(t, checkDeleteVersion(t.Context(), c, nil))
	require.Zero(t, asks())

	// The page is asked without the API key.
	f.hook = func(r *http.Request) {
		if r.URL.Path == "/client-config" {
			require.Empty(t, r.Header.Get("Authorization"))
			require.Equal(t, userAgent, r.UserAgent())
		}
	}
	for version, refused := range map[string]bool{"v2.30.0": true, "v2.34.9": true, "v2.35.0-canary": true, "v2.35.0": false,
		"v2.35.1-rc.1": false, "v2.52.0": false, "v3.0.0": false, "dev": false, "": false} {
		f.version = version
		err := checkDeleteVersion(t.Context(), c, []string{"acmeCo/companies", "acmeCo/people"})
		if refused {
			require.ErrorContains(t, err, "Twenty "+version+` skips the deletes the connector sends, without an error, before v2.35. `+
				`Upgrade Twenty, or set delete_mode to "flag" or "ignore" for acmeCo/companies, acmeCo/people`)
		} else {
			require.NoError(t, err, version)
		}
	}

	// A redirect isn't followed: a page that isn't Twenty's, like a proxy's sign-in, is taken as no
	// version. Here it leads back to the page, which would report an old version.
	f.version = "v2.30.0"
	f.failNext("/client-config", 302, http.Header{"Location": {c.baseURL + "/client-config"}}, nil)
	require.NoError(t, checkDeleteVersion(t.Context(), c, []string{"acmeCo/companies"}))

	// Only a page that answers 200 is read.
	f.failNext("/client-config", 503, nil, map[string]any{"appVersion": "v2.30.0"})
	require.NoError(t, checkDeleteVersion(t.Context(), c, []string{"acmeCo/companies"}))

	// Twenty Cloud runs the latest version, so it isn't asked.
	cloud := newClient(defaultBaseURL, fakeAPIKey, 60000, 1000)
	cloud.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		t.Errorf("asked Twenty Cloud for %s", r.URL)
		return nil, errors.New("no network in tests")
	})}
	require.NoError(t, checkDeleteVersion(t.Context(), cloud, []string{"acmeCo/companies"}))
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOpenRefusesDeletesOnOldTwenty(t *testing.T) {
	f, c := newFakeTwenty(t)
	f.version = "v2.32.0"
	spec := func(options string) *pf.MaterializationSpec {
		tb := companies(options)
		return &pf.MaterializationSpec{
			ConfigJson: json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `"}`),
			Bindings: []*pf.MaterializationSpec_Binding{{
				ResourceConfigJson: json.RawMessage(tb.resource),
				Collection:         tb.collection,
				FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}, Document: "flow_document"},
			}},
		}
	}
	_, _, err := open(t.Context(), spec(`,"delete_mode":"trash"`))
	require.ErrorContains(t, err, "Twenty v2.32.0 skips the deletes")
	_, _, err = open(t.Context(), spec(`,"delete_mode":"delete"`))
	require.ErrorContains(t, err, "Twenty v2.32.0 skips the deletes")
	for _, options := range []string{"", `,"delete_mode":"ignore"`, flagDeletes} {
		_, _, err = open(t.Context(), spec(options))
		require.NoError(t, err, options)
	}
}

func TestConnectOldMetadataAPI(t *testing.T) {
	f, c := newFakeTwenty(t)
	f.failNext("/rest/metadata/objects", 400, nil, map[string]any{"statusCode": 400, "error": "Bad Request",
		"message": `Query path '/rest/metadata/objects?limit=200' invalid. Metadata path "objects?limit=200" does not exist. Valid examples: /rest/metadata/fields or /rest/metadata/objects`})
	_, _, err := connect(t.Context(), config{BaseURL: c.baseURL, APIKey: fakeAPIKey})
	require.ErrorContains(t, err, "Twenty refused the connector's request for its objects, which Twenty older than v2.30 does. The connector needs Twenty v2.30 or later, and v2.35 or later to delete records: ")
	require.ErrorContains(t, err, `Metadata path "objects?limit=200" does not exist`)
	var user *cerrors.UserError
	require.ErrorAs(t, err, &user)

	// The same words with another status, or another 400, keep their own message.
	f.failNext("/rest/metadata/objects", 422, nil, messagesBody(`Metadata path "x" does not exist. Valid examples: none`))
	_, _, err = connect(t.Context(), config{BaseURL: c.baseURL, APIKey: fakeAPIKey})
	require.NotContains(t, err.Error(), "v2.30")

	f.failNext("/rest/metadata/objects", 400, nil, messagesBody("something else"))
	_, _, err = connect(t.Context(), config{BaseURL: c.baseURL, APIKey: fakeAPIKey})
	require.ErrorContains(t, err, "something else")
	require.NotContains(t, err.Error(), "v2.30")
}

func TestValidateRefusesKeyChange(t *testing.T) {
	_, c := newFakeTwenty(t)
	config := json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `"}`)
	tb := companies("")
	last := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
		ResourceConfigJson: json.RawMessage(tb.resource), Collection: tb.collection,
		FieldSelection: pf.FieldSelection{Keys: []string{"id"}},
	}}}
	check := func(groupBy ...string) error {
		_, err := validate(t.Context(), &pm.Request_Validate{
			Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config, LastMaterialization: last,
			Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: json.RawMessage(tb.resource), Collection: tb.collection, GroupBy: groupBy}},
		})
		return err
	}
	require.NoError(t, check())
	require.NoError(t, check("id"))
	require.ErrorContains(t, check("accountCode", "id"), `writes the key /id to "externalId", and can't change it to /code, /id`)
}

func TestValidateAndApplyRefuseDeletesOnOldTwenty(t *testing.T) {
	f, c := newFakeTwenty(t)
	f.version = "v2.32.0"
	config := json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `"}`)
	check := func(options string) error {
		tb := companies(options)
		// validate, since the test collections are too sparse for the request's own checks.
		_, err := validate(t.Context(), &pm.Request_Validate{
			Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config,
			Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: json.RawMessage(tb.resource), Collection: tb.collection}},
		})
		return err
	}
	require.ErrorContains(t, check(`,"delete_mode":"trash"`), "Twenty v2.32.0 skips the deletes")
	require.ErrorContains(t, check(`,"delete_mode":"delete"`), "Twenty v2.32.0 skips the deletes")
	for _, options := range []string{"", flagDeletes} {
		require.NoError(t, check(options), options)
	}

	// Apply refuses before it creates a field.
	tb := createdCompanies(`["region"]`)
	tb.resource = strings.TrimSuffix(tb.resource, "}") + `,"delete_mode":"trash"}`
	_, err := apply(t.Context(), &pm.Request_Apply{Materialization: &pf.MaterializationSpec{
		Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config,
		Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(tb.resource),
			ResourcePath:       []string{"companies", "externalId"},
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name", "region"}, Document: "flow_document"},
		}},
	}})
	require.ErrorContains(t, err, "Twenty v2.32.0 skips the deletes")
	require.NotContains(t, f.objects["companies"].fields, "region")
}

func TestCreateOwnFields(t *testing.T) {
	f, c := newFakeTwenty(t)
	config := json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `","advanced":{"requests_per_minute":6000}}`)
	own := `{"name":"acmeCo/companies","object":"companies","id_field":"crmKey","delete_mode":"flag","deleted_field":"crmGone","deleted_at_field":"crmGoneAt","create_fields":["crmKey","crmGone","crmGoneAt"]}`
	tb := companies("")
	tb.resource = own
	link := people(`,"relations":[{"field":"company","id_field":"crmKey","source_field":"company"}]`)
	validateWith := func(company string) error {
		_, err := validate(t.Context(), &pm.Request_Validate{
			Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config,
			Bindings: []*pm.Request_Validate_Binding{
				{ResourceConfigJson: json.RawMessage(company), Collection: tb.collection},
				{ResourceConfigJson: json.RawMessage(link.resource), Collection: link.collection},
			},
		})
		return err
	}
	// Validate counts the fields the binding will create, and so does a relation to its ID field.
	require.NoError(t, validateWith(own))
	notListed := strings.Replace(own, `"create_fields":["crmKey","crmGone","crmGoneAt"]`, `"create_fields":["crmGone","crmGoneAt"]`, 1)
	require.ErrorContains(t, validateWith(notListed), `"id_field": the Twenty object "companies" has no field "crmKey"`)
	require.Empty(t, f.created)

	// Open checks Twenty itself, so it fails until Apply creates them.
	spec := &pf.MaterializationSpec{
		Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config,
		Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(own),
			ResourcePath:       []string{"companies", "crmKey"},
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}, Document: "flow_document"},
		}},
	}
	_, _, err := open(t.Context(), spec)
	require.ErrorContains(t, err, `has no field "crmKey"`)
	_, err = apply(t.Context(), &pm.Request_Apply{Materialization: spec})
	require.NoError(t, err)
	require.Equal(t, []string{"companies.crmGone:BOOLEAN", "companies.crmGoneAt:DATE_TIME", "companies.crmKey:TEXT"}, f.created)
	require.True(t, f.objects["companies"].fields["crmKey"].unique)
	require.False(t, f.objects["companies"].fields["crmGone"].unique)
	_, _, err = open(t.Context(), spec)
	require.NoError(t, err)

	// A field that exists isn't changed: one that isn't unique stays an error.
	f, c = newFakeTwenty(t)
	f.objects["companies"].fields["crmKey"] = fakeField{typ: "TEXT"}
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	spec.ConfigJson = json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `","advanced":{"requests_per_minute":6000}}`)
	created, err := createFields(t.Context(), c, objects, spec)
	require.NoError(t, err)
	require.Equal(t, []string{"companies.crmGone (BOOLEAN)", "companies.crmGoneAt (DATE_TIME)"}, created)
	require.False(t, f.objects["companies"].fields["crmKey"].unique)
	_, _, err = open(t.Context(), spec)
	require.ErrorContains(t, err, `companies.crmKey must be unique`)
	// A create whose answer was lost comes back from the retry as a name already taken: it counts
	// as created only if Twenty made the field unique.
	for _, unique := range []bool{true, false} {
		f, c := newFakeTwenty(t)
		f.hook = func(r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Path == "/rest/metadata/fields" {
				f.objects["companies"].fields["crmKey"] = fakeField{typ: "TEXT", unique: unique}
			}
		}
		objects, err := c.objects(t.Context())
		require.NoError(t, err)
		spec := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(`{"name":"acmeCo/companies","object":"companies","id_field":"crmKey","create_fields":["crmKey"]}`),
			Collection:         tb.collection,
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"name"}, Document: "flow_document"},
		}}}
		created, err := createFields(t.Context(), c, objects, spec)
		if unique {
			require.NoError(t, err)
			require.Equal(t, []string{"companies.crmKey (TEXT)"}, created)
		} else {
			require.ErrorContains(t, err, `creating the Twenty field companies.crmKey (TEXT)`)
		}
	}
	// After a lost answer, a field found but not unique doesn't count as created.
	require.False(t, createdAs(t.Context(), c, [2]string{"companies", "crmKey"}, newField{typ: "TEXT", unique: true}))
	require.True(t, createdAs(t.Context(), c, [2]string{"companies", "crmKey"}, newField{typ: "TEXT"}))

	// Two bindings can't create one field as different types.
	objects, err = c.objects(t.Context())
	require.NoError(t, err)
	var a, b resource
	require.NoError(t, json.Unmarshal([]byte(own), &a))
	require.NoError(t, json.Unmarshal([]byte(`{"name":"acmeCo/b","object":"companies","id_field":"externalId","delete_mode":"flag","deleted_field":"crmOther","deleted_at_field":"crmNew","create_fields":["crmNew"]}`), &b))
	a.DeletedField, a.CreateFields = "crmNew", []string{"crmNew"}
	before := len(objects[slices.IndexFunc(objects, func(o objectMeta) bool { return o.NamePlural == "companies" })].Fields)
	_, _, err = withOwnFields(objects, []resource{a, b}, []string{"acmeCo/a", "acmeCo/b"})
	require.ErrorContains(t, err, `the collections "acmeCo/a" and "acmeCo/b" would create the Twenty field companies.crmNew as BOOLEAN and as DATE_TIME`)
	b.CreateFields = nil
	_, _, err = withOwnFields(objects, []resource{a, b}, []string{"acmeCo/a", "acmeCo/b"})
	require.NoError(t, err)
	require.Len(t, objects[slices.IndexFunc(objects, func(o objectMeta) bool { return o.NamePlural == "companies" })].Fields, before, "the objects themselves are unchanged")
}

func TestCreateOwnAndCollectionField(t *testing.T) {
	// One binding creates its ID field crmKey, and another creates a collection field of that name.
	for _, typ := range []string{"string", "integer"} {
		for _, ownFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s,own first=%t", typ, ownFirst), func(t *testing.T) {
				f, c := newFakeTwenty(t)
				config := json.RawMessage(`{"base_url":"` + c.baseURL + `","api_key":"` + fakeAPIKey + `","advanced":{"requests_per_minute":6000}}`)
				own := companies("")
				own.resource = `{"name":"acmeCo/companies","object":"companies","id_field":"crmKey","create_fields":["crmKey"]}`
				other := createdCompanies(`["crmKey"]`)
				other.collection.Name = "acmeCo/more"
				other.collection.Projections = append(other.collection.Projections, proj("crmKey", "/crmKey", typ))
				slices.SortFunc(other.collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
				other.resource = strings.Replace(other.resource, "acmeCo/companies", "acmeCo/more", 1)
				pairs := [][2]any{{own, []string{"name"}}, {other, []string{"crmKey", "name"}}}
				if !ownFirst {
					pairs[0], pairs[1] = pairs[1], pairs[0]
				}
				var vb []*pm.Request_Validate_Binding
				spec := &pf.MaterializationSpec{Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config}
				for i, p := range pairs {
					tb, values := p[0].(testBinding), p[1].([]string)
					vb = append(vb, &pm.Request_Validate_Binding{ResourceConfigJson: json.RawMessage(tb.resource), Collection: tb.collection})
					spec.Bindings = append(spec.Bindings, &pf.MaterializationSpec_Binding{
						ResourceConfigJson: json.RawMessage(tb.resource), ResourcePath: []string{"companies", fmt.Sprint(i)}, Collection: tb.collection,
						FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: values, Document: "flow_document"},
					})
				}
				_, verr := validate(t.Context(), &pm.Request_Validate{Name: "acmeCo/twenty", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: config, Bindings: vb})
				_, aerr := apply(t.Context(), &pm.Request_Apply{Materialization: spec})
				if typ == "integer" {
					require.ErrorContains(t, verr, "would create the Twenty field companies.crmKey as")
					require.ErrorContains(t, aerr, "would create the Twenty field companies.crmKey as")
					require.Empty(t, f.created)
					return
				}
				require.NoError(t, verr)
				require.NoError(t, aerr)
				require.Equal(t, []string{"companies.crmKey:TEXT"}, f.created)
				require.True(t, f.objects["companies"].fields["crmKey"].unique, "the ID field is unique whichever binding plans it first")
			})
		}
	}
}

func TestStoreKeepNulls(t *testing.T) {
	first := `{"id":"k1","name":"Acme","domain":"acme.com","city":"Paris","country":"FR","revenue":{"amountMicros":5000000,"currencyCode":"USD"},"employees":10}`
	nulls := `{"id":"k1","name":null,"domain":null,"city":null,"revenue":{"amountMicros":null,"currencyCode":"EUR"},"employees":null}`

	h := newHarness(t, companies(`,"null_values":"keep"`))
	require.NoError(t, h.store(h.doc(0, first)))
	require.NoError(t, h.store(h.doc(0, nulls)))
	rec := h.only("companies", "k1")
	require.Equal(t, "Acme", rec["name"])
	require.Equal(t, "acme.com", rec["domainName"].(map[string]any)["primaryLinkUrl"])
	require.Equal(t, "Paris", rec["address"].(map[string]any)["addressCity"])
	require.Equal(t, "FR", rec["address"].(map[string]any)["addressCountry"])
	require.Equal(t, map[string]any{"amountMicros": json.Number("5000000"), "currencyCode": "EUR"}, rec["annualRevenue"])
	require.Equal(t, json.Number("10"), rec["employees"])

	// In one transaction too: the earlier document's values stay.
	h = newHarness(t, companies(`,"null_values":"keep"`))
	require.NoError(t, h.store(h.doc(0, first), h.doc(0, nulls)))
	require.Equal(t, "Acme", h.only("companies", "k1")["name"])

	// A composite with only nulls is left out.
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","revenue":{"amountMicros":null,"currencyCode":null}}`)))
	require.Equal(t, map[string]any{"amountMicros": json.Number("5000000"), "currencyCode": "EUR"}, h.only("companies", "k1")["annualRevenue"])

	// A null keeps a unique value, so another record can't take it in the same transaction.
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","domain":null}`), h.doc(0, `{"id":"k2","name":"Beta","domain":"acme.com"}`)))
	require.Equal(t, "acme.com", h.only("companies", "k1")["domainName"].(map[string]any)["primaryLinkUrl"])
	require.Empty(t, h.only("companies", "k2")["domainName"].(map[string]any)["primaryLinkUrl"])

	// By default, a null clears.
	h = newHarness(t, companies(""))
	require.NoError(t, h.store(h.doc(0, first)))
	require.NoError(t, h.store(h.doc(0, nulls)))
	rec = h.only("companies", "k1")
	require.Empty(t, rec["name"])
	require.Nil(t, rec["employees"])
	require.Nil(t, rec["annualRevenue"].(map[string]any)["amountMicros"])
}

func TestStoreKeepNullLinks(t *testing.T) {
	h := newHarness(t, people(linkCompany+`,"null_values":"keep"`), companies(""))
	require.NoError(t, h.store(h.doc(1, `{"id":"k1","name":"Acme"}`)))
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":"Jane","company":"k1"}`)))
	acme := h.only("companies", "k1")["id"]
	require.Equal(t, acme, h.only("people", "1")["companyId"])
	// A null link is kept, and an empty one still clears it: no record has an empty key.
	require.NoError(t, h.store(h.doc(0, `{"id":1,"first":null,"company":null}`)))
	require.Equal(t, acme, h.only("people", "1")["companyId"])
	require.Equal(t, "Jane", h.only("people", "1")["name"].(map[string]any)["firstName"])
	require.NoError(t, h.store(h.doc(0, `{"id":1,"company":""}`)))
	require.Nil(t, h.only("people", "1")["companyId"])
}

func TestResourceNullValues(t *testing.T) {
	for _, v := range []string{"", nullClear, nullKeep} {
		require.NoError(t, resource{Object: "companies", IDField: "externalId", NullValues: v}.Validate(), v)
	}
	require.ErrorContains(t, resource{Object: "companies", IDField: "externalId", NullValues: "skip"}.Validate(), `"null_values" must be "clear" or "keep"`)
}

func TestStoreAdopt(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	taken := h.fake.seed("companies", record{"name": "Other", "externalId": "other", "domainName": map[string]any{"primaryLinkUrl": "beta.io"}})
	trashed := h.fake.seed("companies", record{"name": "Trashed", "domainName": map[string]any{"primaryLinkUrl": "gamma.io"}})
	h.fake.softDelete("companies", trashed["id"].(string))
	twice := h.fake.seed("companies", record{"name": "Twice", "domainName": map[string]any{"primaryLinkUrl": "delta.io"}})

	require.NoError(t, h.store(
		h.doc(0, `{"id":"k1","name":"Acme","domain":"https://www.ACME.com"}`),
		h.doc(0, `{"id":"k2","name":"Beta","domain":"beta.io"}`),
		h.doc(0, `{"id":"k3","name":"Gamma","domain":"gamma.io"}`),
		h.doc(0, `{"id":"k4","name":"Delta","domain":"delta.io"}`),
		h.doc(0, `{"id":"k5","name":"Delta again","domain":"delta.io"}`),
	))
	// A live record with an empty ID field and the same domain becomes the document's record.
	acme := h.only("companies", "k1")
	require.Equal(t, []any{hand["id"], "Acme"}, []any{acme["id"], acme["name"]})
	// One whose ID field holds another key, or that is in the trash, is never taken.
	require.Equal(t, "other", h.fake.row("companies", taken["id"].(string))["externalId"])
	require.NotEqual(t, taken["id"], h.only("companies", "k2")["id"])
	require.Equal(t, "", h.fake.row("companies", trashed["id"].(string))["externalId"])
	require.NotEqual(t, trashed["id"], h.only("companies", "k3")["id"])
	// Two documents with one domain: the first adopts, the second meets the value held.
	require.Equal(t, twice["id"], h.only("companies", "k4")["id"])
	require.NotEqual(t, twice["id"], h.only("companies", "k5")["id"])
	st := h.stats("companies")
	require.Equal(t, 2, st.adopted)
	require.Equal(t, 3, st.skipped["domainName_held_by_another_record"])
	// Only the two live records with empty ID fields were asked for: none lost a race.
	require.Equal(t, 2, h.fake.count(http.MethodPatch, "/rest/companies"))

	// A record someone takes between the lookup and the write stays theirs.
	race := h.fake.seed("companies", record{"name": "Race", "domainName": map[string]any{"primaryLinkUrl": "eps.io"}})
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPatch {
			h.fake.rows["companies"][race["id"].(string)]["externalId"] = "person"
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"k6","name":"Eps","domain":"eps.io"}`)))
	h.fake.hook = nil
	require.Equal(t, "person", h.fake.row("companies", race["id"].(string))["externalId"])
	require.NotEqual(t, race["id"], h.only("companies", "k6")["id"])
	require.Equal(t, 0, h.stats("companies").adopted)
	require.NotNil(t, h.logged("a record to adopt changed since the lookup, so the document is written as a new record"))

	// Twenty refusing the claim stops the task rather than creating a duplicate.
	h.fake.seed("companies", record{"name": "Zeta", "domainName": map[string]any{"primaryLinkUrl": "zeta.io"}})
	h.fake.mu.Lock()
	h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPatch, path: "/rest/companies", status: 400, body: messagesBody("Invalid filter")})
	h.fake.mu.Unlock()
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k7","name":"Zeta","domain":"zeta.io"}`)),
		`companies: adopting the record with the same domainName for the document with key "k7"`)
	require.Empty(t, h.fake.byKey("companies", "k7"))
}

func TestAdoptByChecks(t *testing.T) {
	for _, tc := range []struct{ options, wantErr string }{
		{`,"adopt_by":"externalId"`, "not its ID Field"},
		{`,"adopt_by":"domainName","delete_mode":"trash"`, `works only with "delete_mode" "ignore" or "flag"`},
		{`,"adopt_by":"Bad-name"`, "must name a Twenty field"},
	} {
		var res resource
		require.NoError(t, json.Unmarshal([]byte(companies(tc.options).resource), &res))
		require.ErrorContains(t, res.Validate(), tc.wantErr, tc.options)
	}
	require.ErrorContains(t, checkAdoptAlone([]resource{{Object: "companies", AdoptBy: "domainName"}, {Object: "companies", IDField: "other"}}),
		`must be the only binding that writes "companies"`)
	require.NoError(t, checkAdoptAlone([]resource{{Object: "companies", AdoptBy: "domainName"}, {Object: "people"}}))
	// The field must be a unique field the binding writes.
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	for _, tc := range []struct {
		adopt  string
		values []string
	}{{"name", []string{"name", "domainName"}}, {"domainName", []string{"name"}}} {
		b := &pf.MaterializationSpec_Binding{Collection: companiesCollection(),
			FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: tc.values, Document: "flow_document"}}
		_, err = newBinding(resource{Object: "companies", IDField: "externalId", AdoptBy: tc.adopt}, obj, nil, &pf.MaterializationSpec{}, b)
		require.ErrorContains(t, err, `doesn't write "`+tc.adopt+`", or it isn't a unique Text, Links or Emails field`)
	}
}

func TestAdoptReleasedOnRejection(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName","rejected_records":"skip"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	h.fake.mu.Lock()
	h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 400, body: messagesBody("bad value")})
	h.fake.mu.Unlock()
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	// The document was skipped, so the record goes back to having no key, with its values.
	row := h.fake.row("companies", hand["id"].(string))
	require.Equal(t, []any{"", "By hand"}, []any{row["externalId"], row["name"]})
	st := h.stats("companies")
	require.Equal(t, []int{0, 1}, []int{st.adopted, st.rejected})
	// The next time, the document adopts it.
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	require.Equal(t, hand["id"], h.only("companies", "k1")["id"])
}

func TestValidateAdoptBy(t *testing.T) {
	_, c := newFakeTwenty(t)
	objects, err := c.objects(t.Context())
	require.NoError(t, err)
	obj, err := findObject(objects, "companies")
	require.NoError(t, err)
	coll := companiesCollection()
	cs, err := validateBinding(resource{Object: "companies", IDField: "externalId", AdoptBy: "domainName"}, obj, nil, &coll)
	require.NoError(t, err)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_REQUIRED, cs["domainName"][0].Type)
	require.Contains(t, cs["domainName"][0].Reason, "finds existing records to adopt")

	_, err = validateBinding(resource{Object: "companies", IDField: "externalId", AdoptBy: "name"}, obj, nil, &coll)
	require.ErrorContains(t, err, `"name" must be an active unique Text, Links or Emails field of "companies"`)
	coll.Projections = slices.DeleteFunc(coll.Projections, func(p pf.Projection) bool { return p.Field == "domainName" })
	_, err = validateBinding(resource{Object: "companies", IDField: "externalId", AdoptBy: "domainName"}, obj, nil, &coll)
	require.ErrorContains(t, err, `the collection "acmeCo/companies" has no field that writes "domainName"`)
}

func TestAdoptGivenBackWhenTheTaskStops(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	h.fake.mu.Lock()
	h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 400, body: messagesBody("bad value")})
	h.fake.mu.Unlock()
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)), "can't write the document with key \"k1\"")
	row := h.fake.row("companies", hand["id"].(string))
	require.Equal(t, []any{"", "By hand"}, []any{row["externalId"], row["name"]})
}

func TestAdoptGiveBackStopsAtTheFirstFailure(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	h.fake.seed("companies", record{"name": "One", "domainName": map[string]any{"primaryLinkUrl": "one.com"}})
	h.fake.seed("companies", record{"name": "Two", "domainName": map[string]any{"primaryLinkUrl": "two.com"}})
	// Both claims succeed, every write fails, and so does the first give-back.
	posts := 0
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/rest/batch/companies") {
			h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 400, body: messagesBody("bad value")})
			if posts++; posts == 1 {
				h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPatch, path: "/rest/companies", status: 400, body: messagesBody("no")})
			}
		}
	}
	require.Error(t, h.store(h.doc(0, `{"id":"k1","name":"One","domain":"one.com"}`), h.doc(0, `{"id":"k2","name":"Two","domain":"two.com"}`)))
	h.fake.hook = nil
	require.Equal(t, 3, h.fake.count(http.MethodPatch, "/rest/companies"))
}

func TestAdoptFailedClaimGivenBack(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	// Twenty applies the claim but every try answers with an error.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPatch && h.fake.hook != nil {
			h.fake.rows["companies"][hand["id"].(string)]["externalId"] = "k1"
			h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodPatch, path: "/rest/companies", status: 400, body: messagesBody("no")})
			h.fake.hook = nil
		}
	}
	require.ErrorContains(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)), "adopting the record")
	require.Equal(t, "", h.fake.row("companies", hand["id"].(string))["externalId"])
}

func TestAdoptLostAnswerAndRecheckFails(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	// The claim applies, its answer is lost, and the recheck fails too.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPatch && h.fake.hook != nil {
			h.fake.rows["companies"][hand["id"].(string)]["externalId"] = "k1"
			h.fake.failures = append(h.fake.failures, fakeFailure{method: http.MethodGet, path: "/rest/companies", status: 400, body: messagesBody("no")})
			h.fake.hook = nil
		}
	}
	require.Error(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	require.Equal(t, "", h.fake.row("companies", hand["id"].(string))["externalId"])
}

func TestAdoptLostAnswer(t *testing.T) {
	h := newHarness(t, companies(`,"adopt_by":"domainName"`))
	hand := h.fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	// Twenty applies the update, but its answer is lost and a retry matches nothing.
	h.fake.hook = func(r *http.Request) {
		if r.Method == http.MethodPatch {
			h.fake.rows["companies"][hand["id"].(string)]["externalId"] = "k1"
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`)))
	h.fake.hook = nil
	require.Equal(t, hand["id"], h.only("companies", "k1")["id"])
	require.Equal(t, 1, h.stats("companies").adopted)
}

func TestAdoptSelfLinkWaits(t *testing.T) {
	fake, c := newFakeTwenty(t)
	fake.objects["companies"].fields["parentCompany"] = fakeField{typ: "RELATION", target: "companies"}
	coll := companiesCollection()
	coll.Projections = append(coll.Projections, proj("parent", "/parent", "string"))
	slices.SortFunc(coll.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	h := harnessFor(t, c, testBinding{
		resource:   `{"name":"acmeCo/companies","object":"companies","id_field":"externalId","adopt_by":"domainName","rejected_records":"skip","relations":[{"field":"parentCompany","id_field":"externalId","source_field":"parent"}]}`,
		collection: coll,
	})
	h.fake = fake
	hand := fake.seed("companies", record{"name": "By hand", "domainName": map[string]any{"primaryLinkUrl": "acme.com"}})
	// k1 adopts the record and is rejected; k2's link to k1 waits rather than point at it.
	fake.hook = func(r *http.Request) {
		if r.Method != http.MethodPost || !strings.Contains(r.URL.Path, "/rest/batch/companies") {
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if strings.Contains(string(body), `"k1"`) {
			fake.failures = append(fake.failures, fakeFailure{method: http.MethodPost, path: "/rest/batch/companies", status: 400, body: messagesBody("bad value")})
		}
	}
	require.NoError(t, h.store(h.doc(0, `{"id":"k1","name":"Acme","domain":"acme.com"}`), h.doc(0, `{"id":"k2","name":"Child","parent":"k1"}`)))
	require.Equal(t, "", fake.row("companies", hand["id"].(string))["externalId"])
	fake.hook = nil
	require.Nil(t, h.only("companies", "k2")["parentCompanyId"])
}
