package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/net/idna"
)

const fakeAPIKey = "test-key"

// fakeTwenty is an in-memory stand-in for Twenty's REST API. It models what probes of Twenty
// v2.52 showed (Oct 2026):
//   - A batch is all or nothing: one bad record rejects it with a 400 that names the field.
//   - An upsert matches a record on id or ANY unique field, soft-deleted ones included, and
//     restores a soft-deleted match. Two different matches give a 400.
//   - A plain create gives a 400 if a record's id or unique value is already on a record,
//     soft-deleted ones included. So does a unique value twice in one batch.
//   - Omitted fields are kept, and a null TEXT becomes "". A null composite field is ignored.
//   - Composite subfields merge, except that a links field is replaced whole, a phones field
//     loses its additional phones and an emails field its primary email unless the update sets
//     them.
//   - Date-times take any offset and are stored in UTC with milliseconds. A year before 100
//     moves to another century.
//   - Domains are stored as the host only, and emails lowercased. A malformed email, phone or
//     domain rejects the batch, and so does a country or calling code beside a number that
//     starts with "+" (the probe sent a conflicting one; the fake refuses any).
//   - A links field whose primary URL is empty makes its first secondary link the primary one.
//   - A NUL character in a value gives a 500 "Data validation error.".
//   - Lookups hide soft-deleted records unless the filter names deletedAt. Filters are
//     case-sensitive, and an empty TEXT counts as NULL.
//   - A filtered PATCH changes every matching record, soft-deleted ones included unless the
//     filter excludes them, and never creates one.
//   - A many-to-one relation is written through its join column, like companyId, which takes
//     the Twenty id of a record of the object it points at, soft-deleted ones included. Another
//     id gives a 400 that names a foreign key, and so does a value that isn't a UUID. A value
//     under the relation's own name gives a 400.
//   - The REST metadata leaves out which object a relation points at. The GraphQL metadata API
//     answers it for a field id, and answers errors with HTTP 200.
//   - Any write to a rich text field replaces its whole value: Twenty keeps the markdown and
//     makes tiptap from it, and blocknote is null. A null or text value gives a 400.
//   - A select or rating value must be one of the field's options, an array's items must be
//     strings (not null), a UUID field takes only UUIDs, and a null array or multi-select
//     becomes [].
//
// It is a test double, not a spec: anything the probes didn't check isn't modeled.
type fakeTwenty struct {
	mu       sync.Mutex
	objects  map[string]*fakeObject
	rows     map[string]map[string]record // object -> id -> record
	calls    []fakeCall
	failures []fakeFailure
	reject   func(object string, rec record) string // a message rejects the batch holding rec
	version  string                                 // what /client-config reports as appVersion, if anything
	noDelete bool                                   // deletes match nothing and answer no error, as in Twenty v2.30 to v2.34
	hook     func(r *http.Request)                  // runs before each answer, with mu held
	batches  []int                                  // records per batch write
	slept    []time.Duration                        // the client's backoffs
	nextID   int
	created  []string // fields created through the metadata API, as "object.name:TYPE"
}

type fakeObject struct {
	singular string
	fields   map[string]fakeField
}

type fakeField struct {
	typ      string
	unique   bool
	system   bool
	settings map[string]any
	target   string   // a relation's object
	morph    string   // a morph relation's id, shared by its fields
	many     bool     // a relation's list side, like a company's people
	options  []string // a select's values
}

// joinColumn is where a record holds the id of the record a many-to-one relation points at.
func (fd fakeField) joinColumn(name string) string {
	if (fd.typ == "RELATION" || fd.typ == "MORPH_RELATION" && fd.target != "") && !fd.many {
		return name + "Id"
	}
	return ""
}

// fakeFieldID is a field's metadata id.
func fakeFieldID(object, field string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("twenty-fake/"+object+"/"+field)).String()
}

var fakeRatings = []string{"RATING_1", "RATING_2", "RATING_3", "RATING_4", "RATING_5"}

type fakeCall struct {
	method, path string
	query        url.Values
}

type fakeFailure struct {
	method string // any method if ""
	path   string
	status int
	header http.Header
	body   any
}

func fakeObjects() map[string]*fakeObject {
	withOwn := func(fields map[string]fakeField) map[string]fakeField {
		fields["externalId"] = fakeField{typ: "TEXT", unique: true}
		fields["externalDeleted"] = fakeField{typ: "BOOLEAN"}
		fields["externalDeletedAt"] = fakeField{typ: "DATE_TIME"}
		fields["position"] = fakeField{typ: "POSITION", system: true}
		fields["createdBy"] = fakeField{typ: "ACTOR", system: true}
		return fields
	}
	return map[string]*fakeObject{
		"companies": {singular: "company", fields: withOwn(map[string]fakeField{
			"name":                 {typ: "TEXT"},
			"domainName":           {typ: "LINKS", unique: true, settings: map[string]any{"type": "domain", "maxNumberOfValues": 1}},
			"linkedinLink":         {typ: "LINKS"},
			"address":              {typ: "ADDRESS"},
			"annualRevenue":        {typ: "CURRENCY"},
			"employees":            {typ: "NUMBER"},
			"idealCustomerProfile": {typ: "BOOLEAN"},
			"tags":                 {typ: "ARRAY"},
			"lastOrderAt":          {typ: "DATE_TIME"},
			"accountCode":          {typ: "TEXT", unique: true},
			"score":                {typ: "NUMBER", unique: true},
			"externalAccountKey":   {typ: "TEXT"},
			"people":               {typ: "RELATION", target: "people", many: true},
			"accountOwner":         {typ: "RELATION", target: "workspaceMembers"},
		})},
		"workspaceMembers": {singular: "workspaceMember", fields: map[string]fakeField{
			"userEmail": {typ: "TEXT", unique: true, system: true},
			"name":      {typ: "FULL_NAME"},
			"userId":    {typ: "UUID", system: true},
		}},
		"people": {singular: "person", fields: withOwn(map[string]fakeField{
			"name":     {typ: "FULL_NAME"},
			"emails":   {typ: "EMAILS", unique: true},
			"jobTitle": {typ: "TEXT"},
			"phones":   {typ: "PHONES"},
			"company":  {typ: "RELATION", target: "companies"},
		})},
		"notes": {singular: "note", fields: withOwn(map[string]fakeField{
			"title":  {typ: "TEXT"},
			"bodyV2": {typ: "RICH_TEXT"},
		})},
		"tasks": {singular: "task", fields: withOwn(map[string]fakeField{
			"title": {typ: "TEXT"},
		})},
		"taskTargets": {singular: "taskTarget", fields: withOwn(map[string]fakeField{
			"task":          {typ: "RELATION", target: "tasks"},
			"target":        {typ: "MORPH_RELATION", morph: "m-task"},
			"targetCompany": {typ: "MORPH_RELATION", target: "companies", morph: "m-task"},
			"targetPerson":  {typ: "MORPH_RELATION", target: "people", morph: "m-task"},
		})},
		// A system object linking notes to records, through a morph relation with a field per object.
		"noteTargets": {singular: "noteTarget", fields: withOwn(map[string]fakeField{
			"note":          {typ: "RELATION", target: "notes"},
			"target":        {typ: "MORPH_RELATION", morph: "m-note"},
			"targetCompany": {typ: "MORPH_RELATION", target: "companies", morph: "m-note"},
			"targetPerson":  {typ: "MORPH_RELATION", target: "people", morph: "m-note"},
		})},
		// An object with a field of each type, as the contract tests create it in a live Twenty.
		"connectorTypeProbes": {singular: "connectorTypeProbe", fields: map[string]fakeField{
			"name":          {typ: "TEXT"},
			"externalId":    {typ: "TEXT", unique: true},
			"pText":         {typ: "TEXT"},
			"pNumber":       {typ: "NUMBER"},
			"pBoolean":      {typ: "BOOLEAN"},
			"pDate":         {typ: "DATE"},
			"pDateTime":     {typ: "DATE_TIME"},
			"pSelect":       {typ: "SELECT", options: []string{"OPT_A", "OPT_B"}},
			"pMultiSelect":  {typ: "MULTI_SELECT", options: []string{"OPT_A", "OPT_B"}},
			"pRating":       {typ: "RATING"},
			"pArray":        {typ: "ARRAY"},
			"pRawJson":      {typ: "RAW_JSON"},
			"pRichText":     {typ: "RICH_TEXT"},
			"pUuid":         {typ: "UUID"},
			"pEmails":       {typ: "EMAILS"},
			"pPhones":       {typ: "PHONES"},
			"pLinks":        {typ: "LINKS"},
			"pAddress":      {typ: "ADDRESS"},
			"pFullName":     {typ: "FULL_NAME"},
			"pCurrency":     {typ: "CURRENCY"},
			"pFiles":        {typ: "FILES", settings: map[string]any{"maxNumberOfValues": 2}},
			"pCompany":      {typ: "RELATION", target: "companies"},
			"pParent":       {typ: "RELATION", target: "connectorTypeProbes"},
			"probeChildren": {typ: "RELATION", target: "connectorTypeProbes", many: true},
			"pMorph":        {typ: "MORPH_RELATION"},
			"position":      {typ: "POSITION", system: true},
			"createdBy":     {typ: "ACTOR", system: true},
			"searchVector":  {typ: "TS_VECTOR", system: true},
		}},
	}
}

// newFakeTwenty starts a fake Twenty and returns it, with a client for it that doesn't wait.
func newFakeTwenty(t *testing.T) (*fakeTwenty, *client) {
	t.Helper()
	f := &fakeTwenty{objects: fakeObjects(), rows: make(map[string]map[string]record)}
	for name := range f.objects {
		f.rows[name] = make(map[string]record)
	}
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	c := newClient(server.URL, fakeAPIKey, 60000, 1000)
	c.sleep = func(_ context.Context, d time.Duration) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if d > 0 {
			f.slept = append(f.slept, d)
		}
		return nil
	}
	return f, c
}

// --- test helpers ------------------------------------------------------------------------

// failNext answers the next request whose path contains path with status and body.
func (f *fakeTwenty) failNext(path string, status int, header http.Header, body any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, fakeFailure{path: path, status: status, header: header, body: body})
}

// seed adds a record directly, as if someone created it in Twenty.
func (f *fakeTwenty) seed(obj string, fields record) record {
	f.mu.Lock()
	defer f.mu.Unlock()
	row := f.blank(obj)
	id, _ := fields["id"].(string)
	if id == "" {
		id = f.newID()
	}
	merge(row, clone(fields))
	row["id"] = id
	f.rows[obj][id] = row
	return clone(row)
}

func (f *fakeTwenty) softDelete(obj, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows[obj][id]["deletedAt"] = "2026-10-28T00:00:00.000Z"
}

// byKey returns the records whose externalId is key.
func (f *fakeTwenty) byKey(obj, key string) []record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.byKeyLocked(obj, key)
}

func (f *fakeTwenty) byKeyLocked(obj, key string) []record {
	var out []record
	for _, id := range slices.Sorted(maps.Keys(f.rows[obj])) {
		if f.rows[obj][id]["externalId"] == key {
			out = append(out, clone(f.rows[obj][id]))
		}
	}
	return out
}

// writes lists the batch writes to an object, each "create" or "upsert".
func (f *fakeTwenty) writes(obj string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if c.method == http.MethodPost && c.path == "/rest/batch/"+obj {
			if c.query.Has("upsert") {
				out = append(out, "upsert")
			} else {
				out = append(out, "create")
			}
		}
	}
	return out
}

func (f *fakeTwenty) rowCount(obj string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.rows[obj])
}

func (f *fakeTwenty) row(obj, id string) record {
	f.mu.Lock()
	defer f.mu.Unlock()
	return clone(f.rows[obj][id])
}

func (f *fakeTwenty) count(method, pathPrefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.method == method && strings.HasPrefix(c.path, pathPrefix) {
			n++
		}
	}
	return n
}

// --- transport ---------------------------------------------------------------------------

func (f *fakeTwenty) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	status, header, body := f.handle(r)
	f.mu.Unlock()
	for k, v := range header {
		w.Header()[k] = v
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func messagesBody(msg string) map[string]any { return map[string]any{"messages": []string{msg}} }

func (f *fakeTwenty) handle(r *http.Request) (int, http.Header, any) {
	path, query := r.URL.Path, r.URL.Query()
	f.calls = append(f.calls, fakeCall{r.Method, path, query})
	if f.hook != nil {
		f.hook(r)
	}
	for i, fail := range f.failures {
		if strings.Contains(path, fail.path) && (fail.method == "" || fail.method == r.Method) {
			f.failures = slices.Delete(f.failures, i, i+1)
			return fail.status, fail.header, fail.body
		}
	}
	// Twenty Cloud's Cloudflare refused Python's default agent with error 1010. The fake also
	// refuses Go's, which wasn't probed, to check that the connector sends its own.
	if ua := r.UserAgent(); ua == "" || strings.HasPrefix(ua, "Python-urllib") || strings.HasPrefix(ua, "Go-http-client") {
		return 403, nil, map[string]any{"title": "Error 1010: Access denied", "status": 403}
	}
	if path == "/client-config" {
		if f.version == "" {
			return 404, nil, messagesBody("Cannot GET /client-config")
		}
		return 200, nil, map[string]any{"appVersion": f.version}
	}
	if r.Header.Get("Authorization") != "Bearer "+fakeAPIKey {
		return 401, nil, messagesBody("Token invalid.")
	}
	var payload any
	d := json.NewDecoder(r.Body)
	d.UseNumber() // to check that numbers arrive exact
	if err := d.Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return 400, nil, messagesBody("Invalid JSON: " + err.Error())
	}

	parts := strings.Split(strings.Trim(path, "/"), "/")
	switch {
	case r.Method == http.MethodGet && path == "/rest/metadata/objects":
		return 200, nil, map[string]any{"data": f.metadata(), "pageInfo": map[string]any{"hasNextPage": false}}
	case r.Method == http.MethodPost && path == "/metadata":
		return f.graphql(payload)
	case r.Method == http.MethodPost && path == "/rest/metadata/fields":
		return f.createField(payload)
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "rest" && parts[1] == "batch" &&
		f.objects[parts[2]] != nil && (!query.Has("upsert") || query.Get("upsert") == "true"):
		list, ok := payload.([]any)
		if !ok {
			return 400, nil, messagesBody("fake: a batch must be an array")
		}
		records := make([]record, len(list))
		for i, item := range list {
			if records[i], ok = item.(map[string]any); !ok {
				return 400, nil, messagesBody("fake: a batch must hold objects")
			}
		}
		return f.write(parts[2], records, query.Has("upsert"))
	case len(parts) == 2 && parts[0] == "rest" && f.objects[parts[1]] != nil && r.Method == http.MethodGet:
		return f.find(parts[1], query)
	case len(parts) == 2 && parts[0] == "rest" && f.objects[parts[1]] != nil && r.Method == http.MethodPatch:
		values, ok := payload.(map[string]any)
		if !ok {
			return 400, nil, messagesBody("fake: an update must be an object")
		}
		return f.update(parts[1], query, values)
	case len(parts) == 2 && parts[0] == "rest" && f.objects[parts[1]] != nil && r.Method == http.MethodDelete:
		return f.remove(parts[1], query)
	case len(parts) == 3 && parts[0] == "rest" && f.objects[parts[1]] != nil && r.Method == http.MethodDelete &&
		query.Get("soft_delete") == "true":
		row, ok := f.rows[parts[1]][parts[2]]
		if !ok {
			return 404, nil, messagesBody("fake: no record " + parts[2])
		}
		row["deletedAt"] = "2026-10-28T00:00:00.000Z"
		return 200, nil, map[string]any{"data": map[string]any{"delete" + strings.ToUpper(parts[1][:1]) + parts[1][1:]: clone(row)}}
	}
	return 404, nil, messagesBody(fmt.Sprintf("Cannot %s %s", r.Method, path))
}

// --- behavior ----------------------------------------------------------------------------

func (f *fakeTwenty) metadata() []map[string]any {
	var out []map[string]any
	for _, plural := range slices.Sorted(maps.Keys(f.objects)) {
		o := f.objects[plural]
		// Every object has its id, a system field Twenty doesn't call unique.
		fields := []map[string]any{{
			"id": fakeFieldID(plural, "id"), "name": "id", "type": "UUID", "isUnique": false, "isActive": true,
			"isSystem": true, "isNullable": false, "writability": "SYSTEM",
		}}
		for _, name := range slices.Sorted(maps.Keys(o.fields)) {
			fd := o.fields[name]
			settings, writability := fd.settings, "OPEN"
			switch {
			case fd.typ == "RELATION" && fd.many:
				settings = map[string]any{"relationType": "ONE_TO_MANY"}
			case fd.typ == "RELATION":
				settings = map[string]any{"onDelete": "SET_NULL", "relationType": "MANY_TO_ONE", "joinColumnName": fd.joinColumn(name)}
			case fd.typ == "MORPH_RELATION" && fd.target != "":
				settings = map[string]any{"onDelete": "CASCADE", "relationType": "MANY_TO_ONE", "joinColumnName": fd.joinColumn(name)}
			case fd.typ == "MORPH_RELATION":
				settings = map[string]any{"relationType": "MANY_TO_ONE"}
			case fd.typ == "TS_VECTOR":
				writability = "SYSTEM"
			}
			fields = append(fields, map[string]any{
				"id": fakeFieldID(plural, name), "name": name, "type": fd.typ, "isUnique": fd.unique, "isActive": true,
				"isSystem": fd.system, "isNullable": true, "settings": settings, "writability": writability,
				"morphId": fd.morph,
			})
		}
		out = append(out, map[string]any{
			"id": "obj-" + plural, "namePlural": plural, "nameSingular": o.singular, "isActive": true,
			"isSystem": false, "fields": fields,
		})
	}
	return out
}

// createField adds a field as Twenty's metadata API does, with its errors for a bad or taken name.
func (f *fakeTwenty) createField(payload any) (int, http.Header, any) {
	body, _ := payload.(map[string]any)
	plural, _ := strings.CutPrefix(fmt.Sprint(body["objectMetadataId"]), "obj-")
	name, _ := body["name"].(string)
	typ, _ := body["type"].(string)
	fail := func(msg string) (int, http.Header, any) {
		return 400, nil, map[string]any{"statusCode": 400, "error": "METADATA_VALIDATION_ERROR",
			"message": "Multiple validation errors occurred while creating fields",
			"errors":  map[string]any{"fieldMetadata": []any{map[string]any{"status": "fail", "errors": []any{map[string]any{"message": msg}}}}}}
	}
	o := f.objects[plural]
	if o == nil {
		return 400, nil, messagesBody("fake: no object " + plural)
	}
	switch _, taken := o.fields[name]; {
	case body["label"] == nil || body["label"] == "":
		return 400, nil, map[string]any{"message": []string{"label should not be empty"}, "statusCode": 400}
	case len(name) > 63:
		return fail("Name is too long")
	case !fieldName.MatchString(name):
		return fail("Name is not valid: it must start with lowercase letter and contain only alphanumeric letters")
	case taken || name == "id":
		return fail(fmt.Sprintf("Name %q is not available as it is already used by another field", name))
	}
	settings, _ := body["settings"].(map[string]any)
	unique, _ := body["isUnique"].(bool)
	o.fields[name] = fakeField{typ: typ, settings: settings, unique: unique}
	f.created = append(f.created, plural+"."+name+":"+typ)
	return 201, nil, map[string]any{"id": fakeFieldID(plural, name), "name": name, "type": typ, "label": body["label"]}
}

// graphql answers the one GraphQL metadata query the connector sends: which object a relation
// field points at. Like Twenty, it answers errors with HTTP 200.
func (f *fakeTwenty) graphql(payload any) (int, http.Header, any) {
	req, _ := payload.(map[string]any)
	query, _ := req["query"].(string)
	vars, _ := req["variables"].(map[string]any)
	id, _ := vars["id"].(string)
	if !strings.Contains(query, "field(id: $id)") {
		return 400, nil, messagesBody("fake: only the query of one field is modeled")
	}
	for _, plural := range slices.Sorted(maps.Keys(f.objects)) {
		for name, fd := range f.objects[plural].fields {
			if fakeFieldID(plural, name) != id {
				continue
			}
			if strings.Contains(query, "morphRelations") {
				// Each field of a morph relation lists every object the relation points at.
				var morph []any
				for _, other := range slices.Sorted(maps.Keys(f.objects[plural].fields)) {
					od := f.objects[plural].fields[other]
					if od.morph != "" && od.morph == fd.morph && od.target != "" {
						morph = append(morph, map[string]any{"targetObjectMetadata": map[string]any{
							"namePlural": od.target, "nameSingular": f.objects[od.target].singular}})
					}
				}
				return 200, nil, map[string]any{"data": map[string]any{"field": map[string]any{"morphRelations": morph}}}
			}
			var rel any
			if fd.typ == "RELATION" {
				rel = map[string]any{"targetObjectMetadata": map[string]any{"namePlural": fd.target}}
			}
			return 200, nil, map[string]any{"data": map[string]any{"field": map[string]any{"relation": rel}}}
		}
	}
	return 200, nil, map[string]any{"data": nil, "errors": []any{map[string]any{
		"message": "Unable to find FieldMetadataEntity with id: " + id, "extensions": map[string]any{"code": "NOT_FOUND"}}}}
}

// joinColumns returns an object's join columns, by the relation fields they belong to.
func (f *fakeTwenty) joinColumns(obj string) map[string]string {
	out := make(map[string]string)
	for name, fd := range f.objects[obj].fields {
		if col := fd.joinColumn(name); col != "" {
			out[col] = name
		}
	}
	return out
}

// filter parses a request's filter, or returns a 400 like Twenty's.
func (f *fakeTwenty) filter(obj string, q url.Values) (fakeNode, int, any) {
	if !q.Has("filter") {
		return fakeNode{op: "and"}, 0, nil
	}
	text := q.Get("filter")
	n, err := parseFilter(text)
	if err != nil {
		return n, 400, messagesBody(fmt.Sprintf("Invalid filter %q: %v", text, err))
	}
	o := f.objects[obj]
	known := map[string]bool{"id": true, "deletedAt": true, "createdAt": true, "updatedAt": true}
	for name := range o.fields {
		known[name] = true
	}
	for col := range f.joinColumns(obj) {
		known[col] = true
	}
	for _, name := range n.fieldsNamed() {
		if !known[name] {
			return n, 400, messagesBody(fmt.Sprintf(`Object %s doesn't have any "%s" field.`, o.singular, name))
		}
	}
	return n, 0, nil
}

func (f *fakeTwenty) find(obj string, q url.Values) (int, http.Header, any) {
	n, status, body := f.filter(obj, q)
	if status != 0 {
		return status, nil, body
	}
	withDeleted := strings.Contains(q.Get("filter"), "deletedAt")
	var rows []record
	for _, id := range slices.Sorted(maps.Keys(f.rows[obj])) {
		r := f.rows[obj][id]
		if (withDeleted || r["deletedAt"] == nil) && n.matches(r) {
			rows = append(rows, r)
		}
	}
	if after := q.Get("starting_after"); after != "" {
		rows = slices.DeleteFunc(rows, func(r record) bool { return r["id"].(string) <= after })
	}
	limit := 60
	if l, err := strconv.Atoi(q.Get("limit")); err == nil {
		limit = l
	}
	page := rows[:min(limit, len(rows))]
	info := map[string]any{"hasNextPage": len(rows) > limit, "startCursor": nil, "endCursor": nil}
	if len(page) > 0 {
		info["startCursor"], info["endCursor"] = page[0]["id"], page[len(page)-1]["id"]
	}
	return 200, nil, map[string]any{"data": map[string]any{obj: clone(page)}, "totalCount": len(rows), "pageInfo": info}
}

func (f *fakeTwenty) write(obj string, records []record, upsert bool) (int, http.Header, any) {
	f.batches = append(f.batches, len(records))
	o := f.objects[obj]
	for _, rec := range records {
		if b, _ := json.Marshal(rec); bytes.Contains(b, []byte(`\u0000`)) {
			return 500, nil, messagesBody("Data validation error.")
		}
		msg := f.fieldError(obj, rec)
		if msg == "" && f.reject != nil {
			msg = f.reject(obj, rec)
		}
		if msg == "" {
			msg = f.invalid(obj, rec)
		}
		if msg != "" {
			return 400, nil, messagesBody(msg)
		}
	}

	recs := clone(records)
	for _, rec := range recs {
		f.normalize(obj, rec)
	}
	for _, path := range append([]string{"id"}, f.uniquePaths(obj)...) {
		seen := make(map[any]bool)
		for _, r := range recs {
			if v := fakeGet(r, path); v != nil && v != "" {
				if seen[v] {
					return 400, nil, messagesBody("A duplicate entry was detected: unique constraint on " + path + " violated")
				}
				seen[v] = true
			}
		}
	}

	rows := clone(f.rows[obj])
	var written []record
	for _, rec := range recs {
		var matches []string
		for _, id := range slices.Sorted(maps.Keys(rows)) {
			r := rows[id]
			hit := rec["id"] != nil && rec["id"] != "" && r["id"] == rec["id"]
			for _, p := range f.uniquePaths(obj) {
				if v := fakeGet(rec, p); v != nil && v != "" && fakeEq(fakeGet(r, p), v) {
					hit = true
				}
			}
			if hit {
				matches = append(matches, id)
			}
		}
		switch {
		case len(matches) > 0 && !upsert:
			return 400, nil, messagesBody(fmt.Sprintf("A duplicate entry was detected: unique constraint %s.IDX_UNIQUE_0123456789abcdef was violated", o.singular))
		case len(matches) > 1:
			return 400, nil, messagesBody("Multiple records found with the same unique field values. Cannot determine which record to update.")
		}
		var row record
		if len(matches) == 1 {
			row = rows[matches[0]]
			row["deletedAt"] = nil // an upsert restores a soft-deleted match
		} else {
			row = f.blank(obj)
			id, _ := rec["id"].(string)
			if id == "" {
				id = f.newID()
			}
			row["id"] = id
			rows[id] = row
		}
		merge(row, rec)
		written = append(written, row)
	}
	if msg := f.integrityError(obj, rows, written); msg != "" {
		return 400, nil, messagesBody(msg)
	}
	f.rows[obj] = rows
	return 201, nil, map[string]any{"data": map[string]any{"create" + strings.ToUpper(obj[:1]) + obj[1:]: clone(written)}}
}

func (f *fakeTwenty) update(obj string, q url.Values, values record) (int, http.Header, any) {
	if !q.Has("filter") {
		return 400, nil, messagesBody("fake: an update without a filter isn't modeled")
	}
	n, status, body := f.filter(obj, q)
	if status != 0 {
		return status, nil, body
	}
	msg := f.fieldError(obj, values)
	if msg == "" {
		msg = f.invalid(obj, values)
	}
	if msg != "" {
		return 400, nil, messagesBody(msg)
	}
	values = clone(values)
	f.normalize(obj, values)
	rows := clone(f.rows[obj])
	var changed []record
	for _, id := range slices.Sorted(maps.Keys(rows)) {
		if r := rows[id]; n.matches(r) { // soft-deleted records too
			merge(r, values)
			changed = append(changed, r)
		}
	}
	if msg := f.integrityError(obj, rows, changed); msg != "" {
		return 400, nil, messagesBody(msg)
	}
	f.rows[obj] = rows
	return 200, nil, map[string]any{"data": map[string]any{"update" + strings.ToUpper(obj[:1]) + obj[1:]: clone(changed)}}
}

// remove deletes the records a filter matches, as Twenty's DELETE /rest/<object> does: for good
// unless soft_delete=true, which trashes them. A soft delete doesn't match trashed records; a
// permanent one does, unless the filter says deletedAt. Deleting a record for good clears the
// relations that point at it, as Twenty did for a person's company in a probe.
func (f *fakeTwenty) remove(obj string, q url.Values) (int, http.Header, any) {
	if !q.Has("filter") {
		return 400, nil, messagesBody("fake: a delete without a filter isn't modeled")
	}
	n, status, body := f.filter(obj, q)
	if status != 0 {
		return status, nil, body
	}
	soft := q.Get("soft_delete") == "true"
	var deleted []record
	if f.noDelete {
		return 200, nil, map[string]any{"data": map[string]any{"delete" + strings.ToUpper(obj[:1]) + obj[1:]: deleted}}
	}
	for _, id := range slices.Sorted(maps.Keys(f.rows[obj])) {
		r := f.rows[obj][id]
		if !n.matches(r) || (soft && r["deletedAt"] != nil) {
			continue
		}
		deleted = append(deleted, record{"id": id})
		if soft {
			r["deletedAt"] = "2026-10-28T00:00:00.000Z"
			continue
		}
		delete(f.rows[obj], id)
		for other, o := range f.objects {
			for name, fd := range o.fields {
				col := fd.joinColumn(name)
				if col == "" || fd.target != obj {
					continue
				}
				for rid, row := range f.rows[other] {
					if row[col] != id {
						continue
					}
					if fd.typ == "MORPH_RELATION" {
						// A note or task target goes with its record (onDelete CASCADE in Twenty v2.52).
						delete(f.rows[other], rid)
					} else {
						row[col] = nil
					}
				}
			}
		}
	}
	return 200, nil, map[string]any{"data": map[string]any{"delete" + strings.ToUpper(obj[:1]) + obj[1:]: deleted}}
}

var fakeUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (f *fakeTwenty) fieldError(obj string, rec record) string {
	o := f.objects[obj]
	joins := f.joinColumns(obj)
	for _, k := range slices.Sorted(maps.Keys(rec)) {
		v := rec[k]
		if k == "id" {
			continue
		}
		if _, ok := joins[k]; ok {
			if s, _ := v.(string); v != nil && !fakeUUID.MatchString(s) {
				return fmt.Sprintf(`Invalid UUID value '%v' for field "%s"`, v, k)
			}
			continue
		}
		fd, ok := o.fields[k]
		if !ok {
			return fmt.Sprintf(`Object %s doesn't have any "%s" field.`, o.singular, k)
		}
		switch {
		case fd.typ == "RELATION" && fd.many:
			return "fake: writing the list side of a relation isn't modeled"
		case fd.typ == "RELATION":
			return fmt.Sprintf(`Relation "%s" requires create, connect, or disconnect operation`, k)
		case fd.typ == "DATE_TIME" && v != nil:
			if _, err := time.Parse(time.RFC3339Nano, fmt.Sprint(v)); err != nil {
				return fmt.Sprintf(`Invalid value '%v' for field "%s"`, v, k)
			}
		}
	}
	return ""
}

// integrityError checks the foreign keys of the records written, and unique values across the
// whole object.
func (f *fakeTwenty) integrityError(obj string, rows map[string]record, written []record) string {
	o := f.objects[obj]
	for _, row := range written {
		for _, name := range slices.Sorted(maps.Keys(o.fields)) {
			fd := o.fields[name]
			col := fd.joinColumn(name)
			parent, _ := row[col].(string)
			if col == "" || parent == "" {
				continue
			}
			// A soft-deleted record is still there to point at.
			pool := f.rows[fd.target]
			if fd.target == obj {
				pool = rows
			}
			if _, ok := pool[parent]; !ok {
				return fmt.Sprintf(`insert or update on table "%s" violates foreign key constraint "FK_0123456789abcdef012345678"`, o.singular)
			}
		}
	}
	for _, path := range f.uniquePaths(obj) {
		seen := make(map[any]bool)
		for _, id := range slices.Sorted(maps.Keys(rows)) {
			if v := fakeGet(rows[id], path); v != nil && v != "" {
				if seen[v] {
					return "A duplicate entry was detected: unique constraint on " + path + " violated"
				}
				seen[v] = true
			}
		}
	}
	// Twenty keeps one live link of a note or task to each record, while a link with no target can
	// repeat (v2.52).
	if parent := map[string]string{"noteTargets": "noteId", "taskTargets": "taskId"}[obj]; parent != "" {
		seen := make(map[string]bool)
		for _, id := range slices.Sorted(maps.Keys(rows)) {
			r := rows[id]
			for _, col := range []string{"targetCompanyId", "targetPersonId"} {
				if r["deletedAt"] != nil || r[parent] == nil || r[col] == nil {
					continue
				}
				k := fmt.Sprint(r[parent], "/", col, "/", r[col])
				if seen[k] {
					return fmt.Sprintf("A duplicate entry was detected: unique constraint %s.IDX_UNIQUE_d4caca2b253642e0f16cd7c1790 was violated", o.singular)
				}
				seen[k] = true
			}
		}
	}
	return ""
}

var (
	fakeEmailOK     = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
	fakePhoneOK     = regexp.MustCompile(`^[+\d\s()./-]{3,}(\s*ext\s*\d+)?$`)
	fakeOtherScheme = regexp.MustCompile(`(?i)^[a-z][a-z0-9+.-]*://`)
	fakeHTTP        = regexp.MustCompile(`(?i)^https?://`)
)

// fakeList formats a list as Twenty's messages do, like [ 'OPT_X', 1 ].
func fakeList(list []any) string {
	items := make([]string, len(list))
	for i, x := range list {
		switch x := x.(type) {
		case string:
			items[i] = "'" + x + "'"
		case nil:
			items[i] = "null"
		default:
			items[i] = fmt.Sprint(x)
		}
	}
	return "[ " + strings.Join(items, ", ") + " ]"
}

func (f *fakeTwenty) invalid(obj string, rec record) string {
	o := f.objects[obj]
	for _, k := range slices.Sorted(maps.Keys(rec)) {
		fd := o.fields[k]
		if msg := f.invalidValue(k, fd, rec[k]); msg != "" {
			return msg
		}
		v, ok := rec[k].(map[string]any)
		if !ok {
			continue
		}
		if email, _ := v["primaryEmail"].(string); fd.typ == "EMAILS" && email != "" && !fakeEmailOK.MatchString(email) {
			return fmt.Sprintf(`Invalid string value '%s' for email field "%s.primaryEmail"`, email, k)
		}
		if u, _ := v["primaryLinkUrl"].(string); fd.typ == "LINKS" && fd.settings["type"] == "domain" && u != "" {
			if _, ok := twentyDomain(u); !ok {
				if fakeOtherScheme.MatchString(u) && !fakeHTTP.MatchString(u) {
					return "The URL of the link is not valid"
				}
				return fmt.Sprintf(`"%s" is not a domain name, for domain-typed links field "%s.primaryLinkUrl"`, u, k)
			}
		}
		if phone, _ := v["primaryPhoneNumber"].(string); fd.typ == "PHONES" && phone != "" {
			// A number needs its country: a country or calling code beside it, or a leading "+".
			country, _ := v["primaryPhoneCountryCode"].(string)
			calling, _ := v["primaryPhoneCallingCode"].(string)
			if !fakePhoneOK.MatchString(phone) || (country == "" && calling == "" && !strings.HasPrefix(phone, "+")) {
				return "Provided phone number is invalid " + phone
			}
			if strings.HasPrefix(phone, "+") && (country != "" || calling != "") {
				return "Provided and inferred country code are conflicting"
			}
		}
	}
	return ""
}

// invalidValue checks a select, rating, array or rich text value, as Twenty v2.52 did.
func (f *fakeTwenty) invalidValue(name string, fd fakeField, v any) string {
	switch fd.typ {
	case "SELECT", "RATING":
		options := fd.options
		if fd.typ == "RATING" {
			options = fakeRatings
		}
		switch v := v.(type) {
		case nil:
		case string:
			if !slices.Contains(options, v) {
				return fmt.Sprintf(`Invalid value "%s" for field "%s". Valid values are: %s`, v, name, strings.Join(options, ", "))
			}
		default:
			return fmt.Sprintf(`Invalid string value %v for text field "%s"`, v, name)
		}
	case "MULTI_SELECT":
		list, ok := v.([]any)
		if s, isString := v.(string); isString {
			list, ok = []any{s}, true
		}
		if ok && slices.ContainsFunc(list, func(x any) bool { _, s := x.(string); return !s }) {
			return fmt.Sprintf(`Invalid value %s for field "%s - Array values need to be string"`, fakeList(list), name)
		}
		if v != nil && (!ok || slices.ContainsFunc(list, func(x any) bool { s, _ := x.(string); return !slices.Contains(fd.options, s) })) {
			return fmt.Sprintf(`Invalid value %s for multi select field "%s"`, fakeList(list), name)
		}
	case "UUID":
		if s, _ := v.(string); v != nil && !fakeUUID.MatchString(s) {
			return fmt.Sprintf(`Invalid UUID value '%v' for field "%s"`, v, name)
		}
	case "ARRAY":
		if list, ok := v.([]any); ok && slices.ContainsFunc(list, func(x any) bool { _, s := x.(string); return !s }) {
			return fmt.Sprintf(`Invalid value %s for field "%s - Array values need to be string"`, fakeList(list), name)
		}
	case "RICH_TEXT":
		switch v := v.(type) {
		case nil:
			return "Cannot destructure property 'tiptap' of '(intermediate value)(intermediate value)(intermediate value)' as it is null."
		case map[string]any:
			for _, sub := range slices.Sorted(maps.Keys(v)) {
				switch x := v[sub]; sub {
				case "markdown":
					if _, ok := x.(string); x != nil && !ok {
						return fmt.Sprintf(`Invalid string value %v for text field "%s.markdown"`, x, name)
					}
				case "blocknote", "tiptap":
					if x != nil {
						return "fake: rich text from the editor's formats isn't modeled"
					}
				default:
					return fmt.Sprintf(`Invalid subfield '%s' for rich text field "%s"`, sub, name)
				}
			}
		default:
			return fmt.Sprintf(`Invalid object value '%v' for field "%s"`, v, name)
		}
	}
	return ""
}

// fakeTiptap stands in for the tiptap document Twenty makes from markdown. Twenty's parses the
// markdown; this one holds it as one paragraph.
func fakeTiptap(markdown string) string {
	b, _ := json.Marshal(map[string]any{"type": "doc", "attrs": map[string]any{"schemaVersion": 1},
		"content": []any{map[string]any{"type": "paragraph", "content": []any{map[string]any{"type": "text", "text": markdown}}}}})
	return string(b)
}

func (f *fakeTwenty) uniquePaths(obj string) []string {
	var paths []string
	o := f.objects[obj]
	for _, name := range slices.Sorted(maps.Keys(o.fields)) {
		fd := o.fields[name]
		if !fd.unique {
			continue
		}
		if sub, ok := primarySubfield[fd.typ]; ok {
			paths = append(paths, name+"."+sub)
		} else {
			paths = append(paths, name)
		}
	}
	return paths
}

// A null subfield stays null if it is in fakeNullable, becomes [] if it is in fakeLists, and
// becomes "" otherwise.
var (
	fakeNullable = []string{"addressLat", "addressLng", "amountMicros", "currencyCode"}
	fakeLists    = []string{"secondaryLinks", "additionalEmails", "additionalPhones"}
)

// normalize turns the fields of a write into what Twenty merges into the record.
func (f *fakeTwenty) normalize(obj string, rec record) {
	o := f.objects[obj]
	for k, v := range rec {
		fd := o.fields[k]
		_, composite := compositeSubfields[fd.typ]
		switch v := v.(type) {
		case nil:
			switch {
			case composite:
				delete(rec, k)
			case fd.typ == "TEXT":
				rec[k] = ""
			case fd.typ == "MULTI_SELECT" || fd.typ == "ARRAY":
				rec[k] = []any{}
			}
		case string:
			switch fd.typ {
			case "DATE_TIME":
				rec[k] = fakeTime(v)
			case "MULTI_SELECT", "ARRAY":
				rec[k] = []any{v}
			}
		case map[string]any:
			if fd.typ == "RICH_TEXT" {
				md, _ := v["markdown"].(string)
				var tiptap any
				if md != "" {
					tiptap = fakeTiptap(md)
				}
				rec[k] = map[string]any{"blocknote": nil, "markdown": md, "tiptap": tiptap}
				continue
			}
			var replaced []string
			switch fd.typ {
			case "LINKS":
				replaced = []string{"primaryLinkUrl", "primaryLinkLabel", "secondaryLinks"}
			case "PHONES":
				replaced = []string{"additionalPhones"}
			case "EMAILS":
				replaced = []string{"primaryEmail"}
			}
			for _, sub := range replaced {
				if _, ok := v[sub]; !ok {
					v[sub] = nil
				}
			}
			for sub, val := range v {
				switch {
				case val != nil || slices.Contains(fakeNullable, sub):
				case slices.Contains(fakeLists, sub):
					v[sub] = []any{}
				default:
					v[sub] = ""
				}
			}
			if v["currencyCode"] == "" {
				v["currencyCode"] = nil
			}
			if u, _ := v["primaryLinkUrl"].(string); fd.typ == "LINKS" && fd.settings["type"] == "domain" && u != "" {
				v["primaryLinkUrl"], _ = twentyDomain(u)
			}
			if e, _ := v["primaryEmail"].(string); fd.typ == "EMAILS" && e != "" {
				v["primaryEmail"] = strings.ToLower(e)
			}
			if more, _ := v["secondaryLinks"].([]any); fd.typ == "LINKS" && v["primaryLinkUrl"] == "" && len(more) > 0 {
				first, _ := more[0].(map[string]any)
				v["primaryLinkUrl"], v["primaryLinkLabel"], v["secondaryLinks"] = first["url"], first["label"], more[1:]
			}
		}
	}
}

// fakeTime returns a date-time as Twenty stores it.
func fakeTime(s string) string {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s // fieldError refuses it
	}
	t = t.UTC()
	switch y := t.Year(); {
	case y < 50:
		t = t.AddDate(2000, 0, 0)
	case y < 100:
		t = t.AddDate(1900, 0, 0)
	}
	return t.Format("2006-01-02T15:04:05.000Z")
}

func (f *fakeTwenty) blank(obj string) record {
	row := record{"deletedAt": nil}
	for name, fd := range f.objects[obj].fields {
		switch subs, composite := compositeSubfields[fd.typ]; {
		case fd.typ == "RELATION":
			if col := fd.joinColumn(name); col != "" {
				row[col] = nil
			}
		case fd.typ == "RICH_TEXT":
			row[name] = map[string]any{"blocknote": nil, "markdown": "", "tiptap": nil}
		case fd.typ == "MULTI_SELECT" || fd.typ == "ARRAY":
			row[name] = []any{}
		case composite:
			sub := make(map[string]any)
			for s := range subs {
				sub[s] = nil
			}
			row[name] = sub
		case fd.typ == "TEXT":
			row[name] = ""
		case fd.typ == "BOOLEAN":
			row[name] = false
		default:
			row[name] = nil
		}
	}
	return row
}

func (f *fakeTwenty) newID() string {
	f.nextID++
	return fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextID)
}

// merge writes rec's fields into row. A composite field's subfields merge into the row's.
func merge(row, rec record) {
	for k, v := range rec {
		if k == "id" {
			continue
		}
		if nv, ok := v.(map[string]any); ok {
			if ov, ok := row[k].(map[string]any); ok {
				maps.Copy(ov, nv)
				continue
			}
		}
		row[k] = v
	}
}

func clone[T any](v T) T {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	var out T
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&out); err != nil {
		panic(err)
	}
	return out
}

func fakeGet(rec record, path string) any {
	var cur any = rec
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

func fakeEq(a, b any) bool {
	switch a.(type) {
	case nil:
		return b == nil
	case string, bool, json.Number:
		return a == b
	}
	return false
}

var (
	fakeHost   = regexp.MustCompile(`(?i)^(?:(https?)://)?(?:[^@/?#]*@)?([^/?#:]+)(?::(\d*))?(?:[/?#].*)?$`)
	fakeLabel  = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$`)
	fakeDigits = regexp.MustCompile(`^\d+$`)
)

// twentyDomain is what Twenty v2.52 stored for a domain-typed link in the probe, or false where
// it answered 400. It is written apart from normalizeDomain, so the tests compare two versions.
func twentyDomain(value string) (string, bool) {
	s := strings.TrimSpace(value)
	m := fakeHost.FindStringSubmatch(s)
	if m == nil || (fakeOtherScheme.MatchString(s) && m[1] == "") {
		return "", false
	}
	if m[3] != "" {
		if port, err := strconv.Atoi(m[3]); err != nil || port > 65535 {
			return "", false
		}
	}
	host, err := idna.New(idna.VerifyDNSLength(true)).ToASCII(strings.ToLower(m[2]))
	if err != nil {
		return "", false
	}
	// One dot at least, letters, digits and inner hyphens only, a TLD that isn't all digits, and
	// labels of at most 62 characters (61 for the TLD): the limits the probe found.
	labels := strings.Split(host, ".")
	tld := labels[len(labels)-1]
	if len(labels) < 2 || fakeDigits.MatchString(tld) || len(tld) > 61 {
		return "", false
	}
	for i, l := range labels {
		if !fakeLabel.MatchString(l) || (i < len(labels)-1 && len(l) > 62) {
			return "", false
		}
	}
	for strings.HasPrefix(host, "www.") {
		host = host[4:]
	}
	return host, true
}

// --- filters -----------------------------------------------------------------------------

// fakeNode is a parsed Twenty REST filter: and(...), or(...), not(...), or field[op]:value, where
// a value is a "quoted" string, a bare word (NULL, NOT_NULL, true, false, a uuid) or a [list] of
// them. Like Twenty, it reads a quoted string as is: ü stays six characters.
type fakeNode struct {
	op         string // "and", "or", "not" or "cmp"
	items      []fakeNode
	field, cmp string
	value      any // a string, a bool, or a list of them
}

var (
	fakeTerm = regexp.MustCompile(`^([\w.]+)\[(\w+)\]:`)
	fakeWord = regexp.MustCompile(`^[^,()\[\]]+`)
)

func parseFilter(s string) (fakeNode, error) {
	n, end, err := parseNode(s, 0)
	if err == nil && end != len(s) {
		err = fmt.Errorf("unexpected text at %d: %q", end, s[end:])
	}
	return n, err
}

func parseNode(s string, i int) (fakeNode, int, error) {
	for _, op := range []string{"and", "or", "not"} {
		if !strings.HasPrefix(s[i:], op+"(") {
			continue
		}
		i += len(op) + 1
		n := fakeNode{op: op}
		for {
			item, j, err := parseNode(s, i)
			if err != nil {
				return n, 0, err
			}
			n.items = append(n.items, item)
			switch i = j; {
			case strings.HasPrefix(s[i:], ","):
				i++
			case strings.HasPrefix(s[i:], ")"):
				return n, i + 1, nil
			default:
				return n, 0, fmt.Errorf("expected , or ) at %d", i)
			}
		}
	}
	m := fakeTerm.FindStringSubmatch(s[i:])
	if m == nil {
		return fakeNode{}, 0, fmt.Errorf("expected field[op]:value at %d", i)
	}
	n := fakeNode{op: "cmp", field: m[1], cmp: m[2]}
	i += len(m[0])
	if !strings.HasPrefix(s[i:], "[") {
		v, j, err := parseScalar(s, i)
		n.value = v
		return n, j, err
	}
	var list []any
	for i++; !strings.HasPrefix(s[i:], "]"); {
		if i >= len(s) {
			return n, 0, errors.New("a list has no closing ]")
		}
		v, j, err := parseScalar(s, i)
		if err != nil {
			return n, 0, err
		}
		list = append(list, v)
		if i = j; strings.HasPrefix(s[i:], ",") {
			i++
		}
	}
	n.value = list
	return n, i + 1, nil
}

func parseScalar(s string, i int) (any, int, error) {
	if strings.HasPrefix(s[i:], `"`) {
		end := strings.Index(s[i+1:], `"`)
		if end < 0 {
			return nil, 0, fmt.Errorf("no closing quote after %d", i)
		}
		return s[i+1 : i+1+end], i + 2 + end, nil
	}
	w := fakeWord.FindString(s[i:])
	switch w {
	case "":
		return nil, 0, fmt.Errorf("expected a value at %d", i)
	case "true":
		return true, i + len(w), nil
	case "false":
		return false, i + len(w), nil
	}
	return w, i + len(w), nil
}

func (n fakeNode) matches(row record) bool {
	switch n.op {
	case "and":
		return !slices.ContainsFunc(n.items, func(c fakeNode) bool { return !c.matches(row) })
	case "or":
		return slices.ContainsFunc(n.items, func(c fakeNode) bool { return c.matches(row) })
	case "not":
		return !n.items[0].matches(row)
	}
	v := fakeGet(row, n.field)
	if v == "" {
		v = nil
	}
	value := n.value
	if value == "" {
		value = nil
	}
	switch n.cmp {
	case "in":
		list, _ := value.([]any)
		return v != nil && slices.ContainsFunc(list, func(x any) bool { return fakeEq(v, x) })
	case "is":
		return (v == nil) == (value == "NULL")
	case "eq":
		return fakeEq(v, value)
	case "neq":
		return v != nil && !fakeEq(v, value)
	}
	panic("fake: the operator " + n.cmp + " isn't modeled")
}

func (n fakeNode) fieldsNamed() []string {
	if n.op != "cmp" {
		var out []string
		for _, c := range n.items {
			out = append(out, c.fieldsNamed()...)
		}
		return out
	}
	return []string{strings.Split(n.field, ".")[0]}
}
