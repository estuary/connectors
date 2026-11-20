package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// Behavior noted below was probed on Twenty v2.52.0, against a local Twenty and Twenty Cloud
// (Oct 2026).
const (
	// maxBatch is Twenty's documented cap on records per batch request. Its source allows 200.
	maxBatch = 60
	// pageSize is the most records Twenty returns per page.
	pageSize = 200
	// A filter's values go in the URL, which proxies like nginx limit to 8 KB by default. So
	// one request's escaped values take at most maxFilterBytes, and a value longer than
	// maxValueBytes escaped can't go in a filter.
	maxFilterBytes   = 6000
	maxValueBytes    = 3000
	maxResponseBytes = 32 << 20
	maxAttempts      = 8
	requestTimeout   = 60 * time.Second
	rateWindow       = time.Minute // Twenty's rate limit counts requests per minute
	// anyDeleted makes a filter match soft-deleted records too. Twenty hides them unless the
	// filter names deletedAt.
	anyDeleted = "or(deletedAt[is]:NULL,deletedAt[is]:NOT_NULL)"
	// userAgent names the connector. Twenty Cloud sits behind Cloudflare, which refuses some
	// default agents (Python's, with error 1010), so the connector doesn't rely on Go's.
	userAgent = "Estuary materialize-twenty"
)

var (
	retryStatuses = map[int]bool{408: true, 425: true, 429: true, 500: true, 502: true, 503: true, 504: true}
	// rejectStatuses are the answers that blame the records sent, so the connector splits the
	// batch to find them. Others, like 402 or 409 for a workspace at its plan's limits, stop the
	// task instead of skipping every record.
	rejectStatuses = map[int]bool{400: true, 413: true, 422: true}
	notFilterable  = regexp.MustCompile(`["\\\x00-\x1f]`)
)

// filterable reports whether a value can go in a Twenty filter. Twenty reads a filter value as
// the text between its quotes and decodes no escapes. So a quote, a backslash or a control
// character can't be matched, and a quote would end the value and could add filter terms. A
// longer value than maxValueBytes escaped would make the URL too long. Other characters are fine:
// Twenty skips what is between a value's quotes when it counts parentheses and splits and(...)
// terms and [in] lists, so a value may hold , ' ( ) [ ] (v2.52, probed Nov 2026).
func filterable(value string) bool {
	return value != "" && !notFilterable.MatchString(value) && len(escape(value)) <= maxValueBytes
}

// inFilterBytes is the room a value takes in an escaped [in] filter: the value, its quotes and
// a comma.
func inFilterBytes(value string) int {
	return len(escape(value)) + 9
}

// sizedBatches splits values into groups of up to maxBatch whose sizes add up to at most
// maxFilterBytes, so each group's request has a short enough URL.
func sizedBatches(values []string, size func(string) int) [][]string {
	var out [][]string
	for len(values) > 0 {
		n, total := 0, 0
		for n < min(maxBatch, len(values)) {
			s := size(values[n])
			if n > 0 && total+s > maxFilterBytes {
				break
			}
			total += s
			n++
		}
		out = append(out, values[:n])
		values = values[n:]
	}
	return out
}

// quoted returns the value as is, not JSON-escaped: Twenty would read ü as six characters,
// so "ü" would never match.
func quoted(value string) (string, error) {
	if !filterable(value) {
		return "", fmt.Errorf("%q can't go in a Twenty filter", value)
	}
	return `"` + value + `"`, nil
}

func filterEq(field, value string) (string, error) {
	q, err := quoted(value)
	if err != nil {
		return "", err
	}
	return field + "[eq]:" + q, nil
}

// filterIn writes no spaces between values: Twenty reads a space after a comma as part of the
// next value.
func filterIn(field string, values []string) (string, error) {
	qs := make([]string, len(values))
	for i, v := range values {
		q, err := quoted(v)
		if err != nil {
			return "", err
		}
		qs[i] = q
	}
	return field + "[in]:[" + strings.Join(qs, ",") + "]", nil
}

// escape encodes a query parameter value, or a part of a compound key, keeping only letters, digits
// and "-._~", with uppercase hex: "a/b c" is "a%2Fb%20c".
func escape(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

// authError is a 401 or 403: the key is wrong, expired or revoked, or its role lacks a
// permission. Retrying won't help.
type authError struct{ message string }

func (e *authError) Error() string {
	return "Twenty refused the API key (check that it is current and its role can read and write the objects, and delete their records if a binding trashes or deletes them): " + e.message
}

// schemaError means Twenty has no object or field by a name the connector sent.
type schemaError struct{ message string }

func (e *schemaError) Error() string {
	return "Twenty doesn't have an object or field the binding names: " + e.message
}

// rejectedError is a 4xx about the records sent: a bad value, a duplicate, a missing parent.
type rejectedError struct {
	status   int
	messages []string // with emails masked
	raw      []string // as Twenty sent them, for rejectedFields to match values; never logged
}

func (e *rejectedError) Error() string {
	return fmt.Sprintf("Twenty rejected the records (HTTP %d): %s", e.status, strings.Join(e.messages, "; "))
}

type record = map[string]any

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type objectMeta struct {
	ID         string      `json:"id"`
	NamePlural string      `json:"namePlural"`
	IsActive   bool        `json:"isActive"`
	Fields     []fieldMeta `json:"fields"`
}

type fieldMeta struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	IsUnique bool   `json:"isUnique"`
	IsActive bool   `json:"isActive"`
	IsSystem bool   `json:"isSystem"`
	// Writability is "OPEN" for a field that API keys may write. Twenty writes "SYSTEM" ones.
	Writability string `json:"writability"`
	Settings    struct {
		// Type is "domain" for a links field, like a company's domainName, that Twenty stores
		// as a bare host.
		Type string `json:"type"`
		// A relation's side: "MANY_TO_ONE" for a field like a person's company, which holds the
		// other record's id in its join column, like "companyId". "ONE_TO_MANY" for the list on
		// the other side, like a company's people.
		RelationType   string `json:"relationType"`
		JoinColumnName string `json:"joinColumnName"`
	} `json:"settings"`
	// MorphID is shared by a morph relation's fields: one per object it can point at, like a note
	// target's targetCompany and targetPerson, and a generic "target" without a join column.
	MorphID string `json:"morphId"`
}

type client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	limiter *rate.Limiter
	// sleep waits for a backoff. Tests replace it.
	sleep func(context.Context, time.Duration) error

	mu        sync.Mutex
	notBefore time.Time // Twenty's Retry-After holds for every later request too
}

func newClient(baseURL, apiKey string, requestsPerMinute float64, burst int) *client {
	return &client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http: &http.Client{
			Timeout: requestTimeout,
			// A redirect would turn a write into a GET and resend the key, so it stops the task.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		limiter: rate.NewLimiter(rate.Limit(requestsPerMinute/60), burst),
		sleep:   sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// objects lists the workspace's objects with their fields.
func (c *client) objects(ctx context.Context) ([]objectMeta, error) {
	var resp struct {
		Data     []objectMeta `json:"data"`
		PageInfo pageInfo     `json:"pageInfo"`
	}
	if err := c.request(ctx, http.MethodGet, "/rest/metadata/objects?limit=200", nil, &resp); err != nil {
		return nil, err
	}
	if resp.PageInfo.HasNextPage {
		return nil, errors.New("the workspace has more than 200 objects, and the connector reads only the first 200")
	}
	return resp.Data, nil
}

// createField adds a field to an object with Twenty's metadata API, labelled with its name.
func (c *client) createField(ctx context.Context, objectID string, f newField) error {
	body := map[string]any{"objectMetadataId": objectID, "type": f.typ, "name": f.name, "label": f.name}
	if f.settings != nil {
		body["settings"] = f.settings
	}
	if f.unique {
		// Twenty makes the field unique as it creates it, records already there included, which
		// hold no value (Twenty v2.52, probed Nov 2026).
		body["isUnique"] = true
	}
	err := c.request(ctx, http.MethodPost, "/rest/metadata/fields", body, nil)
	var rej *rejectedError
	if errors.As(err, &rej) {
		return fmt.Errorf("Twenty refused the field (HTTP %d): %s", rej.status, strings.Join(rej.messages, "; "))
	}
	return err
}

// relationTarget returns the plural name of the object that a relation field points at.
// Twenty's REST metadata leaves it out, so this asks its GraphQL metadata API, which takes the
// same API key.
func (c *client) relationTarget(ctx context.Context, fieldID string) (string, error) {
	var field *struct {
		Relation *struct {
			Target struct {
				NamePlural string `json:"namePlural"`
			} `json:"targetObjectMetadata"`
		} `json:"relation"`
	}
	query := `query($id: UUID!) { field(id: $id) { relation { targetObjectMetadata { namePlural } } } }`
	if err := c.metadataField(ctx, query, fieldID, &field); err != nil {
		return "", err
	}
	if field == nil || field.Relation == nil || field.Relation.Target.NamePlural == "" {
		return "", errors.New("Twenty's metadata API didn't say which object the relation points at")
	}
	return field.Relation.Target.NamePlural, nil
}

// morphTarget returns the plural name of the object that one field of a morph relation, like
// noteTargets.targetCompany, points at. Twenty's metadata lists every object the morph relation
// can point at for each of its fields, so this picks the one Twenty named the field after: the
// field "target" + its singular name, with that name + "Id" as the join column (Twenty v2.52).
// It returns "" if no one object fits.
func (c *client) morphTarget(ctx context.Context, f fieldMeta) (string, error) {
	var field *struct {
		Morph []struct {
			Target struct {
				NamePlural   string `json:"namePlural"`
				NameSingular string `json:"nameSingular"`
			} `json:"targetObjectMetadata"`
		} `json:"morphRelations"`
	}
	query := `query($id: UUID!) { field(id: $id) { morphRelations { targetObjectMetadata { namePlural nameSingular } } } }`
	if err := c.metadataField(ctx, query, f.ID, &field); err != nil {
		return "", err
	}
	if field == nil {
		return "", errors.New("Twenty's metadata API didn't say which objects the relation points at")
	}
	var found []string
	for _, m := range field.Morph {
		s := m.Target.NameSingular
		if s != "" && "target"+strings.ToUpper(s[:1])+s[1:] == f.Name && f.Settings.JoinColumnName == f.Name+"Id" {
			found = append(found, m.Target.NamePlural)
		}
	}
	if len(found) != 1 {
		return "", nil
	}
	return found[0], nil
}

// metadataField asks Twenty's GraphQL metadata API a query about the field with id, and decodes
// the answer's field into field.
func (c *client) metadataField(ctx context.Context, query, id string, field any) error {
	body := map[string]any{"query": query, "variables": map[string]any{"id": id}}
	var resp struct {
		Data struct {
			Field json.RawMessage `json:"field"`
		} `json:"data"`
		// GraphQL answers errors with HTTP 200.
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := c.request(ctx, http.MethodPost, "/metadata", body, &resp); err != nil {
		return err
	}
	if len(resp.Errors) > 0 {
		return fmt.Errorf("Twenty's metadata API answered: %s", resp.Errors[0].Message)
	}
	if len(resp.Data.Field) == 0 {
		return nil
	}
	return json.Unmarshal(resp.Data.Field, field)
}

// find returns the records whose field (or "field.subfield") holds one of values. Twenty hides
// soft-deleted records unless asked, but its writes still match them, so identity and uniqueness
// checks pass includeDeleted. Values that can't go in a filter never match.
func (c *client) find(ctx context.Context, object, field string, values []string, includeDeleted bool) ([]record, error) {
	var keep []string
	seen := make(map[string]bool)
	for _, v := range values {
		if filterable(v) && !seen[v] {
			seen[v] = true
			keep = append(keep, v)
		}
	}
	var rows []record
	for _, batch := range sizedBatches(keep, inFilterBytes) {
		flt, err := filterIn(field, batch)
		if err != nil {
			return nil, err
		}
		if includeDeleted {
			flt = "and(" + flt + "," + anyDeleted + ")"
		}
		found, err := c.findWhere(ctx, object, flt)
		if err != nil {
			return nil, err
		}
		rows = append(rows, found...)
	}
	return rows, nil
}

// findWhere returns every record that matches a Twenty filter.
func (c *client) findWhere(ctx context.Context, object, filter string) ([]record, error) {
	var rows []record
	err := c.pages(ctx, object, filter, func(page []record) error {
		rows = append(rows, page...)
		return nil
	})
	return rows, err
}

// pages calls fn with the records that match a filter, up to 200 at a time. Each page starts
// after the last record read, so fn may change the records it gets, even so that they stop
// matching, without the next page skipping any.
func (c *client) pages(ctx context.Context, object, filter string, fn func([]record) error) error {
	cursor := ""
	for {
		path := fmt.Sprintf("/rest/%s?limit=%d&filter=%s", object, pageSize, escape(filter))
		if cursor != "" {
			path += "&starting_after=" + escape(cursor)
		}
		var resp struct {
			Data     map[string][]record `json:"data"`
			PageInfo pageInfo            `json:"pageInfo"`
		}
		if err := c.request(ctx, http.MethodGet, path, nil, &resp); err != nil {
			return err
		}
		if err := fn(resp.Data[object]); err != nil {
			return err
		}
		if !resp.PageInfo.HasNextPage {
			return nil
		}
		if resp.PageInfo.EndCursor == "" || resp.PageInfo.EndCursor == cursor {
			// A partial list would read as records missing from Twenty.
			return fmt.Errorf("listing %s: Twenty says there are more pages but gives no new cursor", object)
		}
		cursor = resp.PageInfo.EndCursor
	}
}

// create adds records with Twenty's plain batch create. Twenty rejects the whole batch if any
// unique value is already on another record, soft-deleted ones included, so it never takes a
// record over.
func (c *client) create(ctx context.Context, object string, records []record) ([]record, error) {
	return c.batch(ctx, "/rest/batch/"+object, records)
}

// upsert is Twenty's batch create-or-update. It matches on ANY unique field, not only id,
// restores a soft-deleted match, and rejects the whole batch if one record is bad.
func (c *client) upsert(ctx context.Context, object string, records []record) ([]record, error) {
	return c.batch(ctx, "/rest/batch/"+object+"?upsert=true", records)
}

func (c *client) batch(ctx context.Context, path string, records []record) ([]record, error) {
	if len(records) == 0 || len(records) > maxBatch {
		return nil, fmt.Errorf("a batch takes 1 to %d records, got %d", maxBatch, len(records))
	}
	var resp struct {
		Data map[string][]record `json:"data"`
	}
	if err := c.request(ctx, http.MethodPost, path, records, &resp); err != nil {
		return nil, err
	}
	return onlyList(resp.Data), nil
}

// update sets values on the records among ids that are live and match where (a Twenty filter,
// or ""), in one request, and returns the records it changed. The ids and where go in the URL,
// so the caller keeps them short, as sizedBatches does. Twenty's filtered update never
// creates a record. It does edit soft-deleted ones unless the filter says deletedAt[is]:NULL,
// so this always says it. A record deleted or changed since it was read no longer matches, so
// the update leaves it alone.
func (c *client) update(ctx context.Context, object string, ids []string, values record, where string) ([]record, error) {
	flt, err := liveFilter("an update", ids, where)
	if err != nil {
		return nil, err
	}
	return c.mutate(ctx, http.MethodPatch, "/rest/"+object+"?filter="+flt, values)
}

// remove deletes the records among ids that are live and match where (a Twenty filter, or ""),
// in one request, and returns the ids it deleted. soft moves them to Twenty's trash; otherwise
// they're deleted for good. Like update, it leaves alone a record deleted or changed since it
// was read, and the caller keeps the URL short.
func (c *client) remove(ctx context.Context, object string, ids []string, where string, soft bool) ([]record, error) {
	flt, err := liveFilter("a delete", ids, where)
	if err != nil {
		return nil, err
	}
	// Twenty deletes for good unless told soft_delete=true.
	return c.mutate(ctx, http.MethodDelete, "/rest/"+object+"?filter="+flt+"&soft_delete="+strconv.FormatBool(soft), nil)
}

// liveFilter returns the escaped filter for the live records among ids that match where. The
// id list is what keeps an update or delete from reaching every record.
func liveFilter(what string, ids []string, where string) (string, error) {
	if len(ids) == 0 || len(ids) > maxBatch {
		return "", fmt.Errorf("%s takes 1 to %d ids, got %d", what, maxBatch, len(ids))
	}
	idFilter, err := filterIn("id", ids)
	if err != nil {
		return "", err
	}
	flt := "and(" + idFilter + ",deletedAt[is]:NULL"
	if where != "" {
		flt += "," + where
	}
	return escape(flt + ")"), nil
}

func (c *client) mutate(ctx context.Context, method, path string, body any) ([]record, error) {
	var resp struct {
		Data map[string][]record `json:"data"`
	}
	if err := c.request(ctx, method, path, body, &resp); err != nil {
		return nil, err
	}
	return onlyList(resp.Data), nil
}

// onlyList returns the list in a response like {"data": {"createCompanies": [...]}}.
func onlyList(data map[string][]record) []record {
	for _, rows := range data {
		return rows
	}
	return nil
}

// request sends one request and decodes the JSON answer into out, unless out is nil. It keeps
// to the rate limit, and retries 429, 5xx and network failures with backoff, honoring
// Retry-After.
func (c *client) request(ctx context.Context, method, path string, payload, out any) error {
	var body []byte
	if payload != nil {
		var err error
		if body, err = json.Marshal(payload); err != nil {
			return fmt.Errorf("encoding the request body: %w", err)
		}
	}
	name := method + " " + strings.SplitN(path, "?", 2)[0]

	for attempt := 1; ; attempt++ {
		if err := c.waitTurn(ctx); err != nil {
			return err
		}
		status, header, data, err := c.send(ctx, method, path, body)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var failure string
		if err != nil {
			failure = err.Error()
		} else {
			// Twenty's messages can repeat the values sent.
			raw := messages(data, strings.HasPrefix(path, "/rest/metadata"))
			msgs := make([]string, len(raw))
			for i, m := range raw {
				msgs[i] = email.ReplaceAllString(m, "<email>")
			}
			failure = fmt.Sprintf("HTTP %d: %s", status, strings.Join(msgs, "; "))
			switch {
			case status >= 200 && status < 300:
				if out == nil || len(data) == 0 {
					return nil
				}
				if err := json.Unmarshal(data, out); err != nil {
					return fmt.Errorf("decoding the answer to %s: %w", name, err)
				}
				return nil
			case status == 401 || status == 403:
				return &authError{message: failure}
			case status == 500 && slices.Contains(raw, "Data validation error."):
				// Postgres refused a value Twenty passed on, so the records are at fault and a
				// retry can't help (Twenty v2.52, Nov 2026).
				return &rejectedError{status: status, messages: msgs, raw: raw}
			case status >= 300 && status < 400:
				// Without its query, which repeats a lookup's filter values.
				location, _, _ := strings.Cut(header.Get("Location"), "?")
				return fmt.Errorf("%s answered with a redirect to %q; set the Twenty URL to the address it redirects to", name, location)
			case !retryStatuses[status]:
				if status == 404 || strings.Contains(strings.Join(msgs, " "), "doesn't have any") {
					if len(msgs) == 0 {
						msgs = []string{name + " returned 404"}
					}
					return &schemaError{message: strings.Join(msgs, "; ")}
				}
				if !rejectStatuses[status] {
					return fmt.Errorf("%s failed: %s", name, failure)
				}
				return &rejectedError{status: status, messages: msgs, raw: raw}
			}
		}

		wait, asked := retryAfter(header)
		if asked {
			c.mu.Lock()
			if until := time.Now().Add(wait); until.After(c.notBefore) {
				c.notBefore = until
			}
			c.mu.Unlock()
		}
		if attempt >= maxAttempts {
			return fmt.Errorf("%s failed %d times; last: %s", name, attempt, failure)
		}
		if !asked {
			base := min(30*time.Second, time.Second<<(attempt-1))
			if status == http.StatusTooManyRequests {
				// Twenty counts requests per minute, so a shorter wait would land in the same window.
				base = rateWindow
			}
			wait = time.Duration((0.5 + rand.Float64()/2) * float64(base))
		}
		log.WithFields(log.Fields{"request": name, "attempt": attempt, "wait": wait.String(), "failure": failure}).
			Info("retrying a Twenty request")
		if err := c.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// waitTurn waits out any Retry-After from Twenty, then for the rate limiter.
func (c *client) waitTurn(ctx context.Context) error {
	c.mu.Lock()
	wait := time.Until(c.notBefore)
	c.mu.Unlock()
	if err := c.sleep(ctx, wait); err != nil {
		return err
	}
	return c.limiter.Wait(ctx)
}

func (c *client) send(ctx context.Context, method, path string, body []byte) (int, http.Header, []byte, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, nil, withoutQuery(err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, withoutQuery(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return 0, nil, nil, err
	}
	if len(data) > maxResponseBytes {
		return 0, nil, nil, fmt.Errorf("the answer to %s %s is over %d bytes", method, req.URL.Path, maxResponseBytes)
	}
	return resp.StatusCode, resp.Header, data, nil
}

// withoutQuery takes the query out of the URL a transport error names, since a lookup's filter
// there holds the values it looks up, like keys and emails.
func withoutQuery(err error) error {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		uerr.URL, _, _ = strings.Cut(uerr.URL, "?")
	}
	return err
}

// messages reads Twenty's error messages from an answer: {"messages": [...]}, or a single
// "message" or "error", and for the metadata API the messages nested under "errors".
func messages(data []byte, nested bool) []string {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		if len(data) == 0 {
			return nil
		}
		return []string{string(data[:min(len(data), 300)])}
	}
	var out []string
	for _, k := range []string{"messages", "message", "error"} {
		raw, ok := body[k]
		if !ok {
			continue
		}
		var list []any
		if json.Unmarshal(raw, &list) == nil {
			for _, m := range list {
				out = append(out, fmt.Sprint(m))
			}
			break
		}
		var one any
		if json.Unmarshal(raw, &one) == nil && one != nil {
			out = append(out, fmt.Sprint(one))
			break
		}
	}
	// The metadata API puts what was wrong under "errors", like
	// {"errors":{"fieldMetadata":[{"errors":[{"message":"Name is too long"}]}]}} (Twenty v2.52).
	var errs any
	if raw, ok := body["errors"]; ok && nested && json.Unmarshal(raw, &errs) == nil {
		var walk func(v any)
		walk = func(v any) {
			switch v := v.(type) {
			case map[string]any:
				if m, ok := v["message"].(string); ok {
					out = append(out, m)
				}
				for _, k := range slices.Sorted(maps.Keys(v)) {
					walk(v[k])
				}
			case []any:
				for _, e := range v {
					walk(e)
				}
			}
		}
		walk(errs)
	}
	return out
}

// retryAfter reads a Retry-After header, in seconds or as an HTTP date.
func retryAfter(header http.Header) (time.Duration, bool) {
	value := header.Get("Retry-After")
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		return time.Duration(max(0, seconds) * float64(time.Second)), true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, time.Until(at)), true
	}
	return 0, false
}
