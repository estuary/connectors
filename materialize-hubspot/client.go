package hubspot

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	//go:embed VERSION
	rawVersion string

	versionRegex = regexp.MustCompile(`^v(\d+)\s*$`)
)

func productVersion() string {
	matches := versionRegex.FindStringSubmatch(rawVersion)
	if len(matches) != 2 {
		panic("unable to parse version")
	}
	return matches[1] + ".0.0"
}

func ProductToken() string {
	return "Estuary/" + productVersion()
}

const (
	// MaxResponseBytes is used to limit response size.
	//
	// The largest response the connector handles is reading the list of
	// CRM properties.  Out of the box this is about 400K each for Companies
	// and Contacts.
	MaxResponseBytes = 10 * 1024 * 1024 // 10MiB

	// MaxSearchFilterGroups is the maximum number of filterGroups in a search
	// query.
	//
	// > You can include a maximum of five filterGroups with up to 6 filters in
	// > each group, with a maximum of 18 filters in total.
	MaxSearchFilterGroups = 5

	// MaxSearchPageLimit is the maximum number of results allowed per page
	// of search results if you set the limit.
	MaxSearchPageLimit = 200

	// MaxBatchRecords is the maximum number of records a batch query can
	// operate on.
	//
	// > Object API batch endpoints are limited to 100 inputs per request. For
	// > example, create or retrieve up to 100 contacts per request.
	MaxBatchRecords = 100

	// Maximum length of a property name.
	MaxPropertyNameLength = 100

	// Actual rate limits depend on the associated app, and if the user has
	// purchased the API limit increase.  The search API has a separate rate
	// limit from the reset of the CRM API.
	//
	// https://developers.hubspot.com/docs/developer-tooling/platform/usage-guidelines
	DefaultLimitRatePerSecond       = 10
	DefaultBurst                    = 100
	DefaultSearchLimitRatePerSecond = 5
	DefaultSearchBurst              = 5

	// DisplayOrderLast indicates that HubSpot should display the resource
	// after any resources with a positive integer value.
	DisplayOrderLast = -1
)

var (
	baseURL         *url.URL      = MustParseURL("https://api.hubspot.com")
	tokenPath       string        = "/oauth/2026-03/token"
	objectPath      string        = "/crm/objects/2026-03"
	propertyPath    string        = "/crm/properties/2026-03"
	refreshLeadTime time.Duration = 5 * time.Minute
)

// DetailedError is included in the response for some, but not all, APIs.
type DetailedError struct {
	Code    string          `json:"code"`
	Context json.RawMessage `json:"context"`
	Message string          `json:"message"`
}

type APIError struct {
	StatusCode    int             `json:"-"`
	Status        string          `json:"status"`
	Errors        []DetailedError `json:"errors"`
	Message       string          `json:"message"`
	CorrelationID string          `json:"correlationId"`
	Category      string          `json:"category"`
}

func (e *APIError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("%d %s", e.StatusCode, http.StatusText(e.StatusCode))
	}

	if len(e.Errors) == 1 {
		return fmt.Sprintf("%d %s: %s", e.StatusCode, http.StatusText(e.StatusCode), e.Errors[0].Message)
	}
	return fmt.Sprintf("%d %s: %s", e.StatusCode, http.StatusText(e.StatusCode), e.Message)
}

type TokenResponse struct {
	RefreshToken Secret `json:"refresh_token"`
	AccessToken  Secret `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
}

type TokenUpdate struct {
	RefreshToken         Secret    `json:"refresh_token"`
	AccessToken          Secret    `json:"access_token"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
}

type PropertyGroup struct {
	Archived     bool   `json:"archived"`
	Label        string `json:"label"`
	Name         string `json:"name"`
	DisplayOrder int    `json:"displayOrder"`
}

type PropertyModificationMetadata struct {
	Archivable    bool `json:"archivable"`
	ReadOnlyValue bool `json:"readOnlyValue"`
}

type Filter struct {
	PropertyName string `json:"propertyName"`
	Operator     string `json:"operator"`
	Value        any    `json:"value,omitempty"`
	Values       []any  `json:"values,omitempty"`
}

// FilterGroup is a logical AND of filters.
type FilterGroup struct {
	Filters []Filter `json:"filters"`
}

// NewFilterGroupsEquals creates filter groups for selecting objects by value.
// You can have up to 5 filter groups per request, not enforced here.  Each
// filter group could contain 6 filters but for this one they always have a
// single filter.
func NewFilterGroupsEquals[T any](name string, values []T) []*FilterGroup {
	groups := make([]*FilterGroup, 0, len(values))
	for _, value := range values {
		groups = append(groups, &FilterGroup{
			Filters: []Filter{
				{
					PropertyName: name,
					Operator:     "EQ",
					Value:        value,
				},
			},
		})
	}
	return groups
}

func NewFilterGroupsIn[T any](name string, values []T) []*FilterGroup {
	items := make([]any, 0, len(values))
	for _, v := range values {
		items = append(items, v)
	}

	return []*FilterGroup{
		{
			Filters: []Filter{
				{
					PropertyName: name,
					Operator:     "IN",
					Values:       items,
				},
			},
		},
	}
}

// SearchRequest is the request body for the search API.
//
// The FilterGroups are logically OR'd together.
type SearchRequest struct {
	FilterGroups []*FilterGroup `json:"filterGroups"`
	Properties   []string       `json:"properties,omitempty"`
	Limit        int            `json:"limit,omitempty"`
	After        any            `json:"after,omitempty"`
}

func (r *SearchRequest) Next(response *SearchResponse) {
	r.After = response.Paging.Next.After
}

type Page struct {
	After  string `json:"after"`
	Before string `json:"before"`
	Link   string `json:"link"`
}

type Paging struct {
	Next Page `json:"next"`
	Prev Page `json:"prev"`
}

type SearchResult struct {
	ID string `json:"id"`
	// Values are string or null.
	Properties map[string]any `json:"properties"`
}

type SearchResponse struct {
	Total   int            `json:"total"`
	Results []SearchResult `json:"results"`
	Paging  Paging         `json:"paging"`
}

func (r *SearchResponse) NextPageRequest(request *SearchRequest) (*SearchRequest, bool) {
	if r.Paging.Next.After == "" {
		return nil, false
	}

	request.After = r.Paging.Next.After
	return request, true
}

type BatchReadRequest struct {
	IDProperty string              `json:"idProperty,omitempty"`
	Inputs     []map[string]string `json:"inputs"`
	Properties []string            `json:"properties"`
}

func NewBatchReadInputs(ids []string) []map[string]string {
	inputs := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		inputs = append(inputs, map[string]string{
			"id": id,
		})
	}
	return inputs
}

// IDProperty must be a unique property.
type BatchUpsertInput struct {
	IDProperty string         `json:"idProperty"`
	ID         any            `json:"id"`
	Properties map[string]any `json:"properties"`
}

// IDProperty must be unique or the record id.
type BatchUpdateInput struct {
	IDProperty string         `json:"idProperty,omitempty"`
	ID         any            `json:"id"`
	Properties map[string]any `json:"properties"`
}

type BatchCreateInput struct {
	Properties map[string]any `json:"properties"`
}

type PropertyOption struct {
	Description  string `json:"description"`
	DisplayOrder int    `json:"display_order"`
	Hidden       bool   `json:"hidden"`
	Label        string `json:"label"`
	Value        string `json:"value"`
}

// PropertyType are the types in HubSpot for a property.
//
// https://developers.hubspot.com/docs/api-reference/latest/crm/properties/guide#property-type-and-fieldtype-values
type PropertyType string

const (
	BoolPropertyType     PropertyType = "bool"
	EnumPropertyType     PropertyType = "enumeration"
	DatePropertyType     PropertyType = "date"
	DatetimePropertyType PropertyType = "datetime"
	StringPropertyType   PropertyType = "string"
	NumberPropertyType   PropertyType = "number"
)

var (
	allPropertyTypes = []PropertyType{
		BoolPropertyType,
		EnumPropertyType,
		DatePropertyType,
		DatetimePropertyType,
		StringPropertyType,
		NumberPropertyType,
	}

	allPropertyFieldTypes = []PropertyFieldType{
		BooleanCheckboxPropertyFieldType,
		CheckboxPropertyFieldType,
		DatePropertyFieldType,
		NumberPropertyFieldType,
		PhonenumberPropertyFieldType,
		RadioPropertyFieldType,
		TextPropertyFieldType,
		TextAreaPropertyFieldType,
	}
)

func (t PropertyType) IsValid() bool {
	return slices.Contains(allPropertyTypes, t)
}

type PropertyFieldType string

const (
	BooleanCheckboxPropertyFieldType PropertyFieldType = "booleancheckbox"
	CheckboxPropertyFieldType        PropertyFieldType = "checkbox"
	DatePropertyFieldType            PropertyFieldType = "date"
	NumberPropertyFieldType          PropertyFieldType = "number"
	PhonenumberPropertyFieldType     PropertyFieldType = "phonenumber"
	RadioPropertyFieldType           PropertyFieldType = "radio"
	TextAreaPropertyFieldType        PropertyFieldType = "textarea"
	TextPropertyFieldType            PropertyFieldType = "text"
)

func (t PropertyFieldType) IsValid() bool {
	return slices.Contains(allPropertyFieldTypes, t)
}

type Property struct {
	FieldType            PropertyFieldType            `json:"fieldType"`
	GroupName            string                       `json:"groupName"`
	Label                string                       `json:"label"`
	Name                 string                       `json:"name"`
	Type                 PropertyType                 `json:"type"`
	Description          string                       `json:"description,omitempty"`
	HasUniqueValue       bool                         `json:"hasUniqueValue,omitempty"`
	ModificationMetadata PropertyModificationMetadata `json:"modificationMetadata,omitempty"`
	Options              []PropertyOption             `json:"options,omitempty"`
}

func NewLimiter(limit float64, burst int) *rate.Limiter {
	if limit == 0.0 {
		limit = DefaultLimitRatePerSecond
	}
	if burst == 0 {
		burst = DefaultBurst
	}
	return rate.NewLimiter(rate.Limit(limit), burst)
}

func NewSearchLimiter(limit float64, burst int) *rate.Limiter {
	if limit == 0.0 {
		limit = DefaultSearchLimitRatePerSecond
	}
	if burst == 0 {
		burst = DefaultSearchBurst
	}
	return rate.NewLimiter(rate.Limit(limit), burst)
}

type Client struct {
	httpClient    *http.Client
	limiter       *rate.Limiter
	searchLimiter *rate.Limiter
	productToken  string

	sync.RWMutex
	credentials Credentials
}

func NewClient(credentials Credentials, limiter, searchLimiter *rate.Limiter) (*Client, error) {
	if limiter == nil {
		limiter = rate.NewLimiter(rate.Limit(DefaultLimitRatePerSecond), DefaultBurst)
	}
	if searchLimiter == nil {
		searchLimiter = rate.NewLimiter(rate.Limit(DefaultSearchLimitRatePerSecond), DefaultSearchBurst)
	}

	return &Client{
		credentials: credentials,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		limiter:       limiter,
		searchLimiter: searchLimiter,
		productToken:  ProductToken(),
	}, nil
}

func NewClientDefaultLimiter(credentials Credentials) (*Client, error) {
	return NewClient(credentials, nil, nil)
}

func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}

// BatchRead uses the Batch API to retrieve a list of objects with the
// idProperty set to one of the values.  The idProperty must be a unique
// property.
func (c *Client) BatchRead(
	ctx context.Context,
	object CRMObject,
	read *BatchReadRequest,
) ([]json.RawMessage, error) {
	if len(read.Inputs) > MaxBatchRecords {
		return nil, fmt.Errorf("unable to get batch with more than 100 records")
	}

	data, err := json.Marshal(&read)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal batch load body: %w", err)
	}

	type Response struct {
		Results []json.RawMessage `json:"results"`
	}
	var value Response
	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		uri := baseURL.JoinPath(objectPath, object.String(), "batch", "read")
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		// Returns 207 if one or more records were not matched.
		if resp.StatusCode != 200 && resp.StatusCode != 207 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &value, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return value.Results, nil
}

func (c *Client) BatchUpsert(
	ctx context.Context,
	object CRMObject,
	inputs []*BatchUpsertInput,
) error {
	if len(inputs) > MaxBatchRecords {
		return fmt.Errorf("exceeded request record limit: %d", MaxBatchRecords)
	}

	upsert := struct {
		Inputs []*BatchUpsertInput `json:"inputs"`
	}{
		Inputs: inputs,
	}
	data, err := json.Marshal(&upsert)
	if err != nil {
		return fmt.Errorf("unable to marshal request: %w", err)
	}

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		uri := baseURL.JoinPath(objectPath, object.String(), "batch/upsert")
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) BatchUpdate(
	ctx context.Context,
	object CRMObject,
	inputs []*BatchUpdateInput,
) error {
	if len(inputs) > MaxBatchRecords {
		return fmt.Errorf("exceeded request record limit: %d", MaxBatchRecords)
	}

	update := struct {
		Inputs []*BatchUpdateInput `json:"inputs"`
	}{
		Inputs: inputs,
	}
	data, err := json.Marshal(&update)
	if err != nil {
		return fmt.Errorf("unable to marshal request: %w", err)
	}

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		uri := baseURL.JoinPath(objectPath, object.String(), "batch/update")
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) BatchCreate(
	ctx context.Context,
	object CRMObject,
	inputs []*BatchCreateInput,
) error {
	if len(inputs) > MaxBatchRecords {
		return fmt.Errorf("exceeded request record limit: %d", MaxBatchRecords)
	}

	create := struct {
		Inputs []*BatchCreateInput `json:"inputs"`
	}{
		Inputs: inputs,
	}
	data, err := json.Marshal(&create)
	if err != nil {
		return fmt.Errorf("unable to marshal request: %w", err)
	}

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		uri := baseURL.JoinPath(objectPath, object.String(), "batch/create")
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 201 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) Search(
	ctx context.Context,
	object CRMObject,
	search *SearchRequest,
) (*SearchResponse, error) {
	if len(search.FilterGroups) > MaxSearchFilterGroups {
		return nil, fmt.Errorf("unable to search more than %d filter groups", MaxSearchFilterGroups)
	}

	data, err := json.Marshal(&search)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal search request: %w", err)
	}

	var searchResponse SearchResponse

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		uri := baseURL.JoinPath(objectPath, object.String(), "search")
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.searchLimiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &searchResponse, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &searchResponse, nil
}

func (c *Client) ListPropertyGroups(ctx context.Context, object CRMObject) ([]*PropertyGroup, error) {
	uri := baseURL.JoinPath(propertyPath, object.String(), "groups")

	type Response struct {
		Results []*PropertyGroup `json:"results"`
	}

	var value Response
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "GET", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &value, MaxResponseBytes)
		if err != nil {
			return fmt.Errorf("unable to parse property groups: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return value.Results, nil
}

func (c *Client) CreatePropertyGroup(ctx context.Context, object CRMObject, group *PropertyGroup) error {
	uri := baseURL.JoinPath(propertyPath, object.String(), "groups")

	data, err := json.Marshal(&group)
	if err != nil {
		return fmt.Errorf("unable to marshal property group: %w", err)
	}

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 201 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}
		return nil
	})
	return err
}

// GetPropertyGroup returns property group by name.
func (c *Client) GetPropertyGroup(ctx context.Context, object CRMObject, groupName string) (*PropertyGroup, error) {
	uri := baseURL.JoinPath(propertyPath, object.String(), "groups", groupName)

	var propertyGroup PropertyGroup
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "GET", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &propertyGroup, MaxResponseBytes)
		if err != nil {
			return fmt.Errorf("unable to parse property group: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &propertyGroup, nil
}

// DeletePropertyGroup deletes a property group by name.
func (c *Client) DeletePropertyGroup(ctx context.Context, object CRMObject, groupName string) error {
	uri := baseURL.JoinPath(propertyPath, object.String(), "groups", groupName)

	return Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "DELETE", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		// Assume that it was deleted when a prior attempt triggered a
		// temporary error.
		if resp.StatusCode == 404 && attempt > 1 {
			return nil
		}

		if resp.StatusCode != 204 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return nil
	})
}

func (c *Client) ListProperties(ctx context.Context, object CRMObject) ([]*Property, error) {
	uri := baseURL.JoinPath(propertyPath, object.String())

	type Response struct {
		Results []*Property `json:"results"`
	}

	var value Response
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "GET", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &value, MaxResponseBytes)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return value.Results, nil
}

func (c *Client) CreateProperty(ctx context.Context, object CRMObject, property *Property) error {
	uri := baseURL.JoinPath(propertyPath, object.String())

	data, err := json.Marshal(&property)
	if err != nil {
		return fmt.Errorf("unable to marshal property: %w", err)
	}

	err = Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "POST", uri, bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 201 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}
		return nil
	})
	return err
}

// GetProperty returns property by name.
func (c *Client) GetProperty(ctx context.Context, object CRMObject, name string) (*Property, error) {
	uri := baseURL.JoinPath(propertyPath, object.String(), name)

	var property Property
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "GET", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		err = parseBody(resp, &property, MaxResponseBytes)
		if err != nil {
			return fmt.Errorf("unable to parse property group: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &property, nil
}

func (c *Client) DeleteProperty(ctx context.Context, object CRMObject, name string) error {
	uri := baseURL.JoinPath(propertyPath, object.String(), name)

	return Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		req, err := c.newRequest(ctx, "DELETE", uri, nil)
		if err != nil {
			return err
		}

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		// Assume that property was deleted when a prior attempt triggered a
		// temporary error.
		if resp.StatusCode == 404 && attempt > 1 {
			return nil
		}

		if resp.StatusCode != 204 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return nil
	})
}

// AccessToken returns the access token, suitable to use as the bearer token.
// It may need to retrieve a new one from the HubSpot API if it is approaching
// expiration.
//
// With OAuth, we take care to only refresh the access token once.
func (c *Client) AccessToken(ctx context.Context, now time.Time) (Secret, error) {
	switch c.credentials.AuthType {
	case OAuth2AuthType:
		c.RLock()
		refreshTime := c.credentials.AccessTokenExpiresAt.Add(-refreshLeadTime)
		if now.Before(refreshTime) {
			c.RUnlock()
			return c.credentials.AccessToken, nil
		}
		c.RUnlock()
	case ServiceKeyAuthType:
		return c.credentials.ServiceKey, nil
	default:
		return "", fmt.Errorf("unknown auth type: %q", c.credentials.AuthType)
	}

	c.Lock()
	defer c.Unlock()

	// Since the read lock was released, we recheck the condition in case
	// another thread already refreshed the token.
	refreshTime := c.credentials.AccessTokenExpiresAt.Add(-refreshLeadTime)
	if now.Before(refreshTime) {
		return c.credentials.AccessToken, nil
	}

	update, err := c.RefreshTokens(ctx, now)
	if err != nil {
		return "", err
	}

	c.credentials.RefreshToken = update.RefreshToken
	c.credentials.AccessToken = update.AccessToken
	c.credentials.AccessTokenExpiresAt = update.AccessTokenExpiresAt

	return c.credentials.AccessToken, nil
}

// RefreshTokens retrieves new auth tokens.
func (c *Client) RefreshTokens(ctx context.Context, now time.Time) (*TokenUpdate, error) {
	params := url.Values{}
	params.Set("refresh_token", c.credentials.RefreshToken.Expose())
	params.Set("client_id", c.credentials.ClientID)
	params.Set("client_secret", c.credentials.ClientSecret.Expose())
	params.Set("grant_type", "refresh_token")

	uri := baseURL.JoinPath(tokenPath)

	var tokens TokenResponse
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		reqBody := strings.NewReader(params.Encode())
		req, err := http.NewRequestWithContext(ctx, "POST", uri.String(), reqBody)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", c.productToken)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		return parseBody(resp, &tokens, MaxResponseBytes)
	})
	if err != nil {
		return nil, err
	}

	return &TokenUpdate{
		RefreshToken:         tokens.RefreshToken,
		AccessToken:          tokens.AccessToken,
		AccessTokenExpiresAt: now.Add(time.Duration(tokens.ExpiresIn) * time.Second),
	}, nil
}

func (c *Client) InspectTokens(ctx context.Context) (map[string]any, error) {
	params := url.Values{}
	params.Set("refresh_token", c.credentials.RefreshToken.Expose())
	params.Set("client_id", c.credentials.ClientID)
	params.Set("client_secret", c.credentials.ClientSecret.Expose())
	params.Set("token_type_hint", "refresh_token")
	params.Set("token", c.credentials.RefreshToken.Expose())

	uri := baseURL.JoinPath(tokenPath, "introspect")

	var responseBody map[string]any
	err := Retry[*TemporaryError](ctx, DefaultBackoff, func(attempt int) error {
		reqBody := strings.NewReader(params.Encode())
		req, err := http.NewRequestWithContext(ctx, "POST", uri.String(), reqBody)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", c.productToken)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		start := time.Now()
		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}
		delay := time.Since(start)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		defer logExchange(resp, start, delay)

		if resp.StatusCode != 200 {
			apiError := parseAPIError(resp)
			return fmt.Errorf("unexpected response: %w", apiError)
		}

		if resp.ContentLength > int64(MaxResponseBytes) {
			return fmt.Errorf("response body too large: %d", resp.ContentLength)
		}

		return parseBody(resp, &responseBody, MaxResponseBytes)
	})
	if err != nil {
		return nil, err
	}

	return responseBody, nil
}

func (c *Client) newRequest(ctx context.Context, method string, u *url.URL, body io.Reader) (*http.Request, error) {
	token, err := c.AccessToken(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", c.productToken)
	req.Header.Set("Authorization", "Bearer "+token.Expose())
	return req, nil
}

func parseBody[T any](resp *http.Response, value *T, maxBytes int) error {
	if resp.ContentLength > int64(maxBytes) {
		return fmt.Errorf("response body too large: %d", resp.ContentLength)
	}

	body := io.LimitReader(resp.Body, int64(maxBytes)+1)
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("unable to read body: %w", err)
	}

	if len(data) > maxBytes {
		return fmt.Errorf("response body too large")
	}

	err = json.Unmarshal(data, &value)
	if err != nil {
		return fmt.Errorf("unable to parse json: %w", err)
	}

	return nil
}

func parseAPIError(resp *http.Response) error {
	apiError := &APIError{StatusCode: resp.StatusCode}

	// Try to read error information on a best effort basis.
	err := parseBody(resp, apiError, MaxResponseBytes)
	if err != nil {
		log.WithFields(log.Fields{
			"status": resp.StatusCode,
			"error":  err,
		}).Debug("unable to parse error response")
	}

	if resp.StatusCode >= 500 {
		return &TemporaryError{err: apiError}
	}
	if resp.StatusCode == 429 {
		return &TemporaryError{err: apiError, extraDelay: 10 * time.Second}
	}
	return apiError
}

func logExchange(resp *http.Response, startTime time.Time, delay time.Duration) {
	elapsed := time.Since(startTime)
	log.WithFields(log.Fields{
		"method":        resp.Request.Method,
		"authority":     resp.Request.URL.Host,
		"path":          resp.Request.URL.Path,
		"status":        resp.StatusCode,
		"limiter_delay": fmt.Sprintf("%.3fs", delay.Seconds()),
		"elapsed":       fmt.Sprintf("%.3fs", (elapsed - delay).Seconds()),
	}).Info("HTTP exchange complete")
}

func MustParseURL(rawURL string) *url.URL {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return u
}
