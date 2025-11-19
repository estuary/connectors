package catalog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"golang.org/x/oauth2/clientcredentials"
	"resty.dev/v3"
)

var (
	// ErrTableNotFound may be returned when attempting to get metadata for a
	// non-Iceberg table present in a Glue catalog.
	ErrTableNotFound = fmt.Errorf("table not found")
)

// TODO(whb): This is a specific form a credential vending for REST catalogs. It
// seems to be the most common for S3-backed catalogs, but is also a legacy form
// of the response. This will need to be expanded/modified to support credential
// vending from non-S3 catalogs, and probably even some S3 catalogs. Right now
// we only use credential vending in the iceberg helper script so it's not a big
// deal.
type tableConfig struct {
	ExpirationTime     time.Time `json:"expiration-time"`
	S3_AccessKeyId     string    `json:"s3.access-key-id"`
	S3_SecretAccessKey string    `json:"s3.secret-access-key"`
	S3_SessionToken    string    `json:"s3.session-token"`
}

// UnmarshalJSON for tableConfig casts the unix milliseconds for the expiration
// time as a native time.Time, which is a lot easier to work with.
func (t *tableConfig) UnmarshalJSON(data []byte) error {
	type Alias tableConfig
	aux := &struct {
		ExpirationTime string `json:"expiration-time"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if aux.ExpirationTime != "" {
		millis, err := strconv.Atoi(aux.ExpirationTime)
		if err != nil {
			return err
		}
		t.ExpirationTime = time.UnixMilli(int64(millis)).UTC()
	}

	return nil
}

type Table struct {
	Config           tableConfig    `json:"config"`
	MetadataLocation string         `json:"metadata-location"`
	Metadata         table.Metadata `json:"metadata"`
}

// TableRequirement is a condition that must hold when submitting a request to a
// catalog. Right now there's only assertCurrentSchemaID but it is structured to
// allow for adding more in the future, which may be particularly useful for
// performing atomic table appends of data files if we ever implement that.
type TableRequirement interface {
	isTableRequirement()
}

type baseRequirement struct {
	Type string `json:"type"`
}

type assertCurrentSchemaIdReq struct {
	baseRequirement
	CurrentSchemaID int `json:"current-schema-id"`
}

func (assertCurrentSchemaIdReq) isTableRequirement() {}

func AssertCurrentSchemaID(id int) TableRequirement {
	return &assertCurrentSchemaIdReq{
		baseRequirement: baseRequirement{Type: "assert-current-schema-id"},
		CurrentSchemaID: id,
	}
}

// TableUpdate is an update to a table. As above, this is currently fairly
// limited to adding a new schema to a table and setting the current schema ID -
// both of which should typically be done to accomplish a DDL change.
type TableUpdate interface {
	isTableUpdate()
}

type baseUpdate struct {
	Action string `json:"action"`
}

type addSchemaUpdateReq struct {
	baseUpdate
	Schema *iceberg.Schema `json:"schema"`
}

func (addSchemaUpdateReq) isTableUpdate() {}

func AddSchemaUpdate(schema *iceberg.Schema) TableUpdate {
	return &addSchemaUpdateReq{
		baseUpdate: baseUpdate{Action: "add-schema"},
		Schema:     schema,
	}
}

type setCurrentSchemaUpdateReq struct {
	baseUpdate
	SchemaID int `json:"schema-id"`
}

func (setCurrentSchemaUpdateReq) isTableUpdate() {}

func SetCurrentSchemaUpdate(id int) TableUpdate {
	return &setCurrentSchemaUpdateReq{
		baseUpdate: baseUpdate{Action: "set-current-schema"},
		SchemaID:   id,
	}
}

type catalogOpts struct {
	useClientCredential bool
	oauth2ServerUri     string
	credential          string
	scope               string

	useSigV4           bool
	awsAccessKeyID     string
	awsSecretAccessKey string
	awsRegion          string
	awsSessionToken    string
	signingName        string
}

type CatalogOption = func(*catalogOpts)

func WithClientCredential(credential string, oath2ServerUri string, scope *string) CatalogOption {
	return func(c *catalogOpts) {
		c.oauth2ServerUri = oath2ServerUri
		c.useClientCredential = true
		c.credential = credential
		if scope != nil {
			c.scope = *scope
		}
	}
}

func WithSigV4(signingName, awsAccessKeyID, awsSecretAccessKey, awsRegion, sessionToken string) CatalogOption {
	return func(c *catalogOpts) {
		c.useSigV4 = true
		c.awsAccessKeyID = awsAccessKeyID
		c.awsSecretAccessKey = awsSecretAccessKey
		c.awsRegion = awsRegion
		c.awsSessionToken = sessionToken
		c.signingName = signingName
	}
}

type Catalog struct {
	rHttp      *resty.Client
	isS3tables bool
}

func New(ctx context.Context, catalogUrl string, warehouse string, opts ...CatalogOption) (*Catalog, error) {
	cfg := &catalogOpts{}
	for _, opt := range opts {
		opt(cfg)
	}

	parsedCatalogUrl, err := url.Parse(catalogUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse catalog url: %w", err)
	}

	var rHttp *resty.Client
	if cfg.useClientCredential {
		var scopes []string
		if cfg.scope != "" {
			scopes = []string{cfg.scope}
		}

		if clientID, clientSecret, ok := strings.Cut(cfg.credential, ":"); ok {
			clientCredCfg := &clientcredentials.Config{
				ClientID:     clientID,
				ClientSecret: clientSecret,
				TokenURL:     parsedCatalogUrl.JoinPath(cfg.oauth2ServerUri).String(),
				Scopes:       scopes,
			}
			rHttp = resty.NewWithClient(clientCredCfg.Client(ctx))
		} else {
			rHttp = resty.New().SetAuthToken(cfg.credential)
		}
	} else if cfg.useSigV4 {
		rHttp = resty.New()
		rHttp = rHttp.SetTransport(&sigv4Transport{
			delegate: rHttp.Transport(),
			signer:   v4.NewSigner(),
			region:   cfg.awsRegion,
			creds: aws.Credentials{
				AccessKeyID:     cfg.awsAccessKeyID,
				SecretAccessKey: cfg.awsSecretAccessKey,
				SessionToken:    cfg.awsSessionToken,
			},
			signingName: cfg.signingName,
		})
	} else {
		return nil, errors.New("must provide an authentication option")
	}

	baseURL := parsedCatalogUrl.JoinPath("v1")
	rc := &Catalog{
		rHttp:      rHttp.SetBaseURL(baseURL.String()),
		isS3tables: cfg.signingName == "s3tables",
	}

	type configResponse struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}

	catalogCfg, err := doGet[configResponse](ctx, rc, "/config", [][]string{{"warehouse", warehouse}})
	if err != nil {
		return nil, err
	}

	// Get the extra URL prefix from the configuration defaults if it is there
	// or overrides, preferring the value from overrides if both are present.
	var prefix string
	if catalogCfg.Defaults != nil {
		if p, ok := catalogCfg.Defaults["prefix"]; ok {
			prefix = p
		}
	}
	if catalogCfg.Overrides != nil {
		if p, ok := catalogCfg.Overrides["prefix"]; ok {
			prefix = p

		}
	}
	if prefix != "" {
		rc.rHttp = rc.rHttp.SetBaseURL(baseURL.JoinPath(prefix).String())
	}

	return rc, nil
}

func (c *Catalog) ListNamespaces(ctx context.Context) ([]string, error) {
	type resp struct {
		Namespaces    [][]string `json:"namespaces"`
		NextPageToken string     `json:"next-page-token"`
	}

	pageSize := "100"
	pageToken := ""

	var names []string
	for {
		params := [][]string{{"pageSize", pageSize}}
		if pageToken != "" {
			params = append(params, []string{"pageToken", pageToken})
		}

		res, err := doGet[resp](ctx, c, "/namespaces", params)
		if err != nil {
			return nil, err
		}

		for _, ns := range res.Namespaces {
			names = append(names, ns[0])
		}

		if res.NextPageToken == "" {
			break
		}
		pageToken = res.NextPageToken
	}

	return names, nil
}

func (c *Catalog) CreateNamespace(ctx context.Context, ns string) error {
	if err := doPost(ctx, c, "/namespaces", map[string][]string{
		"namespace": {ns},
	}); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) ListTables(ctx context.Context, ns string) ([]string, error) {
	type resp struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
		NextPageToken string `json:"next-page-token"`
	}

	pageSize := "100"
	pageToken := ""

	var names []string
	for {
		params := [][]string{{"pageSize", pageSize}}
		if pageToken != "" {
			params = append(params, []string{"pageToken", pageToken})
		}

		res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns), params)
		if err != nil {
			return nil, err
		}
		for _, table := range res.Identifiers {
			names = append(names, table.Name)
		}

		if res.NextPageToken == "" {
			break
		}
		pageToken = res.NextPageToken
	}

	return names, nil
}

func (c *Catalog) GetTable(ctx context.Context, ns string, name string) (*Table, error) {
	type resp struct {
		Config           tableConfig     `json:"config"`
		MetadataLocation string          `json:"metadata-location"`
		RawMeta          json.RawMessage `json:"metadata"`
	}

	res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables/%s", ns, name))
	if err != nil {
		return nil, err
	}

	meta, err := table.ParseMetadataBytes(res.RawMeta)
	if err != nil {
		return nil, err
	}

	return &Table{
		Config:           res.Config,
		MetadataLocation: res.MetadataLocation,
		Metadata:         meta,
	}, nil
}

func (c *Catalog) CreateTable(ctx context.Context, ns string, name string, sch *iceberg.Schema, sortFieldIDs []int, location *string) error {
	type sortField struct {
		SourceID  int    `json:"source-id"`
		Transform string `json:"transform"`  // always "identity"
		Direction string `json:"direction"`  // always "asc"
		NullOrder string `json:"null-order"` // always "nulls-first"
	}

	type sortOrder struct {
		OrderID int         `json:"order-id"` // always 1
		Fields  []sortField `json:"fields"`
	}

	type createTableRequest struct {
		Name        string          `json:"name"`
		Schema      *iceberg.Schema `json:"schema"`
		Location    *string         `json:"location,omitempty"`
		WriteOrder  *sortOrder      `json:"write-order,omitempty"`
		StageCreate bool            `json:"stage-create"` // always false, required to exist & be false for s3tables catalogs
	}

	req := createTableRequest{
		Name:        name,
		Schema:      sch,
		Location:    location,
		StageCreate: false,
	}

	if len(sortFieldIDs) > 0 {
		var fields []sortField
		for _, id := range sortFieldIDs {
			fields = append(fields, sortField{
				SourceID:  id,
				Transform: "identity",
				Direction: "asc",
				NullOrder: "nulls-first",
			})
		}

		req.WriteOrder = &sortOrder{
			OrderID: 1,
			Fields:  fields,
		}
	}

	if err := doPost(ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns), req); err != nil {
		return err
	}

	return nil
}

func (c *Catalog) DeleteTable(ctx context.Context, ns string, name string) error {
	req := c.baseReq(ctx)
	if c.isS3tables {
		// This must be set if using s3tables. For glue, it must _not_ be set.
		// For other REST catalogs (Snowflake open catalog, for example) it
		// seems to generally cause permissions errors.
		req = req.SetQueryParam("purgeRequested", "true")
	}
	if got, err := req.Delete(fmt.Sprintf("/namespaces/%s/tables/%s", ns, name)); err != nil {
		return fmt.Errorf("failed to delete table %s.%s: %w", ns, name, err)
	} else if !got.IsSuccess() {
		return fmt.Errorf("failed to delete table %s.%s: %s: %s", ns, name, got.Status(), got.String())
	}

	return nil
}

func (c *Catalog) UpdateTable(ctx context.Context, ns string, name string, reqs []TableRequirement, upds []TableUpdate) error {
	type tableUpdateRequest struct {
		Identifier struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifier"`
		Requirements []TableRequirement `json:"requirements"`
		Updates      []TableUpdate      `json:"updates"`
	}

	req := tableUpdateRequest{
		Requirements: reqs,
		Updates:      upds,
	}
	req.Identifier.Namespace = []string{ns}
	req.Identifier.Name = name

	if err := doPost(ctx, c, fmt.Sprintf("/namespaces/%s/tables/%s", ns, name), req); err != nil {
		return err
	}

	return nil
}

type errorResponse struct {
	Error struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func (err *errorResponse) String() string {
	return fmt.Sprintf("(Code: %d, Message: %s, Type: %s)", err.Error.Code, err.Error.Message, err.Error.Type)
}

func (c *Catalog) baseReq(ctx context.Context) *resty.Request {
	return c.
		rHttp.
		NewRequest().
		WithContext(ctx).
		SetContentType("application/json").
		SetHeader("X-Iceberg-Access-Delegation", "vended-credentials").
		SetError(&errorResponse{})
}

func doGet[T any](ctx context.Context, client *Catalog, path string, params ...[][]string) (*T, error) {
	var res T

	req := client.baseReq(ctx)
	if len(params) > 0 {
		for _, param := range params[0] {
			req.SetQueryParam(param[0], param[1])
		}
	}

	if got, err := req.SetResult(&res).Get(path); err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", path, err)
	} else if !got.IsSuccess() {
		if e := got.Error().(*errorResponse); e.Error.Type == "IcebergTableNotFoundException" {
			return nil, ErrTableNotFound
		} else {
			return nil, fmt.Errorf("failed to GET %s: %s %s", got.Request.URL, got.Status(), e)
		}
	}

	return &res, nil
}

func doPost(ctx context.Context, client *Catalog, path string, body any) error {
	if got, err := client.baseReq(ctx).SetBody(body).Post(path); err != nil {
		return fmt.Errorf("failed to post %s: %w", path, err)
	} else if !got.IsSuccess() {
		return fmt.Errorf("failed to POST %s: %s %s", got.Request.URL, got.Status(), got.Error().(*errorResponse))
	}

	return nil
}

// from https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/signer/v4#Signer.SignHTTP
const emptyStringHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type sigv4Transport struct {
	delegate    http.RoundTripper
	signer      *v4.Signer
	region      string
	creds       aws.Credentials
	signingName string
}

func (t *sigv4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := sha256.New()

	var hsh string
	if req.Body != nil {
		if rdr, err := req.GetBody(); err != nil {
			return nil, err
		} else if _, err = io.Copy(h, rdr); err != nil {
			return nil, err
		} else {
			hsh = hex.EncodeToString(h.Sum(nil))
		}
	} else {
		hsh = emptyStringHash
	}

	if err := t.signer.SignHTTP(req.Context(), t.creds, req, hsh, t.signingName, t.region, time.Now()); err != nil {
		return nil, err
	}

	return t.delegate.RoundTrip(req)
}
