package main

// import (
// 	"context"
// 	"crypto/sha256"
// 	"encoding/hex"
// 	"fmt"
// 	"io"
// 	"net/http"
// 	"net/url"
// 	"strings"
// 	"time"

// 	"github.com/apache/iceberg-go"
// 	"github.com/aws/aws-sdk-go-v2/aws"
// 	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
// 	"golang.org/x/oauth2/clientcredentials"
// 	"resty.dev/v3"
// )

// var (
// 	// errTableNotFound may be returned when attempting to get metadata for a
// 	// non-Iceberg table present in a Glue catalog.
// 	errTableNotFound = fmt.Errorf("table not found")
// )

// type tableMetadata struct {
// 	FormatVersion   int              `json:"format-version"`
// 	Location        string           `json:"location"`
// 	CurrentSchemaID int              `json:"current-schema-id"`
// 	Schemas         []iceberg.Schema `json:"schemas"`
// }

// func (m *tableMetadata) currentSchema() *iceberg.Schema {
// 	for idx := range m.Schemas {
// 		sc := &m.Schemas[idx] // avoid copying the locks used in iceberg.Schema
// 		if sc.ID == m.CurrentSchemaID {
// 			return sc
// 		}
// 	}
// 	panic("not reached")
// }

// // tableRequirement is a condition that must hold when submitting a request to a
// // catalog. Right now there's only assertCurrentSchemaID but it is structured to
// // allow for adding more in the future, which may be particularly useful for
// // performing atomic table appends of data files if we ever implement that.
// type tableRequirement interface {
// 	isTableRequirement()
// }

// type baseRequirement struct {
// 	Type string `json:"type"`
// }

// type assertCurrentSchemaIdReq struct {
// 	baseRequirement
// 	CurrentSchemaID int `json:"current-schema-id"`
// }

// func (assertCurrentSchemaIdReq) isTableRequirement() {}

// func assertCurrentSchemaID(id int) tableRequirement {
// 	return &assertCurrentSchemaIdReq{
// 		baseRequirement: baseRequirement{Type: "assert-current-schema-id"},
// 		CurrentSchemaID: id,
// 	}
// }

// // tableUpdate is an update to a table. As above, this is currently fairly
// // limited to adding a new schema to a table and setting the current schema ID -
// // both of which should typically be done to accomplish a DDL change.
// type tableUpdate interface {
// 	isTableUpdate()
// }

// type baseUpdate struct {
// 	Action string `json:"action"`
// }

// type addSchemaUpdateReq struct {
// 	baseUpdate
// 	Schema *iceberg.Schema `json:"schema"`
// }

// func (addSchemaUpdateReq) isTableUpdate() {}

// func addSchemaUpdate(schema *iceberg.Schema) tableUpdate {
// 	return &addSchemaUpdateReq{
// 		baseUpdate: baseUpdate{Action: "add-schema"},
// 		Schema:     schema,
// 	}
// }

// type setCurrentSchemaUpdateReq struct {
// 	baseUpdate
// 	SchemaID int `json:"schema-id"`
// }

// func (setCurrentSchemaUpdateReq) isTableUpdate() {}

// func setCurrentSchemaUpdate(id int) tableUpdate {
// 	return &setCurrentSchemaUpdateReq{
// 		baseUpdate: baseUpdate{Action: "set-current-schema"},
// 		SchemaID:   id,
// 	}
// }

// // TODO: In general these responses may need to handle pagination.

// type catalog struct {
// 	rHttp    *resty.Client
// 	location string
// }

// func newCatalog(ctx context.Context, cfg config) (*catalog, error) {
// 	var (
// 		rHttp     *resty.Client
// 		baseURL   *url.URL
// 		warehouse = cfg.Warehouse
// 		location  = cfg.Location
// 		err       error
// 	)

// 	if baseURL, err = url.Parse(cfg.URL); err != nil {
// 		return nil, fmt.Errorf("failed to parse base url: %w", err)
// 	}
// 	baseURL = baseURL.JoinPath("v1")

// 	switch cfg.CatalogAuthentication.CatalogAuthType {
// 	case catalogAuthTypeClientCredential:
// 		var scopes []string
// 		if cfg.CatalogAuthentication.Scope != "" {
// 			scopes = []string{cfg.CatalogAuthentication.Scope}
// 		}

// 		if clientID, clientSecret, ok := strings.Cut(cfg.CatalogAuthentication.Credential, ":"); ok {
// 			clientCredCfg := &clientcredentials.Config{
// 				ClientID:     clientID,
// 				ClientSecret: clientSecret,
// 				TokenURL:     baseURL.JoinPath("/oauth/tokens").String(),
// 				Scopes:       scopes,
// 			}
// 			rHttp = resty.NewWithClient(clientCredCfg.Client(ctx))
// 		} else {
// 			rHttp = resty.New().SetAuthToken(cfg.CatalogAuthentication.Credential)
// 		}
// 	case catalogAuthTypeSigV4:
// 		rHttp = resty.New()
// 		rHttp = rHttp.SetTransport(&sigv4Transport{
// 			delegate: rHttp.Transport(),
// 			signer:   v4.NewSigner(),
// 			region:   cfg.CatalogAuthentication.Region,
// 			creds: aws.Credentials{
// 				AccessKeyID:     cfg.CatalogAuthentication.AWSAccessKeyID,
// 				SecretAccessKey: cfg.CatalogAuthentication.AWSSecretAccessKey,
// 			},
// 		})
// 	default:
// 		return nil, fmt.Errorf("unknown catalog authentication type: %s", cfg.CatalogAuthentication.CatalogAuthType)
// 	}

// 	rc := &catalog{
// 		rHttp:    rHttp.SetBaseURL(baseURL.String()),
// 		location: location,
// 	}

// 	type configResponse struct {
// 		Overrides map[string]string `json:"overrides"`
// 	}

// 	catalogCfg, err := doGet[configResponse](ctx, rc, "/config", [][]string{{"warehouse", warehouse}})
// 	if err != nil {
// 		return nil, err
// 	}

// 	if catalogCfg.Overrides != nil {
// 		if prefix, ok := catalogCfg.Overrides["prefix"]; ok {
// 			rc.rHttp = rc.rHttp.SetBaseURL(baseURL.JoinPath(prefix).String())
// 		}
// 	}

// 	return rc, nil
// }

// func (c *catalog) listNamespaces(ctx context.Context) ([]string, error) {
// 	type resp struct {
// 		Namespaces [][]string `json:"namespaces"`
// 	}

// 	res, err := doGet[resp](ctx, c, "/namespaces")
// 	if err != nil {
// 		return nil, err
// 	}

// 	out := make([]string, 0, len(res.Namespaces))
// 	for _, ns := range res.Namespaces {
// 		out = append(out, ns[0])
// 	}

// 	return out, nil
// }

// func (c *catalog) createNamespace(ctx context.Context, ns string) error {
// 	if err := doPost(ctx, c, "/namespaces", map[string][]string{
// 		"namespace": {ns},
// 	}); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (c *catalog) listTables(ctx context.Context, ns string) ([]string, error) {
// 	type resp struct {
// 		Identifiers []struct {
// 			Name string `json:"name"`
// 		} `json:"identifiers"`
// 	}

// 	res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns))
// 	if err != nil {
// 		return nil, err
// 	}

// 	out := make([]string, 0, len(res.Identifiers))
// 	for _, table := range res.Identifiers {
// 		out = append(out, table.Name)
// 	}

// 	return out, nil
// }

// func (c *catalog) tableMetadata(ctx context.Context, ns string, name string) (*tableMetadata, error) {
// 	type resp struct {
// 		Metadata tableMetadata `json:"metadata"`
// 	}

// 	res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables/%s", ns, name))
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &res.Metadata, nil
// }

// func (c *catalog) createTable(ctx context.Context, ns string, name string, sch *iceberg.Schema) error {
// 	type createTableRequest struct {
// 		Name     string          `json:"name"`
// 		Schema   *iceberg.Schema `json:"schema"`
// 		Location string          `json:"location,omitempty"`
// 		// TODO(whb): Consider partition-spec, write-order, and properties as
// 		// additional inputs.
// 	}

// 	if err := doPost(ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns), createTableRequest{
// 		Name:     name,
// 		Schema:   sch,
// 		Location: c.location,
// 	}); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (c *catalog) deleteTable(ctx context.Context, ns string, name string) error {
// 	if got, err := c.baseReq(ctx).Delete(fmt.Sprintf("/namespaces/%s/tables/%s", ns, name)); err != nil {
// 		return fmt.Errorf("failed to delete table %s.%s: %w", ns, name, err)
// 	} else if !got.IsSuccess() {
// 		return fmt.Errorf("failed to delete table %s.%s: %s: %s", ns, name, got.Status(), got.String())
// 	}

// 	return nil
// }

// func (c *catalog) updateTable(ctx context.Context, ns string, name string, reqs []tableRequirement, upds []tableUpdate) error {
// 	type tableUpdateRequest struct {
// 		Identifier struct {
// 			Namespace []string `json:"namespace"`
// 			Name      string   `json:"name"`
// 		} `json:"identifier"`
// 		Requirements []tableRequirement `json:"requirements"`
// 		Updates      []tableUpdate      `json:"updates"`
// 	}

// 	req := tableUpdateRequest{
// 		Requirements: reqs,
// 		Updates:      upds,
// 	}
// 	req.Identifier.Namespace = []string{ns}
// 	req.Identifier.Name = name

// 	if err := doPost(ctx, c, fmt.Sprintf("/namespaces/%s/tables/%s", ns, name), req); err != nil {
// 		return err
// 	}

// 	return nil
// }

// type errorResponse struct {
// 	Error struct {
// 		Code    int    `json:"code"`
// 		Message string `json:"message"`
// 		Type    string `json:"type"`
// 	} `json:"error"`
// }

// func (err *errorResponse) String() string {
// 	return fmt.Sprintf("(Code: %d, Message: %s, Type: %s)", err.Error.Code, err.Error.Message, err.Error.Type)
// }

// func (c *catalog) baseReq(ctx context.Context) *resty.Request {
// 	return c.
// 		rHttp.
// 		NewRequest().
// 		WithContext(ctx).
// 		SetContentType("application/json").
// 		SetError(&errorResponse{})
// }

// func doGet[T any](ctx context.Context, client *catalog, path string, params ...[][]string) (*T, error) {
// 	var res T

// 	req := client.baseReq(ctx)
// 	if len(params) > 0 {
// 		for _, param := range params[0] {
// 			req.SetQueryParam(param[0], param[1])
// 		}
// 	}

// 	if got, err := req.SetResult(&res).Get(path); err != nil {
// 		return nil, fmt.Errorf("failed to get %s: %w", path, err)
// 	} else if !got.IsSuccess() {
// 		if e := got.Error().(*errorResponse); e.Error.Type == "IcebergTableNotFoundException" {
// 			return nil, errTableNotFound
// 		} else {
// 			return nil, fmt.Errorf("failed to GET %s: %s %s", got.Request.URL, got.Status(), e)
// 		}
// 	}

// 	return &res, nil
// }

// func doPost(ctx context.Context, client *catalog, path string, body any) error {
// 	if got, err := client.baseReq(ctx).SetBody(body).Post(path); err != nil {
// 		return fmt.Errorf("failed to post %s: %w", path, err)
// 	} else if !got.IsSuccess() {
// 		return fmt.Errorf("failed to POST %s: %s %s", got.Request.URL, got.Status(), got.Error().(*errorResponse))
// 	}

// 	return nil
// }

// // from https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/signer/v4#Signer.SignHTTP
// const emptyStringHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// type sigv4Transport struct {
// 	delegate http.RoundTripper
// 	signer   *v4.Signer
// 	region   string
// 	creds    aws.Credentials
// }

// func (t *sigv4Transport) RoundTrip(req *http.Request) (*http.Response, error) {
// 	h := sha256.New()

// 	var hsh string
// 	if req.Body != nil {
// 		if rdr, err := req.GetBody(); err != nil {
// 			return nil, err
// 		} else if _, err = io.Copy(h, rdr); err != nil {
// 			return nil, err
// 		} else {
// 			hsh = hex.EncodeToString(h.Sum(nil))
// 		}
// 	} else {
// 		hsh = emptyStringHash
// 	}

// 	if err := t.signer.SignHTTP(req.Context(), t.creds, req, hsh, "glue", t.region, time.Now()); err != nil {
// 		return nil, err
// 	}

// 	return t.delegate.RoundTrip(req)
// }
