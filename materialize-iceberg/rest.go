package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
	"golang.org/x/oauth2/clientcredentials"
	"resty.dev/v3"
)

// TODO: In general these responses may need to handle pagination.

var _ catalog = &restCatalog{}

type restCatalog struct {
	http      *resty.Client
	warehouse string
}

func newRestCatalog(ctx context.Context, cfg restCatalogConfig) (*restCatalog, error) {
	baseURL, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %w", err)
	}
	baseURL = baseURL.JoinPath("v1")

	var scopes []string
	if cfg.Scope != "" {
		scopes = []string{cfg.Scope}
	}

	var http *resty.Client
	clientID, clientSecret, ok := strings.Cut(cfg.Credential, ":")
	if ok {
		clientCredCfg := &clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     baseURL.JoinPath("/oauth/tokens").String(),
			Scopes:       scopes,
		}
		http = resty.NewWithClient(clientCredCfg.Client(ctx))
	} else {
		http = resty.New().SetAuthToken(cfg.Credential)
	}

	rc := &restCatalog{
		http:      http.SetBaseURL(baseURL.String()),
		warehouse: cfg.Warehouse,
	}

	type configResponse struct {
		Overrides map[string]string `json:"overrides"`
	}

	catalogCfg, err := doGet[configResponse](ctx, rc, "/config")
	if err != nil {
		return nil, err
	}

	if catalogCfg.Overrides != nil {
		if prefix, ok := catalogCfg.Overrides["prefix"]; ok {
			rc.http = rc.http.SetBaseURL(baseURL.JoinPath(prefix).String())
		}
	}

	return rc, nil
}

func (c *restCatalog) listNamespaces(ctx context.Context) ([]string, error) {
	type resp struct {
		Namespaces [][]string `json:"namespaces"`
	}

	res, err := doGet[resp](ctx, c, "/namespaces")
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(res.Namespaces))
	for _, ns := range res.Namespaces {
		out = append(out, ns[0])
	}

	return out, nil
}

func (c *restCatalog) createNamespace(ctx context.Context, ns string) error {
	if err := doPost(ctx, c, "/namespaces", map[string][]string{
		"namespace": {ns},
	}); err != nil {
		return err
	}

	return nil
}

func (c *restCatalog) listTables(ctx context.Context, ns string) ([]string, error) {
	type resp struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}

	res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns))
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(res.Identifiers))
	for _, table := range res.Identifiers {
		out = append(out, table.Name)
	}

	return out, nil
}

func (c *restCatalog) tableMetadata(ctx context.Context, ns string, name string) (*tableMetadata, error) {
	type resp struct {
		Metadata tableMetadata `json:"metadata"`
	}

	res, err := doGet[resp](ctx, c, fmt.Sprintf("/namespaces/%s/tables/%s", ns, name))
	if err != nil {
		return nil, err
	}

	return &res.Metadata, nil
}

func (c *restCatalog) createTable(ctx context.Context, ns string, name string, sch *iceberg.Schema) error {
	type createTableRequest struct {
		Name   string          `json:"name"`
		Schema *iceberg.Schema `json:"schema"`
		// TODO(whb): Might need to provide a location for some catalogs? It is
		// needed for AWS Glue.
		// TODO(whb): Consider partition-spec, write-order, and properties as
		// additional inputs.
	}

	if err := doPost(ctx, c, fmt.Sprintf("/namespaces/%s/tables", ns), createTableRequest{
		Name:   name,
		Schema: sch,
	}); err != nil {
		return err
	}

	return nil
}

func (c *restCatalog) deleteTable(ctx context.Context, ns string, name string) error {
	if got, err := c.baseReq(ctx).Delete(fmt.Sprintf("/namespaces/%s/tables/%s", ns, name)); err != nil {
		return fmt.Errorf("failed to delete table %s.%s: %w", ns, name, err)
	} else if !got.IsSuccess() {
		return fmt.Errorf("failed to delete table %s.%s: %s: %s", ns, name, got.Status(), got.String())
	}

	return nil
}

func (c *restCatalog) updateTable(ctx context.Context, ns string, name string, reqs []tableRequirement, upds []tableUpdate) error {
	type tableUpdateRequest struct {
		Identifier struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifier"`
		Requirements []tableRequirement `json:"requirements"`
		Updates      []tableUpdate      `json:"updates"`
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

func (c *restCatalog) baseReq(ctx context.Context) *resty.Request {
	return c.
		http.
		NewRequest().
		WithContext(ctx).
		SetContentType("application/json").
		SetQueryParam("warehouse", c.warehouse)
}

func doGet[T any](ctx context.Context, client *restCatalog, path string) (*T, error) {
	var res T

	if got, err := client.baseReq(ctx).SetResult(&res).Get(path); err != nil {
		return nil, fmt.Errorf("failed to get %s: %w", path, err)
	} else if !got.IsSuccess() {
		return nil, fmt.Errorf("failed to get %s: %s: %s", path, got.Status(), got.String())
	}

	return &res, nil
}

func doPost(ctx context.Context, client *restCatalog, path string, body any) error {
	if got, err := client.baseReq(ctx).SetBody(body).Post(path); err != nil {
		return fmt.Errorf("failed to post %s: %w", path, err)
	} else if !got.IsSuccess() {
		return fmt.Errorf("failed to post %s: %s: %s", path, got.Status(), got.String())
	}

	return nil
}
