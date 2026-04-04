package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

var (
	authURL        = MustParse("https://app.hubspot.com/oauth/authorize")
	accessTokenURL = MustParse("https://api.hubspot.com/oauth/v3/token")
	oauth2Provider = "materialize-hubspot"

	oauth2Scopes = []string{
		"oauth",
		"crm.objects.companies.read",
		"crm.objects.companies.write",
		"crm.objects.contacts.read",
		"crm.objects.contacts.write",
	}

	oauth2OptionalScopes = []string{}
)

func MustParse(rawURL string) *url.URL {
	uri, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return uri
}

func OAuth2Spec() *pf.OAuth2 {
	authParams := "" +
		"client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}" +
		"&scope=" + url.QueryEscape(strings.Join(oauth2Scopes, " ")) +
		"&optional_scope=" + url.QueryEscape(strings.Join(oauth2OptionalScopes, " ")) +
		"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}" +
		"&state={{#urlencode}}{{{ state }}}{{/urlencode}}"
	authURLTemplate := *authURL
	authURLTemplate.RawQuery = authParams

	accessParams := "" +
		"grant_type=authorization_code" +
		"&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}" +
		"&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}" +
		"&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}" +
		"&code={{#urlencode}}{{{ code }}}{{/urlencode}}"

	return &pf.OAuth2{
		Provider:        oauth2Provider,
		AccessTokenBody: accessParams,
		AuthUrlTemplate: authURLTemplate.String(),
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"content-type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token":           json.RawMessage(`"/refresh_token"`),
			"access_token":            json.RawMessage(`"/access_token"`),
			"access_token_expires_at": json.RawMessage(`"{{#now_plus}}{{ expires_in }}{{/now_plus}}"`),
		},
		AccessTokenUrlTemplate: "https://api.hubapi.com/oauth/v3/token",
	}
}

type Driver struct{}

var _ boilerplate.Connector = (*Driver)(nil)

func (*Driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("HubSpot", &Config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("HubSpot Objects", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-hubspot",
		Oauth2:                   OAuth2Spec(),
	}, nil
}

func (*Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (*Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (*Driver) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	be *m.BindingEvents,
) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	transactor, opened, options, err := boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
	log.WithFields(log.Fields{
		"transactor": transactor,
		"opened":     opened,
		"options":    options,
		"err":        err,
	}).Info("NewTransactor")
	return transactor, opened, options, err
}

type materialization struct {
	config *Config
}

var _ boilerplate.Materializer[*Config, FieldConfig, *Resource, mappedType] = (*materialization)(nil)

func (m *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

func (m *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	log.WithFields(log.Fields{
		"resourcePaths": resourcePaths,
		"is":            is,
	}).Info("PopulateInfoSchema")

	return nil
}

func (m *materialization) CheckPrerequisites(context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	err := m.config.Validate()
	if err != nil {
		errs.Err(err)
	}

	return errs
}

func (m *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fieldConfig FieldConfig) pm.Response_Validated_Constraint {
	log.WithFields(log.Fields{
		"projection":   p,
		"deltaUpdates": deltaUpdates,
		"fieldConfig":  fieldConfig,
	}).Info("NewConstraint")
	var constraint = pm.Response_Validated_Constraint{}

	constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
	constraint.Reason = "This field is able to be materialized"

	return constraint
}

func (m *materialization) MapType(
	p boilerplate.Projection,
	fieldConfig FieldConfig,
) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (m *materialization) Setup(context.Context, *boilerplate.InfoSchema) (string, error) {
	log.Info("Setup")
	return "", nil
}

func (m *materialization) CreateNamespace(ctx context.Context, name string) (string, error) {
	log.WithField("name", name).Info("CreateNamespace")
	return "", nil
}

func (m *materialization) CreateResource(ctx context.Context, binding boilerplate.MappedBinding[*Config, *Resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	log.WithField("keys", binding.Keys).Info("CreateResource")
	return "", nil, nil
}

func (m *materialization) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	log.WithField("path", path).Info("DeleteResource")
	return "", nil, nil
}

func (m *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[*Config, *Resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	log.Info("UpdateResource")
	return "", nil, nil
}

func (m *materialization) TruncateResource(context.Context, []string) (string, boilerplate.ActionApplyFn, error) {
	log.Info("TruncateResource")
	return "", nil, nil
}

func (m *materialization) MustRecreateResource(
	req *pm.Request_Apply,
	lastBinding,
	newBinding *pf.MaterializationSpec_Binding,
) (bool, error) {
	return false, nil
}

func (m *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[*Config, *Resource, mappedType],
	be *m.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	client, err := NewClient(m.config.Credentials)
	if err != nil {
		return nil, err
	}
	return &transactor{
		client: client,
		config: m.config,
	}, nil
}

func (m *materialization) Close(context.Context) {
}

func newMaterialization(
	ctx context.Context,
	materializationName string,
	config *Config,
	featureFlags map[string]bool,
) (boilerplate.Materializer[*Config, FieldConfig, *Resource, mappedType], error) {
	log.WithFields(log.Fields{
		"client_secret":           config.Credentials.ClientSecret,
		"client_id":               config.Credentials.ClientID,
		"refresh_token":           config.Credentials.RefreshToken,
		"access_token":            config.Credentials.AccessToken,
		"access_token_expires_at": config.Credentials.AccessTokenExpiresAt,
	}).Info("newMaterialization")
	return &materialization{
		config: config,
	}, nil
}

func main() {
	boilerplate.RunMain(new(Driver))
}
