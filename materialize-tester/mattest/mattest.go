package mattest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	fm "github.com/estuary/flow/go/materialize"
	"github.com/estuary/flow/go/materialize/driver/image"
	"github.com/estuary/protocols/catalog"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	network = "host"
)

type Config struct {
	Test     string `long:"test" default:"basic" description:"Which test to run"`
	Image    string `long:"image" default:"" description:"The connector image to run"`
	Config   string `long:"config" default:"" description:"Single line yaml config or file with connector configuration"`
	Resource string `long:"resource" default:"" description:"Single line yaml config or file with resource configuration"`
	Network  string `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
}

type MaterializeTester interface {
	GenerateTestCatalog() (*TestCatalog, error)
	RunTest(ctx context.Context, catalog *catalog.BuiltCatalog, client pm.DriverClient) error
}

func (c *Config) Run(ctx context.Context) error {

	matConfig, err := resolveYamlOrFile(c.Config)
	if err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}

	matResource, err := resolveYamlOrFile(c.Resource)
	if err != nil {
		return fmt.Errorf("parsing resource: %w", err)
	}

	var mt MaterializeTester
	switch c.Test {
	case "basic":
		mt = NewBasicTester(c.Image, matConfig, matResource)
	default:
		return fmt.Errorf("unknown test type: %s", c.Test)
	}

	// Generate the TestCatalog struct.
	testCatalog, err := mt.GenerateTestCatalog()
	if err != nil {
		return fmt.Errorf("generate test catalog: %s", c.Test)
	}

	// Build/Compile the TestCatalog into a Flow BuiltCatalog.
	builtCatalog, err := BuildCatalog(ctx, testCatalog, network)
	if err != nil {
		return fmt.Errorf("build catalog: %v", err)
	}

	client := fm.AdaptServerToClient(image.NewDriver(network))

	// Pass off the transactions stream to the tester
	if err = mt.RunTest(ctx, builtCatalog, client); err != nil {
		return fmt.Errorf("test failed: %v", err)
	}

	return nil

}

func SetupSingleMaterializationTransactions(ctx context.Context, catalog *catalog.BuiltCatalog, client pm.DriverClient) (pm.Driver_TransactionsClient, error) {

	// It's assumed we're only testing one materialization.
	if len(catalog.Materializations) != 1 {
		return nil, fmt.Errorf("this test only supports a single materialization. found %d", len(catalog.Materializations))
	}
	materialization := catalog.Materializations[0]

	// Make the spec request to setup the materialization.
	respSpec, err := client.Spec(ctx, &pm.SpecRequest{
		EndpointType:     pf.EndpointType_FLOW_SINK,
		EndpointSpecJson: materialization.EndpointSpecJson,
	})
	if err != nil {
		return nil, fmt.Errorf("spec error: %v", err)
	}
	log.Debugf("spec response: %#v", respSpec)

	// Build the bindings and send the Validate request.
	var bindings = make([]*pm.ValidateRequest_Binding, len(materialization.Bindings))
	for i, binding := range materialization.Bindings {
		bindings[i] = &pm.ValidateRequest_Binding{
			Collection:       binding.Collection,
			ResourceSpecJson: binding.ResourceSpecJson,
			FieldConfigJson:  binding.FieldSelection.FieldConfigJson,
		}
	}

	respValidate, err := client.Validate(ctx, &pm.ValidateRequest{
		Materialization:  materialization.Materialization,
		EndpointType:     materialization.EndpointType,
		EndpointSpecJson: materialization.EndpointSpecJson,
		Bindings:         bindings,
	})
	if err != nil {
		return nil, fmt.Errorf("validate error: %v", err)
	}
	log.Debugf("validate response: %#v", respValidate)

	for _, respBindings := range respValidate.Bindings {
		for field, constraint := range respBindings.Constraints {
			switch constraint.Type {
			case pm.Constraint_FIELD_FORBIDDEN, pm.Constraint_UNSATISFIABLE:
				log.Warnf("binding: %v field: %s is missing from materialization.", respBindings.ResourcePath, field)
				err = fmt.Errorf("binding: %v field: %s is missing from materialization", respBindings.ResourcePath, field)
			}
		}
	}
	if err != nil {
		return nil, err
	}

	// Apply the materialization.
	respApply, err := client.Apply(ctx, &pm.ApplyRequest{
		Materialization: &materialization,
		Version:         "1.0",
		DryRun:          false,
	})
	if err != nil {
		return nil, fmt.Errorf("apply error: %v", err)
	}
	log.Debugf("apply response: %#v", respApply)

	// Open the transactions connection
	stream, err := client.Transactions(ctx)
	if err != nil {
		return nil, fmt.Errorf("transactions error: %v", err)
	}

	return stream, err

}

func resolveYamlOrFile(in string) (ConfigMap, error) {

	var out = make(ConfigMap)
	if err := yaml.Unmarshal([]byte(in), &out); err == nil {
		return out, nil
	}

	fileData, err := os.ReadFile(in)
	if err != nil {
		return nil, fmt.Errorf("%s is not valid yaml or file", in)
	}

	if err := yaml.Unmarshal([]byte(fileData), &out); err != nil {
		return nil, fmt.Errorf("%s does not contain valid yaml", in)
	}

	return out, nil

}

func PrettyPrintJSON(v interface{}) {
	e := json.NewEncoder(os.Stderr)
	e.SetEscapeHTML(false)
	e.SetIndent("", "  ")
	e.Encode(v)
}
