package matsim

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/estuary/connectors/flowsim/testcat"
	"github.com/estuary/connectors/flowsim/testdata"
	"github.com/estuary/flow/go/materialize/driver/image"
	pm "github.com/estuary/protocols/materialize"
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

// Config is the main test config that is applicable to all tests.
type Config struct {
	Image    string    `long:"image" default:"" description:"The connector image to run"`
	Network  string    `long:"network" default:"host" description:"The Docker network that connector containers are given access to."`
	Config   string    `long:"config" default:"" description:"Single line yaml config or file with connector configuration"`
	Resource string    `long:"resource" default:"" description:"Single line yaml config or file with resource configuration"`
	Log      LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`

	ctx          context.Context
	driverServer pm.DriverServer
	newTestData  func() testdata.TestData // Used to generate test data.

	mConfig   testcat.ConfigMap // Materialization Config.
	mResource testcat.ConfigMap // Materialization Resource Config.
}

// LogConfig configures handling of application log events.
type LogConfig struct {
	Level  string `long:"level" env:"LEVEL" default:"info" choice:"trace" choice:"debug" choice:"info" choice:"warn" choice:"error" choice:"fatal" description:"Logging level"`
	Format string `long:"format" env:"FORMAT" default:"text" choice:"json" choice:"text" choice:"color" description:"Logging output format"`
}

// Run starts this testing framework. DriverServer is an optional argument where you can pass in your driver directly and run
// this framework against it. If not specified it assumes it's driving a docker image based connector.
// To test your connector directly, just import this package and call Run(context.Background(), pm.DriverServer, func() testdata.TestData)
// with your connector that implements the pm.DriverServer interface. If the newTestData function is nil it will
// use the default test data generator.
func Run(ctx context.Context, driverServer pm.DriverServer, newTestData func() testdata.TestData) {
	var parser = flags.NewParser(nil, flags.Default)
	ctx, _ = signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)

	AddCommand(ctx, parser.Command, driverServer, newTestData)

	var _, err = parser.Parse()
	if err != nil {
		log.Fatal(err)
	}
}

// AddCommand sets up the matsim commands on the provided parser command. This allows it to be included
// from the outer flowsim command or called directly from a materialization.
func AddCommand(ctx context.Context, command *flags.Command, driverServer pm.DriverServer, newTestData func() testdata.TestData) {
	var config = Config{
		ctx:          ctx,
		driverServer: driverServer,
		newTestData:  newTestData,
	}

	command.AddCommand("basic", "Basic test",
		"Basic functionality test", &BasicConfig{Config: config})

	command.AddCommand("delta", "Delta test",
		"Delta functionality test", &DeltaConfig{Config: config})

	command.AddCommand("fence", "Fence test",
		"Fence functionality test", &FenceConfig{Config: config})

}

// ParseConfig sets up the base Config struct, logging, parses the config and resource config options.
// It sets up the driverServer with the flowSink adapter if none has been supplied.
// It sets up the testDataGenerator if none has been configured.
func (c *Config) ParseConfig() error {

	// Setup logging
	if c.Log.Format == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	} else if c.Log.Format == "text" {
		log.SetFormatter(&log.TextFormatter{})
	} else if c.Log.Format == "color" {
		log.SetFormatter(&log.TextFormatter{ForceColors: true})
	}

	if lvl, err := log.ParseLevel(c.Log.Level); err != nil {
		log.WithField("err", err).Fatal("unrecognized log level")
	} else {
		log.SetLevel(lvl)
	}

	// The mConfig and mResource are the connector config and resource configurations
	// They can either be in single line yaml format or a path to a file.
	var err error
	if c.mConfig, err = resolveYamlOrFile(c.Config); err != nil {
		return fmt.Errorf("parsing config: %w", err)
	}
	if c.mResource, err = resolveYamlOrFile(c.Resource); err != nil {
		return fmt.Errorf("parsing resource: %w", err)
	}

	if c.driverServer == nil {
		// If driverServer is nil it means we were invoked as a command and should initialize
		// the FlowSink driver and expect an image.
		c.driverServer = image.NewDriver(c.Network)
	} else {
		// If the driverServer is already initialized, it means we are being called from a
		// connector. We need to wrap it in a FlowSinkAdapter to ensure configuration for the
		// adapter is unwrapped.
		c.driverServer = NewFlowSinkAdapter(c.driverServer)
		c.Image = "not-used" // With the adapter, we will not be calling an actual image.
	}

	// If no testDataGenerator specified, use the testdata.BasicData generator.
	if c.newTestData == nil {
		c.newTestData = func() testdata.TestData {
			var data testdata.Basic
			gofakeit.Struct(&data)
			return &data
		}
	}

	return nil

}

// NewTestCatalog returns a simple catalog with a single collection using the schema of your TestData
// and a single materialization setup to use FlowSync with your materialization.
func (c *Config) NewTestCatalog() (*testcat.TestCatalog, error) {

	var collection, err = testcat.BuildCollection(c.newTestData())
	if err != nil {
		return nil, err
	}

	return &testcat.TestCatalog{
		Collections: map[string]testcat.TestCollection{
			// Generate a collection using our test data type.
			"coltest": collection,
		},
		Materializations: map[string]testcat.TestMaterialization{
			"mattest": {
				Endpoint: testcat.TestEndpointFlowSync{
					Image:  c.Image,
					Config: c.mConfig,
				},
				Bindings: []testcat.Binding{
					{
						Source:   "coltest",
						Resource: c.mResource,
					},
				},
			},
		},
	}, nil
}

// resolveYamlOrFile takes a string and tries to parse it as yaml or if it can't assumes it's a yaml file.
func resolveYamlOrFile(in string) (testcat.ConfigMap, error) {

	var out = make(testcat.ConfigMap)
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
