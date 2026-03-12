package main

import (
	_ "embed"
	"fmt"
	"regexp"
	"strings"
)

const productName = "Estuary Technologies Flow"
const productFeature = "materialize-databricks"

//go:embed VERSION
var rawVersion string

func productVersion() string {
	return regexp.MustCompile(`^v(\d+)\s*$`).FindStringSubmatch(rawVersion)[1] + ".0.0"
}

// DSN parameter for "partner identification".
var productParameterDSN = struct {
	key, value string
}{
	// This DSN key is an "optional parameter" for the Golang driver.
	// https://docs.databricks.com/aws/en/dev-tools/go-sql-driver#optional-parameters
	key: "userAgentEntry",
	// The format for this value is found in the JDBC driver docs.
	// https://docs.databricks.com/aws/en/integrations/jdbc-oss/properties#-other-feature-properties
	// "This value is in the following format: [ProductName]/[ProductVersion] [Comment]"
	value: fmt.Sprintf("%s/%s %s", productName, productVersion(), productFeature),
}

// Values for global product identification.
// github.com/databricks/databricks-sdk-go/useragent
var productGlobalDescription = struct {
	name, version string
}{
	name:    strings.ReplaceAll(fmt.Sprintf("%s_%s", productName, productFeature), " ", "-"),
	version: productVersion(),
}
