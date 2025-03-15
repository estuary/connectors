package main

import (
	awsHTTP "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

func main() {
	awsHTTP.DefaultHTTPTransportMaxIdleConns = 100        // From 100.
	awsHTTP.DefaultHTTPTransportMaxIdleConnsPerHost = 100 // From 10.
	boilerplate.RunMain(driver{})
}
