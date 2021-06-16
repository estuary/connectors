module github.com/estuary/connectors/source-kinesis

go 1.16

require (
	github.com/aws/aws-sdk-go v1.38.47
	github.com/estuary/connectors/go-types v0.0.0
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
)

replace github.com/estuary/connectors/go-types => ../go-types
