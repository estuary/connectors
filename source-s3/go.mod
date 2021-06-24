module github.com/estuary/connectors/source-s3

go 1.16

require (
	github.com/aws/aws-sdk-go v1.38.47
	github.com/estuary/connectors/go-types v0.0.0-20210616215509-62fbb9b27896
	github.com/sirupsen/logrus v1.8.1
)

replace github.com/estuary/connectors/go-types => ../go-types
