module github.com/estuary/connectors

go 1.16

require (
	cloud.google.com/go/storage v1.16.0
	github.com/aws/aws-sdk-go v1.40.29
	github.com/estuary/protocols v0.0.0-20210825031721-68a8bbd0c136
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/api v0.54.0
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20210726192503-178f10d4ba3d
