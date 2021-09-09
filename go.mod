module github.com/estuary/connectors

go 1.16

require (
	cloud.google.com/go/storage v1.16.0
	github.com/alecthomas/jsonschema v0.0.0-20210818095345-1014919a589c
	github.com/aws/aws-sdk-go v1.40.29
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/estuary/protocols v0.0.0-20210909024935-863b8c43e714
	github.com/gogo/protobuf v1.3.2
	github.com/google/uuid v1.3.0
	github.com/jackc/pgconn v1.8.0
	github.com/jackc/pgx/v4 v4.10.1
	github.com/jessevdk/go-flags v1.5.0
	github.com/sirupsen/logrus v1.8.1
	github.com/snowflakedb/gosnowflake v1.6.1
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/api v0.54.0
	google.golang.org/grpc v1.40.0
)

replace go.gazette.dev/core => github.com/jgraettinger/gazette v0.0.0-20210726192503-178f10d4ba3d
