module github.com/estuary/connectors

go 1.19

require (
	cloud.google.com/go/bigquery v1.32.0
	cloud.google.com/go/firestore v1.6.1
	cloud.google.com/go/pubsub v1.25.1
	cloud.google.com/go/storage v1.23.0
	firebase.google.com/go v3.13.0+incompatible
	github.com/alpacahq/alpaca-trade-api-go/v2 v2.8.0
	github.com/aws/aws-sdk-go v1.44.26
	github.com/aws/aws-sdk-go-v2 v1.17.5
	github.com/aws/aws-sdk-go-v2/config v1.18.15
	github.com/aws/aws-sdk-go-v2/credentials v1.13.15
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.33
	github.com/aws/aws-sdk-go-v2/service/s3 v1.30.5
	github.com/aws/smithy-go v1.13.5
	github.com/benbjohnson/clock v1.3.0
	github.com/bradleyjkemp/cupaloy v2.3.0+incompatible
	github.com/elastic/go-elasticsearch/v7 v7.17.1
	github.com/estuary/flow v0.1.9-0.20230327223815-afe6d29a1203
	github.com/evanphx/json-patch/v5 v5.6.0
	github.com/go-mysql-org/go-mysql v1.5.0
	github.com/gogo/protobuf v1.3.2
	github.com/google/uuid v1.3.0
	github.com/iancoleman/orderedmap v0.2.0
	github.com/invopop/jsonschema v0.5.0
	github.com/jackc/pgconn v1.12.1
	github.com/jackc/pglogrepl v0.0.0-20220516121607-70a00e46998b
	github.com/jackc/pgproto3/v2 v2.3.0
	github.com/jackc/pgtype v1.11.0
	github.com/jackc/pgx/v4 v4.16.1
	github.com/jessevdk/go-flags v1.5.0
	github.com/mattn/go-sqlite3 v2.0.3+incompatible
	github.com/microsoft/go-mssqldb v0.19.0
	github.com/mitchellh/mapstructure v1.4.1
	github.com/rockset/rockset-go-client v0.15.4
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed
	github.com/sirupsen/logrus v1.9.0
	github.com/snowflakedb/gosnowflake v1.6.19
	github.com/stretchr/testify v1.8.1
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20220527110425-ba4adb87a31b
	go.gazette.dev/core v0.89.1-0.20230106205712-a2d374cdd6be
	go.mongodb.org/mongo-driver v1.11.2
	golang.org/x/oauth2 v0.3.0
	golang.org/x/sync v0.1.0
	golang.org/x/time v0.0.0-20220609170525-579cf78fd858
	google.golang.org/api v0.93.0
	google.golang.org/genproto v0.0.0-20220822174746-9e6da59bd2fc
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
	vitess.io/vitess v0.13.1
)

require (
	cloud.google.com/go v0.104.0 // indirect
	cloud.google.com/go/compute v1.7.0 // indirect
	cloud.google.com/go/iam v0.3.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.4.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.1.2 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0 // indirect
	github.com/Azure/azure-storage-blob-go v0.15.0 // indirect
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/apache/arrow/go/v10 v10.0.1 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.29 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.30 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.24 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.12.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.14.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.18.5 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/gabriel-vasile/mimetype v1.4.1 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v2.0.8+incompatible // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.1.0 // indirect
	github.com/googleapis/gax-go/v2 v2.4.0 // indirect
	github.com/googleapis/go-type-adapters v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	github.com/jackc/puddle v1.2.1 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.15.11 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.6 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/minio/highwayhash v1.0.2 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/opentracing-contrib/go-grpc v0.0.0-20180928155321-4b5a12d3ff02 // indirect
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.16 // indirect
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3 // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.12.2 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.34.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/zerolog v1.28.0 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/spf13/afero v1.8.2 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tinylib/msgp v1.1.1 // indirect
	github.com/uber/jaeger-client-go v2.16.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.1 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	github.com/youmark/pkcs8 v0.0.0-20181117223130-1be2e3e5546d // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/tools v0.6.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/DataDog/dd-trace-go.v1 v1.17.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apimachinery v0.20.6 // indirect
	nhooyr.io/websocket v1.8.7 // indirect
)
