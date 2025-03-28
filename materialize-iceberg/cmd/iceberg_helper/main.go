package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/estuary/connectors/materialize-iceberg/catalog"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var catalogUrl = flag.String("catalog-url", "", "catalog url to connect to")
var warehouse = flag.String("warehouse", "", "warehouse")

var force = flag.Bool("force", false, "bypass the confirmation check before purging a table")

var clientCredential = flag.String("client-credential", "", "credential for client credential authentication")
var scope = flag.String("scope", "", "scope for client credential authentication")
var oauth2ServerUri = flag.String("oauth2-server-uri", "v1/oauth/tokens", "OAuth 2.0 server URI for requesting access tokens when using OAuth client credentials")

var awsAccessKeyID = flag.String("aws-access-key-id", "", "AWS access key ID for using SigV4 authentication")
var awsSecretAccessKey = flag.String("aws-secret-access-key", "", "AWS secret access key for using SigV4 authentication")
var region = flag.String("region", "", "AWS region for using SigV4 authentication")
var signingName = flag.String("signing-name", "", "Signing name for using SigV4 authentication")

func getCatalog(ctx context.Context) (*catalog.Catalog, error) {
	usingClientCredentialAuth := *clientCredential != ""
	usingSigV4Auth := *awsAccessKeyID != ""

	if *catalogUrl == "" {
		return nil, errors.New("missing required flag for catalog url")
	} else if *warehouse == "" {
		return nil, errors.New("missing required flag for warehouse")
	} else if usingClientCredentialAuth && usingSigV4Auth {
		return nil, errors.New("cannot use both client credential and SigV4 authentication")
	} else if !usingClientCredentialAuth && !usingSigV4Auth {
		return nil, errors.New("must use either client credential or SigV4 authentication")
	} else if usingSigV4Auth {
		if *signingName == "" || *awsAccessKeyID == "" || *awsSecretAccessKey == "" || *region == "" {
			return nil, errors.New("missing required flags for SigV4 authentication")
		}
	}

	var opts []catalog.CatalogOption
	if usingClientCredentialAuth {
		opts = append(opts, catalog.WithClientCredential(*clientCredential, *oauth2ServerUri, scope))
	} else if usingSigV4Auth {
		opts = append(opts, catalog.WithSigV4(*signingName, *awsAccessKeyID, *awsSecretAccessKey, *region))
	}

	cat, err := catalog.New(ctx, *catalogUrl, *warehouse, opts...)
	if err != nil {
		return nil, err
	}

	return cat, nil
}

var scriptDescription = `
This tool allows you to list namespaces and tables, describe table metadata, read table contents, and delete tables. 
It supports both client credential and AWS SigV4 authentication methods.

Each command requires a fully qualified table name (namespace.table) where applicable.
The tool automatically handles S3 bucket region detection and AWS credentials management.

Example commands:
  list namespaces      - List all available namespaces
  list tables          - List all tables across all namespaces
  describe myns.mytbl  - Show schema and metadata for a specific table
  read myns.mytbl      - Output the table contents as JSON
  purge myns.mytbl     - Delete a table record from the catalog and purge its data and metadata files

Required Flags:
  --catalog-url string              REST catalog URL to connect to
  --warehouse string                Warehouse name

Optional Flags:
  --force                           Bypass the confirmation check before purging a table
  --oauth2-server-uri               OAuth 2.0 server URI for requesting access tokens when using OAuth client credentials


Authentication (use one of the following methods):
  
  Client Credential Auth:
  --client-credential string        Credential for client credential authentication
  --scope string                    Scope for client credential authentication (optional)
  
  AWS SigV4 Auth:
  --aws-access-key-id string        AWS access key ID
  --aws-secret-access-key string    AWS secret access key
  --region string                   AWS region
`

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s\n", scriptDescription)
	}
	flag.Parse()
	args := flag.Args()

	if len(flag.Args()) < 2 {
		fmt.Println("must provide at least 2 positional arguments")
		os.Exit(1)
	}

	ctx := context.Background()

	cat, err := getCatalog(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var output string
	switch args[0] {
	case "list":
		output, err = doList(ctx, cat, args[1:])
		if err != nil {
			log.Fatal(err)
		}
	case "describe":
		output, err = doDescribeTable(ctx, cat, args[1:])
		if err != nil {
			log.Fatal(err)
		}
	case "read":
		output, err = doReadTable(ctx, cat, args[1:])
		if err != nil {
			log.Fatal(err)
		}
	case "purge":
		output, err = doPurgeTable(ctx, cat, args[1:])
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command %s", args[0])
	}

	fmt.Println(output)
}

func doList(ctx context.Context, cat *catalog.Catalog, args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("must provide positional argument of either 'namespaces' or 'tables' to list")
	}

	var out strings.Builder
	if args[0] == "namespaces" {
		ns, err := cat.ListNamespaces(ctx)
		if err != nil {
			return "", err
		}

		for _, n := range ns {
			out.WriteString(fmt.Sprintf("%s\n", n))
		}
	} else if args[0] == "tables" {
		ns, err := cat.ListNamespaces(ctx)
		if err != nil {
			return "", err
		}

		for _, n := range ns {
			out.WriteString(fmt.Sprintf("Tables in namespace %s:\n", n))

			tables, err := cat.ListTables(ctx, n)
			if err != nil {
				return "", err
			}

			count := 1
			for _, t := range tables {
				out.WriteString(fmt.Sprintf("%d: %s.%s\n", count, n, t))
				count++
			}
		}
	} else {
		return "", fmt.Errorf("invalid argument %q for list, must be either 'namespaces' or 'tables'", args[1])
	}

	return out.String(), nil
}

func doDescribeTable(ctx context.Context, cat *catalog.Catalog, args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("must provide positional argument for the table to describe as 'namespace.table'")
	}

	ns, name, err := tableParts(args[0])
	if err != nil {
		return "", err
	}

	table, err := cat.GetTable(ctx, ns, name)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	out.WriteString(fmt.Sprintf("Table: %s.%s\n", ns, name))
	out.WriteString(fmt.Sprintf("Location: %s\n", table.Metadata.Location()))
	out.WriteString(fmt.Sprintf("Metadata Location: %s\n", table.MetadataLocation))
	out.WriteString(("Identifier Field IDs: "))
	json.NewEncoder(&out).Encode(table.Metadata.CurrentSchema().IdentifierFieldIDs)
	out.WriteString(fmt.Sprintf("Sort Order: %s\n", table.Metadata.SortOrder().String()))
	json.NewEncoder(&out).Encode(table.Metadata.Properties())
	out.WriteString(table.Metadata.CurrentSchema().String())

	return out.String(), nil
}

func doReadTable(ctx context.Context, cat *catalog.Catalog, args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("must provide positional argument for the table to read as 'namespace.table'")
	}

	ns, name, err := tableParts(args[0])
	if err != nil {
		return "", err
	}

	table, err := cat.GetTable(ctx, ns, name)
	if err != nil {
		return "", err
	}

	region, creds, err := resolveCredentials(table)
	if err != nil {
		return "", err
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return "", err
	}
	defer db.Close()

	initQueries := []string{
		"INSTALL iceberg;",
		"LOAD iceberg;",
		"INSTALL httpfs;",
		"LOAD httpfs;",
		fmt.Sprintf("SET s3_region = '%s';", region),
		fmt.Sprintf("SET s3_access_key_id='%s';", creds.AccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key='%s';", creds.SecretAccessKey),
	}

	if creds.SessionToken != "" {
		initQueries = append(initQueries, fmt.Sprintf("SET s3_session_token='%s';", creds.SessionToken))
	}

	for _, q := range initQueries {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return "", err
		}
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM iceberg_scan('%s');", table.MetadataLocation))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)

	for rows.Next() {
		row := make(map[string]any)
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))

		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err = rows.Scan(valuePtrs...); err != nil {
			return "", err
		}
		for i := range cols {
			if v, ok := values[i].(float64); ok {
				if math.IsNaN(v) {
					values[i] = "NaN"
				} else if math.IsInf(v, 0) {
					values[i] = "Infinity"
				} else if math.IsInf(v, -1) {
					values[i] = "-Infinity"
				}
			}

			row[cols[i]] = values[i]
		}
		if err := enc.Encode(row); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

func doPurgeTable(ctx context.Context, cat *catalog.Catalog, args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("must provide positional argument for the table to delete as 'namespace.table'")
	}

	ns, name, err := tableParts(args[0])
	if err != nil {
		return "", err
	}

	table, err := cat.GetTable(ctx, ns, name)
	if err != nil {
		return "", err
	}

	loc := table.Metadata.Location()

	if !*force {
		fmt.Printf("Are you sure you want to DELETE %q from the catalog and all of its files from %q?\n", args[0], loc)
		fmt.Println("Enter Y to continue, anything else to cancel:")
		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}

		if !strings.EqualFold(input, "Y\n") {
			return "purge table aborted", nil
		}
	}

	region, creds, err := resolveCredentials(table)
	if err != nil {
		return "", err
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken),
		),
		config.WithRegion(string(region)),
	)
	if err != nil {
		return "", err
	}

	s3client := s3.NewFromConfig(cfg)

	loc = strings.TrimPrefix(loc, "s3://")
	split := strings.SplitN(loc, "/", 2)
	bucket := split[0]
	prefix := split[1]

	for _, p := range []string{"data", "metadata"} {
		prefix := path.Join(prefix, p)
		if err := cleanPrefix(ctx, s3client, bucket, prefix); err != nil {
			return "", fmt.Errorf("failed to purge prefix %q: %w", prefix, err)
		}
	}

	if err := cat.DeleteTable(ctx, ns, name); err != nil {
		return "", err
	}

	return table.Metadata.Location(), nil
}

func cleanPrefix(ctx context.Context, s3client *s3.Client, bucket string, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(s3client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	count := 0
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to get next page: %w", err)
		}
		count += len(page.Contents)

		thisPage := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			thisPage = append(thisPage, types.ObjectIdentifier{Key: obj.Key})
		}

		if len(thisPage) == 0 {
			// No data files for this table, if the table has no data.
			continue
		}

		if _, err := s3client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: thisPage},
		}); err != nil {
			return err
		}
	}

	fmt.Printf("Deleted %d objects from %s/%s\n", count, bucket, prefix)

	return nil
}

func tableParts(fqn string) (ns string, name string, err error) {
	parts := strings.Split(fqn, ".")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid table FQN %q: must be of the form 'namespace.table'", fqn)
	} else if len(parts) > 2 {
		return "", "", fmt.Errorf("invalid table FQN %q: heirachical namespaces are currently not supported", fqn)
	}

	return parts[0], parts[1], nil
}

func resolveCredentials(table *catalog.Table) (types.BucketLocationConstraint, aws.Credentials, error) {
	metadataLocation := table.MetadataLocation
	creds := aws.Credentials{
		AccessKeyID:     table.Config.S3_AccessKeyId,
		SecretAccessKey: table.Config.S3_SecretAccessKey,
		SessionToken:    table.Config.S3_SessionToken,
		CanExpire:       !table.Config.ExpirationTime.IsZero(),
		Expires:         table.Config.ExpirationTime,
	}
	var awsRegion types.BucketLocationConstraint

	if creds.AccessKeyID == "" {
		creds.AccessKeyID = *awsAccessKeyID
		creds.SecretAccessKey = *awsSecretAccessKey
		awsRegion = types.BucketLocationConstraint(*region)
	} else {
		gotRegion, err := getBucketRegion(metadataLocation)
		if err != nil {
			return "", aws.Credentials{}, err
		}
		awsRegion = types.BucketLocationConstraint(gotRegion)
	}

	return awsRegion, creds, nil
}

func getBucketRegion(uri string) (string, error) {
	// It's apparently impossible to get the region of a bucket if you don't
	// already know it using the AWS Go SDKs, but this silly mechanism actually
	// seems to work 100% of the time.
	uri = strings.TrimPrefix(uri, "s3://")
	split := strings.SplitN(uri, "/", 2)
	bucket := split[0]

	resp, err := http.DefaultClient.Head(fmt.Sprintf("https://%s.s3.amazonaws.com", bucket))
	if resp != nil && resp.Header != nil {
		if bucketRegion := resp.Header.Get("x-amz-bucket-region"); bucketRegion != "" {
			return bucketRegion, nil
		}
	}
	if err != nil {
		return "", err
	}

	return "", fmt.Errorf("could not find region for bucket %s", bucket)
}
