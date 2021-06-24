package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/estuary/connectors/go-types/airbyte"
	log "github.com/sirupsen/logrus"
)

func discoverStreams(ctx context.Context, config *Config, client *s3.S3) ([]airbyte.Stream, error) {
	// Always return a "base stream" that represents a recursive import of everything in the given
	// bucket/prefix. We'll then do a listing of child prefixes within this location so that we can
	// also present those as options.
	var baseStreamName string
	if config.Prefix == "" {
		baseStreamName = formatStreamName(config.Bucket)
	} else {
		baseStreamName = formatStreamName(config.Bucket, config.Prefix)
	}
	var streams = []airbyte.Stream{
		{
			Name:               baseStreamName,
			JSONSchema:         json.RawMessage(`{"type":"object"}`),
			SupportedSyncModes: airbyte.AllSyncModes,
		},
	}

	var input = &s3.ListObjectsV2Input{
		Bucket:    &config.Bucket,
		Delimiter: aws.String(s3Delimiter),
		Prefix:    &config.Prefix,
	}
	var output, err = client.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	// return each common prefix as a stream. This is a bit arbitrary, but seems like a decent
	// starting point. We anticipate that buckets can have *many* objects and prefixes, and so we
	// don't bother with paginating the results, as we don't want to return thousands of streams
	// anyway.
	for _, prefix := range output.CommonPrefixes {
		streams = append(streams, airbyte.Stream{
			Name:               formatStreamName(config.Bucket, config.Prefix, *prefix.Prefix),
			JSONSchema:         json.RawMessage(`{"type":"object"}`),
			SupportedSyncModes: airbyte.AllSyncModes,
		})
	}

	return streams, nil
}

// formatStreamName returns a canonical stream name for the given bucket and prefix components.
// The full stream name includes the bucket name, which is important because it allows the stream
// name to fully identify the resource. The thought being that we might eventually expand this
// connector to list buckets automatically, and could potentially return prefixes from multiple
// buckets.
func formatStreamName(bucket string, prefix ...string) string {
	var builder strings.Builder
	builder.WriteString(bucket)
	builder.WriteString(s3Delimiter)
	builder.WriteString(formatPrefix(prefix...))
	return builder.String()
}

// parses a stream name into its constituent bucket and prefix. The prefix may be empty.
func parseStreamName(streamName string) (bucket, prefix string) {
	var split = strings.SplitN(streamName, s3Delimiter, 2)
	bucket = split[0]
	if len(split) == 2 {
		prefix = split[1]
	}
	return
}

// formatPrefix returns an s3 prefix from the given componenents, separated by a '/'.
// The returned prefix will always end with a '/' if it is non-empty. It will never begin with a
// '/'.
func formatPrefix(prefixComponents ...string) string {
	var builder strings.Builder
	for _, p := range prefixComponents {
		builder.WriteString(p)
		if p != "" && !strings.HasSuffix(p, s3Delimiter) {
			builder.WriteString(s3Delimiter)
		}
	}
	return builder.String()
}

func formatKey(prefixComponents []string, name string) string {
	return formatPrefix(prefixComponents...) + name
}

// s3Object represents an object that was listed from an s3 bucket. The complete key is a simple
// concatenation of the prefix and relativeKey. These two fields are kept separate because the
// relativeKey is likely what will make sense to the user as a filename.
type s3Object struct {
	bucket string
	// The prefix that the user passed in the configuration.
	prefix string
	// The remainder of the object key, minus the prefix from the configuration. This may still
	// contain some prefix components, but they will be relative to the prefix.
	relativeKey  string
	etag         string
	lastModified time.Time
}

func (o *s3Object) fullKey() string {
	return o.prefix + o.relativeKey
}

// listResult is an asynchronous result of a bucket listing, which can contain a reference to an
// object or an error, but not both.
type listResult struct {
	object *s3Object
	err    error
}

func listAllObjects(ctx context.Context, bucket string, prefix string, client *s3.S3) <-chan listResult {
	var resultCh = make(chan listResult)
	go func() {
		var err = listAllObjectsInternal(ctx, bucket, prefix, client, resultCh)
		if err != nil {
			select {
			case <-ctx.Done():
			case resultCh <- listResult{err: err}:
			}
		}
		close(resultCh)
	}()
	return resultCh
}

func listAllObjectsInternal(ctx context.Context, bucket, prefix string, client *s3.S3, resultCh chan<- listResult) error {
	var readableObjects, glacierObjects, prefixObjects int
	var continuationToken *string
	for {
		var listObjectInput = &s3.ListObjectsV2Input{
			Bucket: &bucket,
			Prefix: &prefix,
			// will be nil for the first request
			ContinuationToken: continuationToken,
		}
		var output, err = client.ListObjectsV2(listObjectInput)
		if err != nil {
			return err
		}

		for _, obj := range output.Contents {
			// Is this a prefix object? These are commonly added to buckets as a way to mimic
			// directories.
			if *obj.Size == 0 {
				prefixObjects++
				continue
			} else if isGlacier(obj) {
				glacierObjects++
				continue
			}
			readableObjects++
			var result = listResult{
				object: &s3Object{
					bucket:       bucket,
					prefix:       prefix,
					relativeKey:  *obj.Key,
					etag:         *obj.ETag,
					lastModified: *obj.LastModified,
				},
			}
			select {
			case <-ctx.Done():
				return nil
			case resultCh <- result:
			}
		}

		if output.ContinuationToken != nil {
			continuationToken = output.ContinuationToken
		} else {
			break
		}
	}
	log.WithFields(log.Fields{
		"readableObjects": readableObjects,
		"glacierObjects":  glacierObjects,
		"prefixObjects":   prefixObjects,
	}).Info("finished listing all objects under prefix")
	return nil
}

// returns true if the object is one that may be readable. This function filters out any objects
// with a "glacier" storage class because those objects could take minutes or even hours to
// retrieve. This behavior matches that of other popular tool that ingest from S3.
func isGlacier(obj *s3.Object) bool {
	return *obj.StorageClass == s3.StorageClassGlacier ||
		*obj.StorageClass == s3.StorageClassDeepArchive
}

// Technically, s3 lets you use whatever delimiter you want, but I've never seen anyone use anything
// besides '/', so I don't think it makes sense to expose this as an option to users. But using a
// constant will at least make it easier to do that in the future if we want.
const s3Delimiter = "/"
