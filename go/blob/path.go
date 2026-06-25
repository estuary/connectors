package blob

import (
	"fmt"
	"strings"
)

// ValidateBucketPath checks that a configured object-storage key prefix is a
// bare prefix within the bucket, rather than a leading-slash path or a full URI.
//
// The bucket itself is configured separately, and the prefix is joined onto
// object keys via path.Join. Two malformed inputs corrupt the resulting keys:
//
//   - A leading "/" produces an object key that starts with "/".
//   - A full URI such as "s3://bucket/prefix" is collapsed by path.Clean (which
//     reduces "s3://" to "s3:/"), leaving a stray "s3:" path segment. Downstream
//     readers such as Spark/Hadoop then fail to parse it, e.g.
//     "java.net.URISyntaxException: Expected scheme-specific part at index 3: s3:".
//
// field is the configuration field's label, used in the error message (for
// example "prefix" or "bucket path"). An empty prefix is always valid.
func ValidateBucketPath(field, prefix string) error {
	if prefix == "" {
		return nil
	}
	if strings.HasPrefix(prefix, "/") {
		return fmt.Errorf("%s %q cannot start with /", field, prefix)
	}
	if strings.Contains(prefix, "://") {
		return fmt.Errorf(
			"%s %q must be a bare key prefix within the bucket, not a full URI like \"s3://bucket/prefix\"; the bucket is configured separately",
			field, prefix,
		)
	}
	return nil
}
