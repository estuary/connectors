package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/avro/ocf"
)

// repairManifestListNullPartitions heals the corruption described in issue
// #4816: iceberg-go v0.6.0 encoded present-but-empty `partitions` field
// summaries in manifest lists as the Avro null union branch, which strict
// readers such as Redshift Spectrum reject, and later iceberg-go versions
// re-encode a decoded null as null on every commit, so the corruption
// propagates forward indefinitely.
//
// Every table this connector creates is unpartitioned, so a null
// `partitions` in the current manifest list is unambiguous corruption: for
// an unpartitioned table, null and empty summaries are semantically
// identical, and normalizing cannot change query results. Revisit that
// invariant if partitioned-table support ever lands.
//
// The repair rewrites the object in place, which needs no catalog commit
// because a manifest list is referenced by exact path with no checksum. It
// is race-free at Open: the connector is the table's sole appender, and a
// concurrent external rewrite only makes the repair moot. Once one clean
// manifest list exists, subsequent commits inherit present-empty arrays and
// stay clean, so the check degrades to a single GET per table per Open.
func repairManifestListNullPartitions(ctx context.Context, s3client *s3.Client, bucket, manifestList string) error {
	if manifestList == "" {
		// The table has no snapshots, so there is nothing to heal.
		return nil
	}

	prefix := "s3://" + bucket + "/"
	if !strings.HasPrefix(manifestList, prefix) {
		// An adopted pre-existing table's metadata can live outside the
		// configured bucket (or use another URI scheme). The repair is
		// opportunistic, so an unreachable manifest list is skipped rather
		// than failing the task's Open.
		log.WithFields(log.Fields{
			"manifestList": manifestList,
			"bucket":       bucket,
		}).Warn("skipping manifest-list repair of a table outside the configured bucket")
		return nil
	}
	key := strings.TrimPrefix(manifestList, prefix)

	out, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("reading manifest list %q: %w", manifestList, err)
	}
	data, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		return fmt.Errorf("reading manifest list %q: %w", manifestList, err)
	}

	repaired, healed, err := healNullPartitions(data)
	if err != nil {
		return fmt.Errorf("repairing manifest list %q: %w", manifestList, err)
	}
	if healed == 0 {
		return nil
	}

	if _, err := s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(repaired),
	}); err != nil {
		return fmt.Errorf("writing repaired manifest list %q: %w", manifestList, err)
	}

	log.WithFields(log.Fields{
		"manifestList": manifestList,
		"entries":      healed,
	}).Warn("repaired manifest list entries with null partition summaries (issue #4816)")

	return nil
}

// healNullPartitions decodes an Avro manifest list generically, replaces
// each null `partitions` value with an empty array, and re-encodes with the
// file's own embedded writer schema, codec, and metadata, so the union
// branch is the only thing that changes. The generic map decode is required:
// iceberg-go's ManifestFile.Partitions() returns an empty slice for both the
// null and the present-but-empty encoding, and round-tripping through its
// typed reader/writer would substitute the current library's schema for the
// file's own. Returns the rewritten file and the number of entries healed;
// the rewritten bytes are nil when that count is zero.
func healNullPartitions(data []byte) ([]byte, int, error) {
	rd, err := ocf.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("opening avro file: %w", err)
	}
	defer rd.Close()

	var records []map[string]any
	var healed int
	for {
		var rec map[string]any
		if err := rd.Decode(&rec); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, 0, fmt.Errorf("decoding entry: %w", err)
		}
		// The two-value lookup keeps a schema without a `partitions` field —
		// where the single-value form also yields nil — out of the healed
		// count, since "healing" it would rewrite the file on every Open.
		if v, ok := rec["partitions"]; ok && v == nil {
			rec["partitions"] = []any{}
			healed++
		}
		records = append(records, rec)
	}
	if healed == 0 {
		return nil, 0, nil
	}

	meta := rd.Metadata()
	userMeta := make(map[string][]byte, len(meta))
	for k, v := range meta {
		// The writer emits the avro.* keys itself and rejects them as user
		// metadata.
		if !strings.HasPrefix(k, "avro.") {
			userMeta[k] = v
		}
	}

	opts := []ocf.WriterOpt{
		// Preserve the file's schema JSON byte-for-byte rather than letting
		// the writer emit the canonical form of the parsed schema.
		ocf.WithSchema(string(meta["avro.schema"])),
		ocf.WithMetadata(userMeta),
	}
	codec, err := codecByName(string(meta["avro.codec"]))
	if err != nil {
		return nil, 0, err
	}
	if codec != nil {
		opts = append(opts, ocf.WithCodec(codec))
	}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, rd.Schema(), opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("opening avro writer: %w", err)
	}
	for _, rec := range records {
		if err := w.Encode(rec); err != nil {
			return nil, 0, fmt.Errorf("encoding entry: %w", err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, 0, fmt.Errorf("closing avro writer: %w", err)
	}

	return buf.Bytes(), healed, nil
}

func codecByName(name string) (ocf.Codec, error) {
	switch name {
	case "", "null":
		return nil, nil
	case "deflate":
		return ocf.DeflateCodec(-1), nil
	case "snappy":
		return ocf.SnappyCodec(), nil
	// "zstandard" is the Avro spec's on-disk avro.codec name (Iceberg's
	// write.avro.compression-codec table property spells it "zstd", but that
	// is translated before it reaches the file header).
	case "zstandard":
		return ocf.ZstdCodec(nil, nil)
	default:
		return nil, fmt.Errorf("unsupported avro codec %q", name)
	}
}
