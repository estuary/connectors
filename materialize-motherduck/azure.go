package main

import (
	"context"
	"io"
	"iter"
	"path"
	"strings"

	"github.com/estuary/connectors/go/blob"
)

// azureBucketWrapper wraps blob.AzureBlobBucket to translate between URI schemes.
//
// This wrapper is necessary because of a mismatch between what MotherDuck expects
// and what the Azure SDK produces:
//
//   - MotherDuck/DuckDB Azure extension requires "az://" URI scheme for COPY operations
//     Example: az://mycontainer/path/to/file.json
//
//   - The blob.AzureBlobBucket.URI() method returns full HTTPS URLs
//     Example: https://myaccount.blob.core.windows.net/mycontainer/path/to/file.json
//
// DuckDB's Azure extension does NOT support direct HTTPS URLs - it only recognizes
// the "az://", "azure://", or "abfss://" URI schemes.
//
// The wrapper overrides:
//   - URI(): Returns az://container/key format for MotherDuck COPY queries
//   - Delete(): Converts az:// URIs back to HTTPS URLs for the Azure SDK
//
// All other blob.Bucket methods delegate directly to the embedded AzureBlobBucket
// since they operate on keys, not full URIs.
type azureBucketWrapper struct {
	*blob.AzureBlobBucket
	container string
}

// URI returns the key in az:// format that MotherDuck expects for COPY operations.
func (b *azureBucketWrapper) URI(key string) string {
	return "az://" + path.Join(b.container, key)
}

// Delete converts az:// URIs back to HTTPS URLs before delegating to the Azure SDK.
func (b *azureBucketWrapper) Delete(ctx context.Context, uris []string) error {
	httpsURIs := make([]string, len(uris))
	for i, uri := range uris {
		key := strings.TrimPrefix(uri, "az://"+b.container+"/")
		httpsURIs[i] = b.AzureBlobBucket.URI(key)
	}
	return b.AzureBlobBucket.Delete(ctx, httpsURIs)
}

// The remaining blob.Bucket interface methods delegate to the embedded AzureBlobBucket.
// These methods operate on keys (not full URIs), so no translation is needed.

func (b *azureBucketWrapper) NewReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return b.AzureBlobBucket.NewReader(ctx, key)
}

func (b *azureBucketWrapper) NewWriter(ctx context.Context, key string, opts ...blob.WriterOption) io.WriteCloser {
	return b.AzureBlobBucket.NewWriter(ctx, key, opts...)
}

func (b *azureBucketWrapper) Upload(ctx context.Context, key string, r io.Reader, opts ...blob.WriterOption) error {
	return b.AzureBlobBucket.Upload(ctx, key, r, opts...)
}

func (b *azureBucketWrapper) CheckPermissions(ctx context.Context, cfg blob.CheckPermissionsConfig) error {
	return b.AzureBlobBucket.CheckPermissions(ctx, cfg)
}

func (b *azureBucketWrapper) List(ctx context.Context, query blob.Query) iter.Seq2[blob.ObjectInfo, error] {
	return b.AzureBlobBucket.List(ctx, query)
}
