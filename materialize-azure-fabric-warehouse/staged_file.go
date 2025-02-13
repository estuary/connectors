package main

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	"golang.org/x/sync/errgroup"
)

// Multiple files are loaded faster by COPY INTO than a single large file.
// Splitting files into 250MiB chunks (after compression) seems to work well
// enough for larger transactions.
const fileSizeLimit = 250 * 1024 * 1024

type azureBlobObject struct {
	directory string
	name      string
}

func (o azureBlobObject) blobName() string {
	return path.Join(o.directory, o.name)
}

func newFileClient(azClient *azblob.Client, container string, directory string) *stagedFileClient {
	return &stagedFileClient{
		container: container,
		directory: directory,
		azClient:  azClient,
	}
}

type stagedFileClient struct {
	container string
	directory string
	azClient  *azblob.Client
}

func (s *stagedFileClient) NewEncoder(w io.WriteCloser, fields []string) boilerplate.Encoder {
	return enc.NewCsvEncoder(w, fields, enc.WithCsvSkipHeaders(), enc.WithCsvQuoteChar('`'))
}

func (s *stagedFileClient) NewObject(uuid string) azureBlobObject {
	return azureBlobObject{directory: s.directory, name: uuid}
}

func (s *stagedFileClient) URI(o azureBlobObject) string {
	return s.azClient.URL() + path.Join(s.container, o.blobName())
}

func (s *stagedFileClient) UploadStream(ctx context.Context, o azureBlobObject, r io.Reader) error {
	if _, err := s.azClient.UploadStream(ctx, s.container, o.blobName(), r, nil); err != nil {
		return err
	}

	return nil
}

func (s *stagedFileClient) Delete(ctx context.Context, uris []string) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, uri := range uris {
		parts := strings.TrimPrefix(uri, s.azClient.URL())
		firstSlash := strings.Index(parts, "/")
		container := parts[:firstSlash]
		blobName := parts[firstSlash+1:]
		group.Go(func() error {
			if _, err := s.azClient.DeleteBlob(groupCtx, container, blobName, nil); err != nil {
				return fmt.Errorf("deleting blob %q: %w", blobName, err)
			}
			return nil
		})
	}

	return group.Wait()
}
