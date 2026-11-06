package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBlobWriteCloser(t *testing.T) {
	ctx := context.Background()
	data := []byte("hello world")

	t.Run("success", func(t *testing.T) {
		fn := func(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
			var got bytes.Buffer
			if _, err := io.Copy(&got, r); err != nil {
				return err
			} else if !bytes.Equal(got.Bytes(), data) {
				return fmt.Errorf("got %q, want %q", got.Bytes(), data)
			}

			return nil
		}

		bwc := newBlobWriteCloser(ctx, fn, "foo")
		_, err := bwc.Write(data)
		require.NoError(t, err)
		require.NoError(t, bwc.Close())
	})

	t.Run("error while writing", func(t *testing.T) {
		fn := func(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
			buf := make([]byte, 1)
			if _, err := r.Read(buf); err != nil {
				return err
			}

			return errors.New("some error")
		}

		bwc := newBlobWriteCloser(ctx, fn, "foo")
		_, err := bwc.Write(data)
		require.Error(t, err)
	})

	t.Run("error after writing", func(t *testing.T) {
		fn := func(ctx context.Context, key string, r io.Reader, opts ...WriterOption) error {
			var got bytes.Buffer
			if _, err := io.Copy(&got, r); err != nil {
				return err
			} else if !bytes.Equal(got.Bytes(), data) {
				return fmt.Errorf("got %q, want %q", got.Bytes(), data)
			}

			return errors.New("some error")
		}

		bwc := newBlobWriteCloser(ctx, fn, "foo")
		_, err := bwc.Write(data)
		require.NoError(t, err)
		require.Error(t, bwc.Close())
	})
}

// loadSopsConfig decrypts the sops-encrypted YAML file at path into T.
func loadSopsConfig[T any](t *testing.T, path string) T {
	t.Helper()

	out, err := exec.Command("sops", "--decrypt", "--output-type", "json", path).Output()
	require.NoError(t, err, "decrypting %s", path)

	var cfg T
	require.NoError(t, json.Unmarshal(out, &cfg))
	return cfg
}

// testBucket exercises every Bucket method under a prefix unique to this run,
// so concurrent runs against the same bucket do not observe each other.
func testBucket(t *testing.T, bucket Bucket) {
	var ctx = context.Background()
	var prefix = path.Join("blob-integration-test", uuid.NewString())

	var listKeys = func(t *testing.T, prefix string) []string {
		t.Helper()
		var keys []string
		for obj, err := range bucket.List(ctx, Query{Prefix: prefix}) {
			require.NoError(t, err)
			keys = append(keys, obj.Key)
		}
		slices.Sort(keys)
		return keys
	}

	var readKey = func(t *testing.T, key string) string {
		t.Helper()
		r, err := bucket.NewReader(ctx, key)
		require.NoError(t, err)
		defer r.Close()
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		return string(got)
	}

	t.Cleanup(func() {
		var uris []string
		for obj, err := range bucket.List(ctx, Query{Prefix: prefix}) {
			if err != nil {
				t.Logf("listing for cleanup: %v", err)
				return
			}
			uris = append(uris, bucket.URI(obj.Key))
		}
		if len(uris) > 0 {
			if err := bucket.Delete(ctx, uris); err != nil {
				t.Logf("cleanup: %v", err)
			}
		}
	})

	t.Run("check permissions", func(t *testing.T) {
		for _, cfg := range []CheckPermissionsConfig{
			{Prefix: prefix},
			{Prefix: prefix, Lister: true},
			{Prefix: prefix, ReadOnly: true},
		} {
			require.NoError(t, bucket.CheckPermissions(ctx, cfg), "%+v", cfg)
		}
		require.Empty(t, listKeys(t, prefix), "CheckPermissions left objects behind")
	})

	var uploaded = path.Join(prefix, "a/uploaded.txt")
	var written = path.Join(prefix, "a/b/written.txt")
	var other = path.Join(prefix, "c/other.txt")

	t.Run("upload and read", func(t *testing.T) {
		require.NoError(t, bucket.Upload(ctx, uploaded, strings.NewReader("uploaded content")))
		require.Equal(t, "uploaded content", readKey(t, uploaded))
	})

	t.Run("writer and read", func(t *testing.T) {
		var w = bucket.NewWriter(ctx, written, WithObjectMetadata(map[string]string{"testkey": "testvalue"}))
		_, err := w.Write([]byte("written "))
		require.NoError(t, err)
		_, err = w.Write([]byte("content"))
		require.NoError(t, err)
		require.NoError(t, w.Close())
		require.Equal(t, "written content", readKey(t, written))

		require.NoError(t, bucket.Upload(ctx, other, strings.NewReader("other")))
	})

	t.Run("read missing key", func(t *testing.T) {
		_, err := bucket.NewReader(ctx, path.Join(prefix, "does-not-exist"))
		require.Error(t, err)
	})

	t.Run("list", func(t *testing.T) {
		require.Equal(t, []string{written, uploaded, other}, listKeys(t, prefix))
		require.Equal(t, []string{written, uploaded}, listKeys(t, path.Join(prefix, "a")+"/"))
		require.Equal(t, []string{written}, listKeys(t, path.Join(prefix, "a/b")))
		require.Empty(t, listKeys(t, path.Join(prefix, "nothing-here")))
	})

	t.Run("delete", func(t *testing.T) {
		require.NoError(t, bucket.Delete(ctx, []string{bucket.URI(uploaded), bucket.URI(written)}))
		require.Equal(t, []string{other}, listKeys(t, prefix))
		require.NoError(t, bucket.Delete(ctx, []string{bucket.URI(other)}))
		require.Empty(t, listKeys(t, prefix))
	})
}

// testStoredMD5 uploads an object larger than every provider's multipart or
// chunking threshold and requires that the hash the provider stores for it,
// as returned by storedMD5 in hex, is the MD5 of its bytes.
func testStoredMD5(t *testing.T, bucket Bucket, storedMD5 func(t *testing.T, key string) string) {
	var ctx = context.Background()
	var key = path.Join("blob-integration-test", uuid.NewString(), "large")
	t.Cleanup(func() {
		if err := bucket.Delete(ctx, []string{bucket.URI(key)}); err != nil {
			t.Logf("cleanup: %v", err)
		}
	})

	var data = make([]byte, 40<<20)
	_, err := rand.Read(data)
	require.NoError(t, err)
	var want = md5.Sum(data)

	require.NoError(t, bucket.Upload(ctx, key, bytes.NewReader(data)))
	require.Equal(t, hex.EncodeToString(want[:]), storedMD5(t, key))
}
