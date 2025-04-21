package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

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
