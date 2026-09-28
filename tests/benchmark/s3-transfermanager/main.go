// s3-transfermanager uploads N files concurrently through the AWS transfer manager with
// the options gosnowflake's S3 client uses, and reports peak Go heap.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	n := flag.Int("n", 34, "concurrent uploads")
	sizeMiB := flag.Int("size", 250, "file size in MiB")
	partMiB := flag.Int("part", 8, "part size / multipart threshold in MiB")
	parallel := flag.Int("parallel", 1, "transfer manager concurrency (PARALLEL)")
	bucket := flag.String("bucket", "", "bucket")
	prefix := flag.String("prefix", "tmbench", "key prefix")
	region := flag.String("region", "us-east-2", "region")
	dir := flag.String("dir", os.TempDir(), "scratch dir")
	flag.Parse()

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(*region))
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)
	partSize := int64(*partMiB) << 20
	uploader := transfermanager.New(client, func(o *transfermanager.Options) {
		o.Concurrency = *parallel
		o.PartSizeBytes = partSize
		o.MultipartUploadThreshold = partSize
	})

	// Files of incompressible data, generated once.
	files := make([]string, *n)
	var wg sync.WaitGroup
	for i := range files {
		files[i] = filepath.Join(*dir, fmt.Sprintf("tmbench-%d", i))
		if st, err := os.Stat(files[i]); err == nil && st.Size() == int64(*sizeMiB)<<20 {
			continue // reuse a file from an earlier run
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f, err := os.Create(files[i])
			if err != nil {
				log.Fatal(err)
			}
			buf := make([]byte, 1<<20)
			r := rand.New(rand.NewSource(int64(i)))
			for j := 0; j < *sizeMiB; j++ {
				r.Read(buf)
				f.Write(buf)
			}
			f.Close()
		}(i)
	}
	wg.Wait()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	fmt.Printf("baseline heap_inuse=%dMiB sys=%dMiB\n", base.HeapInuse>>20, base.Sys>>20)

	// Sample the heap while uploads run.
	var peakInuse, peakSys uint64
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				peakInuse = max(peakInuse, m.HeapInuse)
				peakSys = max(peakSys, m.Sys)
			}
		}
	}()

	start := time.Now()
	for i, path := range files {
		wg.Add(1)
		go func(i int, path string) {
			defer wg.Done()
			f, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()
			key := fmt.Sprintf("%s/%d", *prefix, i)
			if _, err := uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
				Bucket: bucket, Key: &key, Body: f,
			}); err != nil {
				log.Fatalf("upload %d: %v", i, err)
			}
		}(i, path)
	}
	wg.Wait()
	close(done)
	elapsed := time.Since(start)
	fmt.Printf("uploads=%d size=%dMiB part=%dMiB parallel=%d took=%s peak heap_inuse=%dMiB sys=%dMiB per_upload=%.1fMiB\n",
		*n, *sizeMiB, *partMiB, *parallel, elapsed.Round(time.Second), peakInuse>>20, peakSys>>20,
		float64(peakInuse-base.HeapInuse)/float64(*n)/(1<<20))

	for i := range files {
		key := fmt.Sprintf("%s/%d", *prefix, i)
		client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: aws.String(key)})
	}
}
