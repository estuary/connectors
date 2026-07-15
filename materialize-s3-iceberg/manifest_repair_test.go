package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// writeManifestListFixture builds a two-entry manifest list of the given
// format version: entry one carries the null `partitions` union branch that
// iceberg-go v0.6.0 wrote for inherited manifests, entry two the
// present-but-empty encoding of a healthy unpartitioned commit.
func writeManifestListFixture(t *testing.T, version int) []byte {
	t.Helper()

	corrupt := iceberg.NewManifestFile(version, "s3://bucket/metadata/m1.avro", 111, 0, 1).
		AddedFiles(1).AddedRows(10)
	clean := iceberg.NewManifestFile(version, "s3://bucket/metadata/m2.avro", 222, 0, 2).
		AddedFiles(2).AddedRows(20).
		Partitions([]iceberg.FieldSummary{})

	var seq *int64
	if version != 1 {
		corrupt = corrupt.SequenceNum(1, 1)
		clean = clean.SequenceNum(2, 2)
		s := int64(2)
		seq = &s
	}

	parent := int64(1)
	var buf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(version, &buf, 2, &parent, seq, 0,
		[]iceberg.ManifestFile{corrupt.Build(), clean.Build()}))
	return buf.Bytes()
}

// readOCFRecords generically decodes every record of an Avro OCF alongside
// its header metadata.
func readOCFRecords(t *testing.T, data []byte) ([]map[string]any, map[string][]byte) {
	t.Helper()

	rd, err := ocf.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer rd.Close()

	var recs []map[string]any
	for {
		var rec map[string]any
		if err := rd.Decode(&rec); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		recs = append(recs, rec)
	}
	return recs, rd.Metadata()
}

// transcodeOCF rewrites an OCF with a different codec, preserving its schema
// and metadata, so codec variants of a fixture can be synthesized.
func transcodeOCF(t *testing.T, data []byte, codec ocf.Codec) []byte {
	t.Helper()

	rd, err := ocf.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer rd.Close()

	meta := rd.Metadata()
	userMeta := map[string][]byte{}
	for k, v := range meta {
		if !strings.HasPrefix(k, "avro.") {
			userMeta[k] = v
		}
	}

	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, rd.Schema(),
		ocf.WithSchema(string(meta["avro.schema"])),
		ocf.WithMetadata(userMeta),
		ocf.WithCodec(codec))
	require.NoError(t, err)
	for {
		var rec map[string]any
		if err := rd.Decode(&rec); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		require.NoError(t, w.Encode(rec))
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// withoutPartitions returns copies of the records with the `partitions` key
// removed, for asserting that a repair changed nothing else.
func withoutPartitions(recs []map[string]any) []map[string]any {
	out := make([]map[string]any, len(recs))
	for i, rec := range recs {
		c := make(map[string]any, len(rec))
		for k, v := range rec {
			if k != "partitions" {
				c[k] = v
			}
		}
		out[i] = c
	}
	return out
}

func TestHealNullPartitions(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "v1", 2: "v2"}[version], func(t *testing.T) {
			data := writeManifestListFixture(t, version)

			beforeRecs, beforeMeta := readOCFRecords(t, data)
			require.Len(t, beforeRecs, 2)
			require.Nil(t, beforeRecs[0]["partitions"])
			require.NotNil(t, beforeRecs[1]["partitions"])

			repaired, healed, err := healNullPartitions(data)
			require.NoError(t, err)
			require.Equal(t, 1, healed)

			afterRecs, afterMeta := readOCFRecords(t, repaired)
			require.Len(t, afterRecs, 2)
			for _, rec := range afterRecs {
				require.NotNil(t, rec["partitions"])
			}
			require.Equal(t, withoutPartitions(beforeRecs), withoutPartitions(afterRecs),
				"the repair must change nothing but the partitions union branch")
			require.Equal(t, beforeMeta["avro.schema"], afterMeta["avro.schema"],
				"the repair must preserve the writer schema byte-for-byte")
			require.Equal(t, beforeMeta["avro.codec"], afterMeta["avro.codec"])
			for k, v := range beforeMeta {
				if !strings.HasPrefix(k, "avro.") {
					require.Equal(t, v, afterMeta[k], "metadata key %q must be preserved", k)
				}
			}
		})
	}

	t.Run("clean file is a no-op", func(t *testing.T) {
		data := writeManifestListFixture(t, 2)
		clean, healed, err := healNullPartitions(data)
		require.NoError(t, err)
		require.Equal(t, 1, healed)

		repaired, healed, err := healNullPartitions(clean)
		require.NoError(t, err)
		require.Zero(t, healed)
		require.Nil(t, repaired)
	})

	t.Run("absent partitions field is not healed", func(t *testing.T) {
		schema := avro.MustParse(`{"type":"record","name":"r","fields":[{"name":"x","type":"long"}]}`)
		var buf bytes.Buffer
		w, err := ocf.NewWriter(&buf, schema)
		require.NoError(t, err)
		require.NoError(t, w.Encode(map[string]any{"x": int64(1)}))
		require.NoError(t, w.Close())

		repaired, healed, err := healNullPartitions(buf.Bytes())
		require.NoError(t, err)
		require.Zero(t, healed)
		require.Nil(t, repaired)
	})

	t.Run("codecs are preserved", func(t *testing.T) {
		for name, codec := range map[string]ocf.Codec{
			"snappy":    ocf.SnappyCodec(),
			"zstandard": ocf.MustZstdCodec(nil, nil),
		} {
			data := transcodeOCF(t, writeManifestListFixture(t, 2), codec)

			repaired, healed, err := healNullPartitions(data)
			require.NoError(t, err)
			require.Equal(t, 1, healed)

			afterRecs, afterMeta := readOCFRecords(t, repaired)
			require.Equal(t, name, string(afterMeta["avro.codec"]))
			for _, rec := range afterRecs {
				require.NotNil(t, rec["partitions"])
			}
		}
	})

	t.Run("unknown codec errors", func(t *testing.T) {
		_, err := codecByName("lzo")
		require.Error(t, err)
	})
}

// fakeLoadCatalog serves constructed tables from LoadTable; the embedded
// interface panics if any other method is called.
type fakeLoadCatalog struct {
	icebergcatalog.Catalog
	tables map[string]*icebergtable.Table
}

func (f *fakeLoadCatalog) LoadTable(_ context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	return f.tables[ident[len(ident)-1]], nil
}

func newFakeTable(t *testing.T, spec *iceberg.PartitionSpec) *icebergtable.Table {
	t.Helper()

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: iceberg.PrimitiveTypes.String})
	md, err := icebergtable.NewMetadata(schema, spec, icebergtable.UnsortedSortOrder, "s3://bucket/loc", nil)
	require.NoError(t, err)
	return icebergtable.New(icebergtable.Identifier{"ns", "tbl"}, md, "s3://bucket/loc/metadata/v1.json", nil, nil)
}

// TestTableInfosPartitionGate pins the flag that gates the issue #4816
// manifest-list repair: it must be true only for unpartitioned tables, since
// a partitioned table may legitimately carry null partition summaries.
func TestTableInfosPartitionGate(t *testing.T) {
	partSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "v", Transform: iceberg.IdentityTransform{},
	})
	c := &catalog{cfg: &config{}, locationStyle: NestedLocationStyle, cat: &fakeLoadCatalog{
		tables: map[string]*icebergtable.Table{
			"unpart": newFakeTable(t, nil),
			"part":   newFakeTable(t, &partSpec),
		},
	}}

	infos, err := c.tableInfos(context.Background(), [][]string{{"ns", "unpart"}, {"ns", "part"}})
	require.NoError(t, err)
	require.True(t, infos[0].unpartitioned)
	require.False(t, infos[1].unpartitioned)
	// A table with no snapshots has no manifest list to repair.
	require.Empty(t, infos[0].manifestList)
}
